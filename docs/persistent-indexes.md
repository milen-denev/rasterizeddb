# Persistent indexes

Status: **designed, not built.** This revision cuts the design to a small v1
and applies the 2026-10-17 verification round (43 corrections), the owner's
v1 review (27 findings, RV1-RV27, §17) and the verification of those fixes
(33 problems, P1-P33, §17), then the owner's re-check of those (R1-R12 and
four still-open items, §17), with §7 rebuilt on a table-lock manager, then
the verification of that revision (19 problems, Q1-Q19, §17). Line
anchors were read on 2026-10-17 from the `feat/partitioning` working
tree, relative to `rasterizeddb_core/src/`. `core/index/*` is owned by
rasterizeddb-99 and is changing, and `core/row/table.rs` is shared, so anchors
drift; function names are the stable part. Siblings:
[Hash partitioning](partitioning.md), whose phase 3 (lazy open, idle close)
v1 hooks into, and [Sharding](sharding.md), whose layout (§11) sizes it.

Today every B-tree, hash, path and gin index, and the filter, lives in memory
and is rebuilt by a full scan at open ([Indexes](indexes.md), "Lifetime"). The
filter sidecar `_flt.db` persists, but it is trusted on a dirty byte that the
code does not keep correct (§5).

**v1** persists a partition's (or plain table's) indexes and filter **only at
a clean point**: idle close, shutdown, or an explicit checkpoint. The snapshot
is written to new files and published by rename. It is trusted only while a
marker log ends in `CLEAN`, and `DIRTY` is made durable before the first write
of any kind after it. An open that proves the snapshot clean loads it; any
other open rebuilds exactly as today, that rebuild stays the source of truth,
and the next clean point rewrites the snapshot. While open, writes go to
today's in-memory structures only. There is no online flush, freeze, debt,
grace period, merge or frontier. Where a correction would have needed online
machinery, v1 refuses the snapshot and rebuilds instead.

**v2**, the previous revision's online LSM design, in which the index owner's
two reviews confirmed 64 defects, becomes requirements (§16), built only when a
measurement says v1 is not enough.

```
 CLEAN ─open─▶ proof ok? ──yes──▶ load snapshot (starts at routed open) ─┐
   ▲                      └─no──▶ rebuild as today (BuildToken) ─────────┤
   │                                                                     ▼
   │                    first mutation through the write gate:
   │                       append DIRTY, fdatasync (T1)
   │                                                                     ▼
   │                      writes land in RAM structures only
   │                                                                     ▼
   └─ append CLEAN, fdatasync ◀─ temp, fsync, rename, dir fsync ◀─ seal, land .tmp, barrier
                                                     (clean point only; writers preempt)
```

## What was asked, and what v1 gives

The target: 1,000,000 tenants × ~800,000 rows, 8×10¹¹ rows, ~207 TB, laid out
as in [sharding §11](sharding.md#11-capacity-planning-against-the-target): 128
shards of 6.25×10⁹ rows, each 4,096 partitions of 1.53M rows (1.9 tenants),
six shard processes per machine. The goal: **RAM proportional to active data.**

| Ask | v1 |
|---|---|
| RAM tracks active tenants | **Yes, with partitioning phase 3, while the hot set fits in RAM** (§9). A closed partition costs ~64 B and no index RAM; an open one holds today's in-memory index. v1 makes reopening cheap (read ~0.1-0.14 GB, not scan ~0.4 GB plus ~1.3 s CPU per index), so idle windows can be short and the open set follows the active set. |
| Startup without a rebuild | **Yes after a clean shutdown (within its deadline, §3) or idle close.** A partition written since its last snapshot rebuilds after a crash, as today. |
| No new commit cost | **Yes.** No index bytes in the WAL, no per-commit fsync. One `DIRTY` fdatasync at the first mutation after a clean open (§2). |
| Hot set larger than RAM | **No.** That is v2 (§16). |

## What v1 is not

- **Not online persistence.** Nothing is written while a partition serves
  writes.
- **Not a WAL-logged B+tree** (commit-log bytes, recovery changes, concurrent
  splits), and **not mmap**: snapshots are read with `pread`.
- **Not on KV-backed tables or `wal_enabled=false` directories.** These take no
  snapshot and keep **no persistent filter**: it is backfilled from rows at
  every open, with no `_flt.db`, no dirty byte and no runtime checkpoint (§5).
  A table in bulk-load mode, or with an unlogged write since open, passes the
  gate like any other, so its marker log ends `DIRTY`, and §3 refuses its
  snapshot.
- **Not a rewrite of `core/index`.** New code lives in `core/index_store/`;
  six small hooks land in `core/index` (§15).

## 0. The rule

Every index candidate is re-checked: the scan re-reads the pointer, checks
visibility and evaluates the whole `WHERE`, and UNIQUE compares bytes. A stale
entry costs a fetch; **a missing entry is a silent wrong answer.** v1 has one
trust rule, for indexes and filter alike:

> **A snapshot is used only if it is proven clean: no write of any kind was
> issued to the partition after it was taken, and its recorded identity
> matches the data. Otherwise everything is rebuilt from rows.**

This is owner's invariant 1 at its simplest: "checkpoint plus delta" with an
always-empty delta, since any write untrusts the checkpoint. The superset
becomes **"proven-clean snapshot, or rebuild"**: a snapshot is the in-memory
index captured with no write in flight, its outcomes durable and every index
ready (§3). The owner's traps T1-T4 (invariants file, item 11) bind both
halves: §2 (power loss, every mutator), §4 (identity against data), §5 (the
filter).

Bug letters (a)-(d) follow rasterizeddb-99's 2026-10-17 list of today's bugs;
(e), vacuum publishing pre-compaction free space (§2), came from this
design's verification (Q10).

## 1. Files

For partition `pid` of `__pt_<uid>`, in its pointer file's directory (a plain
table uses `<t>.ixs` and so on):

```
__pt_<uid>_<pid>.ixs       snapshot: filter section + one section per index
__pt_<uid>_<pid>.ixs.tmp   snapshot being written; never trusted, reaped at open
__pt_<uid>_<pid>.ixm       marker log: HEADER, then DIRTY / CLEAN records
__pt_<uid>_<pid>.ixm.tmp   marker log being rewritten; never trusted, reaped at open
rdb.ixe                    slot-epoch log, one per directory, WAL'd
```

**The stem is the physical name the rows file uses** (`physical`, or
`table_name` at open; `table.rs:869`, `:1003-1005`), never `schema.name`
(P32). `schema.name` is written through a filter keeping only `[A-Za-z0-9_]`
(`schema.rs:540-555`), so the legal tables `a.b` and `ab` would share one
pair, and a partition's `schema.name` is the logical table's, shared by every
partition (`table.rs:792`), so all partitions would share one log.

**No table file can have these names** (RV6, P20). Every file a table owns is
its name plus a `RENAMED_SUFFIXES` entry, all ending in `.db`, or that file
plus `.tmp` (`database/mod.rs:220-242`). The obvious `<t>.ixs` would be the
rows file of a table named `<t>.ixs`, which publish would rename over, the log
would truncate and DROP would sweep. A name ending `.ixs`, `.ixs.tmp`, `.ixm`,
`.ixm.tmp` or `.ixe` matches no table file, and two stems never yield the same
index-store name. The epoch log is `rdb.ixe` for the same reason: `rdb_ixe.db`
would be the rows file of a table named `rdb_ixe`, which `table_name_refusal`
accepts (it refuses only long names, `__pt_`, path separators and `.db`) and
`RESERVED_FILES` (`database/mod.rs:267-274`) does not list, so epoch records
and that table's rows would share a file. A leftover from a failed drop is
rejected by the incarnation (C7) and unlinked by the next create of that name.

| Item | Status | Losing it means |
|---|---|---|
| Rows, pointers, status log, schema, declarations | authoritative, unchanged | — |
| Table incarnation (random u128 in `_schema.db`, hook C7) | authoritative | snapshot rejected |
| `rdb.ixe` (incarnation, pid, slot epoch) | WAL'd; bumped durably before a renumbering's first effect | snapshot rejected |
| `.ixs`, `.ixm` | derived; raw `File`, error-returning `pwrite`/`fdatasync`, never WAL'd | rebuild |

`.ixs` and `.ixm` bypass `LocalStorageProvider` (five fds, an mmap, writes
that swallow errors, and WAL replay into a renamed file). Neither holds an fd
while the partition is open, and v1 drops the `_flt.db` provider (§5;
`table.rs:434`, `:1005`), so
[partitioning §7.1](partitioning.md#71-resident-cost)'s per-partition fd term
falls from 15 to **10** (two providers × 5); `rdb.ixe` costs five fds per
directory.

**Snapshot.** A header (magic `RZIXSNP1`, `format_ver`, `header_len`, the
identity of §4, a section table of kind, ordinal, offset, len, crc32c, and a
**header crc32c** over every header byte before it), then sections. The
header checksum lets a fan-out open (§4) trust the section table without
reading the whole file for the footer: a rotted offset would otherwise send
its FILTER `pread` elsewhere, and only that section's own crc32c would
notice. The sections:
- **FILTER**: today's `persist` base-image format, written by
  `serialize_sealed` with the true frontier (§5);
- **INDEX** per B-tree, hash or path index: sorted `(key ‖ slot)` blocks,
  prefix-compressed with restarts every 16 entries; `UNKEYED`, sorted distinct
  slots; `PRESENT`, the slot set vacuum's `holds` reads (`index/mod.rs:224-245`),
  run-length coded;
- **GIN** per gin index: `item_u64 BE` plus a delta-varint posting list,
  `UNKEYED`, `PRESENT`.

A footer holds an xxh64 (§8) of all preceding bytes. Hash indexes are stored
sorted by `encode_canonical` `KeyBuf` bytes; `RandomState` hashes never reach
disk. At ~14-24 B per entry, three indexes over 1.53M rows are 64-110 MB; with
the ~34 MB filter ([sharding §11.2](sharding.md#112-ram-per-machine)) a
snapshot is **~98-144 MB**, against ~395 MB of rows and pointers.

**Marker log.** Records are `len u32 ‖ crc32c u32 ‖ kind u8 ‖ rec_seq u64 ‖
body`: `HEADER` (format, incarnation), `DIRTY` (no body), `CLEAN` (identity,
snapshot length, footer checksum). Rules (finding 11, N33):
- Records are read up to the first bad frame. The snapshot is proven clean
  only if the last good record is `CLEAN` **and no bytes follow it**.
- **The file never shrinks to end at a `CLEAN`.** Bytes after the last good
  record may be a torn tail, or a `DIRTY` that was durable, had writes after
  it, and then rotted. Cutting back to the `CLEAN` and crashing before the next
  `DIRTY` is durable would leave `CLEAN` last over changed data. So an append
  after bad bytes overwrites in place: `pwrite` the new record at the last good
  record's end, fdatasync, `ftruncate` just past it, fdatasync, with nothing
  else issued in between and no write admitted until the last fdatasync has
  returned. Some bytes follow the `CLEAN` at every instant, so a crash
  anywhere leaves it untrusted. The second fdatasync matters as much as the
  first: `rec_seq` binds a record to its position, so a stale record that
  happens to be valid and sits right after the new one reads as its successor
  until the truncate is durable (an old `CLEAN` there would prove a snapshot
  the new `DIRTY` revoked). B11 pins the order in a crash-image test.
- Compaction happens only inside a publish while `DIRTY` is last, **before**
  the `PUBLISHING` compare-and-swap (§3 step 6), so a writer's `PUBLISHING`
  wait never includes it (P31): rewrite as `HEADER` + `DIRTY` via `.ixm.tmp`,
  fsync, rename, directory fsync; then the compare-and-swap and the `CLEAN`
  append (failpoint `marker_compaction_after_rename`). The `Unknown` rewrite
  (§2) uses the same `.ixm.tmp` (P29); the two never overlap, since a seal
  starts only from `Dirty`, the `Unknown` slow path runs only in `Unknown`,
  single-flight, and seals hold the maintenance lock (§3).
- A log that cannot be read (EIO, a read racing an unlink or rename, a bad
  header) proves nothing and puts the gate in `Unknown` (§2). A missing log
  (`ENOENT`) means no `CLEAN` was ever durable.

## 2. The write gate and `DIRTY`

**T1.** `DIRTY` is appended to `.ixm` and fdatasynced **before the first
post-snapshot write of any kind is issued** in the partition: data, WAL,
sidecar, filter or index store. Issued means `write(2)`: writeback may persist
any issued page, so a `DIRTY` that is only appended, or appended after the
write, can be lost while the write survives, leaving `CLEAN` over changed data.

**The state is the marker log's, not the proof's** (N21). Three states:
- **`Clean`**: the last good record is `CLEAN`, even if this open rejected the
  snapshot (version mismatch, load checksum failure, transient EIO on
  `.ixs`, bytes after it). Otherwise a binary that rejected it could write
  without `DIRTY`, and a later binary or a retry would accept it.
- **`Dirty`**: the last good record is `DIRTY` or `HEADER`, or there is no log.
- **`Unknown`**: the log could not be read at open; or a `CLEAN` write was
  issued and its fdatasync, or anything after it, failed (§3 step 7); or a
  `_flt.db` could not be deleted (§5). It behaves as `Clean`, and its slow
  path trusts none of the log's bytes: first rewrite the log as `HEADER` +
  `DIRTY` (`.ixm.tmp`, fsync, rename, directory fsync, failpoint
  `unknown_rewrite_after_first_write`), or else unlink `.ixs` and fsync the
  directory; only then delete a lingering `_flt.db`. **One rule for every
  path** (R9): `_flt.db` is deleted only after the log durably ends `DIRTY` or
  `.ixs` is durably gone, since it is the only evidence that an old binary
  wrote after `CLEAN` (failpoint `unknown_flt_delete_order`). **The word
  leaves `Unknown` only after the `_flt.db` delete returns `Ok`** (Q12): if
  the rewrite or unlink succeeds and the delete then fails, the ticket fails,
  the word stays `Unknown`, and the next slow path repeats the harmless
  rewrite and retries the delete. Moving to `Dirty` once the log is durably
  `DIRTY` would let later tickets take the fast path and write beside the
  surviving `_flt.db`, which a downgraded binary would then trust.

**The gate belongs to the file set, not the `Table` instance** (RV1, RV14).
It lives in the table's map slot (§7 step 4), so a reload's old and new
instances, and the old instance's maintenance tasks, count against one word.
The word's continuity is part of the design (P6):
- only the **first** open of a file set derives the word from `.ixm` (§4); a
  reload's open within the same slot keeps the existing word, count and flags,
  untouched;
- each ticket holds an `Arc` to its word, so a ticket that outlives a DROP
  decrements a detached word harmlessly;
- RENAME and MOVE carry the word, with the caller family's ticket on it, to the
  target's slot (§6, §7); an ALTER rewrite makes a new file set and a new word
  (§7);
- a debug assertion checks that no decrement wraps the count, which would
  borrow into the state and flag bits.

**T2: structural, not per call site.** A guard that enumerates its cases
misses one ("a guard is an allowlist"), so the gate has two layers.

**1. Tickets for in-memory mutations.** The gate is **one packed `AtomicU64`**
per partition: the state (2 bits), `SEALED`, `MANDATORY`, `PUBLISHING`,
`CANCEL`, and the count of outstanding tickets.
- `ticket()` is `fetch_add(1)`. If the returned word is `Dirty` and either
  `SEALED` is clear or `CANCEL` is set without `PUBLISHING`, the ticket is
  held: one RMW, no lock. If it shows an **optional seal** (`Dirty`,
  `SEALED`, not `MANDATORY`, not `PUBLISHING`, not `CANCEL`), the writer
  keeps its count and sets `CANCEL` by a compare-and-swap loop that succeeds
  only while the word still shows that same seal; it is then held. If the
  word has moved on (the seal skipped, became mandatory or reached
  `PUBLISHING`), it does `fetch_sub(1)` and starts `ticket()` again from the
  top, so `CANCEL` is never left on an unsealed word, where it would let later
  tickets skip a mandatory seal's queue (P7). The seal cannot publish
  meanwhile: the `PUBLISHING` compare-and-swap requires `CANCEL` clear **and a
  count of zero** (§3 step 6), and the writer's count is held throughout. The
  writer does not wait for the sealer's fdatasync, rename or barrier to notice
  (RV7). The log still ends `DIRTY` (a seal starts only from `Dirty`, §3), so
  T1 holds. Otherwise it does `fetch_sub(1)`, wakes a sealer waiting on the
  count, and takes the slow path: it waits for a mandatory seal or a
  `PUBLISHING` window to end; then, single-flight, `Clean` appends `DIRTY` and
  fdatasyncs and `Unknown` rewrites or unlinks, then deletes a lingering
  `_flt.db`, as above; the state becomes `Dirty` only when every step returned
  `Ok`, and it retries. **A writer's wait on an optional seal is bounded by
  one `PUBLISHING` window** (one `CLEAN` append and fdatasync) plus any
  landing or device writeback already in flight (§3 Cost).
- **Lock order** (RV16): a ticket is taken after the table lock (§7) and
  before the maintenance lock (§3),
  `compaction_gate`, `write_lock`, the unique-key stripes and row locks, never
  while holding any of them, so no reader queues behind a gate wait. The
  sealer holds no writer-path lock (`compaction_gate`, `write_lock`, stripes,
  row locks, index write locks) across a capture block boundary, an
  fdatasync, a rename or the barrier (P15).
- Dropping a ticket is `fetch_sub(1)`, waking a sealer at zero.
- A seal is `fetch_or(SEALED)`, then a check of the count. Under a mandatory
  seal each ticket is either counted before the seal looks or sees `SEALED`
  and backs out. Under an optional seal a ticket may stay counted, and held,
  after the seal looked; the count at the `PUBLISHING` compare-and-swap is
  what then stops the publish. Either way **no ticket is held while the state
  is `Clean`**. A plain load can do neither: a writer whose load ran just
  before the seal could then mutate the index during capture with nothing left
  to reach `GatedIo`, and `CLEAN` would be published without its entry.
- A loom model test interleaves `ticket()`, drop, seal, `CANCEL`, a `GatedIo`
  write and retirement's seal cancel (§7 step 2, R8), asserting that no ticket
  is held and no `GatedIo` write is in flight while the state is `Clean`, and
  that `CANCEL` is never set on an unsealed word. Mutations that must fail it:
  either `fetch_add` replaced by a load; the `PUBLISHING` compare-and-swap
  ignores the count; the second RMW (writer or `GatedIo`) ignores the state or
  `SEALED`; retirement sets `CANCEL` with a `fetch_or`.

**Mutation reach (hook C10): the proof is in the signature** (RV3, RV8,
RV13). `pub(in crate::core::row)` cannot be written for items in
`core::index` or `core::filter` (E0742: the path must be an ancestor; checked),
and every ancestor that compiles admits `vacuum.rs`, `database/*` and
`executor.rs`. Instead every mutator of `LiveIndex`, `GinIndex`,
`TableIndexes` and `TableFilters` takes a proof (P14):

```rust
pub struct Proof<'a> { kind: ProofKind, _borrow: PhantomData<&'a ()> }
pub enum ProofKind { Ticket, Build }
impl<'a> From<&'a WriteTicket> for Proof<'a> { … }
impl<'a> From<&'a BuildToken> for Proof<'a> { … }
```

One byte, `Copy` and passed by value; the lifetime keeps the token borrowed
while the proof lives, and phase 1 needs only the kind. With no address in
it, a forged kind is a forged proof, so its fields are private, the two
`From<&token>` impls are its only constructors, and neither `Proof` nor
`ProofKind` implements `Default` or any conversion from a kind (the source
test pins all three).

`WriteTicket` and `BuildToken` have private fields and are defined in
`core::index_store::gate`. Neither implements `Clone`, `Copy`, `Default` or
`From`: a cloned `WriteTicket` would drop twice, and the second `fetch_sub`
would borrow into the state bits. Today's mutators are `LiveIndex` `insert`,
`remove`, `forget`, `clear` (`index/mod.rs:247-296`); `GinIndex` `insert`,
`forget`, `compact`, `clear` (`gin.rs:141-237`); `TableIndexes` `on_insert`,
`index_row`, `index_unreadable`, `on_replace`, `forget`, `clear`, `set_ready`
(`:707-818`); `TableFilters` `on_insert`, `on_insert_batch`, `on_delete`,
`backfill_row`, `backfill_batch`, `set_unindexed_prefix`,
`clear_unindexed_prefix`. v1 adds C9's `load_sorted`, the demotion flag's
setter and clearer inside `TableFilters` (§6), the generation-checked
readiness setter (§6) and the `cleared_below` restore (§2) (P11). The list is
the census's checklist, not the mechanism: the source test below is.
Visibility stays `pub`. `indexes()` (`table.rs:530`) and the catalog views
(`catalog_views.rs:990`, `:1056`) get a read-only `IndexView`. C10 proves the
caller holds *a* proof, not that it is this gate's or this structure's; that
the right gate was ticketed is the census's (V3) and `GatedIo`'s job.

Pinned by:
- **an in-crate source test**, in three parts (P9, P11, R6), parsing every `.rs`
  file under `src/` with `syn` (a dev-dependency), not grepping, and following
  `#[path]`. Two rules come first, because syn neither expands macros nor
  resolves names (Q16, Q17). **Macro bodies are read**: syn keeps every
  invocation's body as an unparsed `TokenStream` (`Expr::Macro`,
  `Stmt::Macro`), and `tokio::select!`, `join!`, `assert!`, `matches!` and
  `lazy_static!` all take expressions here. So for each invocation the test
  scans the tokens for `WriteTicket`, `BuildToken` and `Proof` anywhere, and
  for `unsafe`, `forget`, `ManuallyDrop` and `leak` inside the fns the
  no-`unsafe` rule below covers. It fails unless the invocation is on a short
  allowlist (`matches!`). It also runs the full
  visitor on whatever parses as a `Block` or a comma-separated expression
  list. **Names are pinned**: no `type` alias names `WriteTicket`,
  `BuildToken` or `Proof`, no `use … as` renames them, and no re-export gives
  them another name. Both tokens are braced structs with named fields
  (`Fields::Named`), so `Name(..)` and `Self(..)` cannot construct one.
  (1) **Construction, crate-wide.** Private fields are visible to
  `gate` *and all its descendants*, and a token can be handed out without being
  returned, so return types alone prove nothing. The test asserts: a
  `WriteTicket` or `BuildToken` struct literal, or `Self { .. }` inside an
  `impl` of either, appears only in the bodies of the three mints,
  `WriteGate::ticket`, the `BuildToken` mint (below) and one
  `#[cfg(any(test, feature = "test_tokens"))]` `test_mint`, whatever module the
  literal sits in (inline and file child modules of `gate`, the loom model's
  included, are scanned like any file; a `#[path]` or `include!` reaching
  outside `src/` fails the test); no `macro_rules!` body names either type; outside the mints and `Proof`'s variants, no item names
  either type in a return type (nested in `Option`, `Result`, tuples, `Box` or
  `impl Trait` included), a closure or `Fn*` bound (`FnOnce(&BuildToken)`,
  `FnOnce(Proof<'_>)`), a `Deref` target or associated type, a `static`,
  `const` or `thread_local!`, except the mutators' `Proof` parameters and a
  named allowlist of fields that store an already-minted ticket (the family's
  lock-owner record, §2); the `BuildToken` mint is called only from the named
  rebuild paths, and `test_mint` only from `cfg(test)` or `test_tokens` items
  (R10); no `impl` of `Clone`, `Copy`, `Default` or `From`, and no `derive`
  but `Debug`, exists for either token. **No `unsafe`** (Q18): no `unsafe`
  block or `unsafe fn` anywhere in `index_store`, in any fn that names either
  token or `Proof`, or in any fn that touches an allowlisted ticket-holding
  field. A structural rule, not a list: every way to duplicate a ticket
  (`ptr::read` and `.read*()` on a raw pointer, `ptr::copy*`, `transmute*`,
  `zeroed`, `assume_init_read`, a generic `dup<T>`) needs `unsafe`, and a
  duplicate decrements twice. The same fns may not name `mem::forget`,
  `ManuallyDrop`, `Box::leak` or `Vec::leak`, by path or as a method, since a
  leaked count is a seal waiting forever. A
  `pub(crate) fn with_build_token(f)` in `gate.rs`, a mint in `gate/loom.rs`,
  a guard with `Deref<Target = WriteTicket>` and a mint inside a
  `tokio::select!` arm each fail it.
  (2) Over **every** `impl` block, inherent and trait, of `LiveIndex`,
  `GinIndex`, `TableIndexes`, `TableFilters` and `ColumnFilter`, every fn **at
  any visibility, private ones included** (Q15), takes a `Proof` unless it is
  on a named read-only allowlist (`holds`, `sizes`, `plan*`, `lookup*`,
  `evaluate`, `stats`, `resident_bytes`, `for_each_sorted`, …) or on a named
  list of internal helpers, and the test checks that every caller of a
  helper takes a `Proof`. A visibility filter would skip exactly the callers
  C10 fences out: syn gives trait-impl methods `Inherited` visibility, and
  `pub(super)` on an item in `core::index` reaches all of `core`.
  `ColumnFilter` is listed because its own mutators (`set`, `set_prepared`,
  `set_reindex`, `set_hash`, `apply_delta`, `mark_checkpointed`;
  `column.rs:478-1030`) are reachable through `TableFilters.columns`. These
  types mutate through `&self` (`RwLock`s and atomics), so `&mut self` cannot
  pick out a mutator. A new method fails the test until someone puts it on a
  list. (3) **Private fields stay inside their impls.** Private fields are
  visible to descendant modules (`filter::{persist, sarg, column, …}`,
  `index::gin`, `index::tests`), so outside the five types' own `impl`
  blocks, no `ExprField` names their mutable fields: `columns` (a
  `Vec<RwLock<ColumnFilter>>`, `filter/mod.rs:365`), the `LiveIndex` and
  `GinIndex` maps and slot sets, `disk_dirty`, and so on. A new
  `filter/vacuum_clear.rs` calling `tf.columns[i].write().set_hash(..)`
  fails it;
- **one pair of doctests** in `gate.rs` (P13). rustdoc builds each doctest as
  an external crate against the library without `cfg(test)`, like trybuild, so
  these are signature checks: a `compile_fail` call of a mutator without its
  proof, and a twin that compiles and differs only in that line, taking the
  proof as a function parameter (`fn _f(ix: &LiveIndex, t: &WriteTicket) {
  ix.insert(Proof::from(t), …) }`), so it needs no mint. Stable rustdoc does
  not check `compile_fail,E…` codes (checked); the twin catches a doctest
  failing for another reason, such as a path that stopped resolving, so
  `index_store` and `gate` stay `pub mod`. There is no "token built outside
  `gate`" pair: every external crate fails on a private field, a `pub(crate)`
  field and a `pub(crate) fn new` alike, so it could only pin that the fields
  are not `pub`. Per-mutator coverage is the source test's;
- the census (V3).

**Callers without `cfg(test)`** need a mint too (P9, P28). Doctests,
`rasterizeddb_core/benches/filter_benchmarks.rs` (`on_insert` `:195`,
`on_insert_batch` `:199`, `:205`, `backfill_row` `:340`) and the
`rasterizeddb_filter_tests` crate (15 call sites in `src/lib.rs` and `tests/`)
link the library built without `cfg(test)`, where a `#[cfg(test)]` item does
not exist (E0425, checked). They use `test_mint` behind the non-default
feature `test_tokens`, enabled only where a server build cannot see it: the
bench target's `required-features` (run as `cargo bench --features
test_tokens`), and a `[dev-dependencies]` entry in `rasterizeddb_filter_tests`,
whose library helpers (`backfill_all`, `src/lib.rs:229-237`) take the proof as
a parameter. `cargo build --release` never carries it, the same arrangement as
`index_store_failpoints` (§11), but a server build *can* see it (R10): the
server is this package's bin target, and `cargo bench --features test_tokens`
or `cargo test --release --workspace` (dev-dependency features unify for the
whole invocation) rebuild `target/release/rasterizeddb_core` with the feature
on. So `main.rs` refuses to start, logging why and exiting non-zero, when
`cfg!(any(feature = "test_tokens", feature = "index_store_failpoints"))`; a
`compile_error!` would break the bench build of the bin. The benches change
only by that argument.

Callers today: `table.rs:714-726`, `:1143`, `:1177`, `:1858-1869`,
`:3395-3400`, `:4782`, `:4841`, `:4892-4895`; `vacuum.rs:996-998`, `:1074`;
`filter/sarg.rs:882-884`, `filter/mod.rs:1273-1274`; tests in
`index/tests.rs`, `gin.rs`, `filter/tests.rs`, `integration_tests.rs`; the
bench and `rasterizeddb_filter_tests` sites above.

**`BuildToken`: rebuilds take no ticket.** `build_indexes` (`table.rs:4861`),
`backfill_into` (`:4762`) and the open-time `set_unindexed_prefix` (`:1143`)
add entries to a structure that is not ready, which no snapshot can capture
(§3 eligibility), and change no data. Their `BuildToken` is minted only for a
structure that is unready at the current generation (P16): for
`TableIndexes`, `ready == false` (its initial state, `index/mod.rs:665`); for
`TableFilters`, demoted, or not yet installed in the `Table`. A fresh
`TableFilters` from `for_table` (`table.rs:1136`) has no unindexed prefix, so
`is_complete()` (`filter/mod.rs:799`) is true and a readiness test would
refuse the open path; the open path therefore mints the filter's token before
installing the filter, and the `set_unindexed_prefix` that makes it unready
runs under that token. The readiness test is made only at mint, so a token
also records the slot epoch it was minted at, and every batch it applies is
re-checked against it (Deferred work, below; Q9). A `BuildToken` appends no
`DIRTY` and is not counted by the seal. So the open future never writes the marker
log, and a read-only partition stays read-only. Persisting what a rebuild
produced is the clean-point rule's job (§3), not the gate's.

**2. A provider backstop for disk writes.** Every provider the partition owns
(rows, pointers, schema family, `_seq`, `.tmp`) is wrapped in `GatedIo<S>`.
Each write registers with the ticket RMW (`fetch_add` on the count, judged on
the returned word) and holds it until the inner write returns, so a seal's
count covers writes parked inside the provider (RV15). A plain load would
let a write checked before `SEALED` land after `CLEAN`. On the returned word:
- `Dirty`, not sealed: issued.
- `Dirty` with `SEALED` and `CANCEL`, without `PUBLISHING`: issued, no panic
  (P8). The seal can no longer publish, and a writer that preempted it (layer
  1) legitimately writes while the abandoning sealer still has `SEALED` set,
  for instance during a long fdatasync. A ticketless write in this window is
  caught only by the census journal (V3).
- `Dirty` with `SEALED`, without `CANCEL` or `PUBLISHING`, and a returned
  count of zero: no ticket exists anywhere, so the write is ticketless. Tests
  panic with its call site. In release builds it sets `CANCEL` by the same
  compare-and-swap rule as `ticket()` (only while the word still shows that
  unpublished seal; P7), so every seal, mandatory ones included, is cancelled
  before any `CLEAN` (§3 step 6), and the write is issued. With a nonzero
  count the write may belong to a ticket holder that a mandatory seal is still
  waiting for, so it is issued without a verdict: its registration keeps the
  count above zero, so the seal cannot publish until it returns, and a
  ticketless one is again the census's to catch.
  The one write a seal admits is the sealer's own `.tmp` landing (§3 step 2),
  made through its `SealGuard`.
- `PUBLISHING`: the write waits for the seal to end and re-checks, so after a
  published `CLEAN` it takes the slow path.
- `Clean` or `Unknown`: tests panic with the call site; release takes the slow
  path inline before issuing.

**A gate failure cannot be returned by the write itself:** `append_data_txn`
returns `(u64, u64)` and `write_data_txn` returns `u64` (`traits.rs:139-160`).
So `GatedIo` records it with `failed_appends::record(file, txn, e)`
(`file_sync.rs:509`, widened from `pub(super)`), skips the write, and
`commit_durable` fails the statement (`file_sync.rs:476-520`); nothing reaches
the file. Untokened writes come only from maintenance paths, which take a
ticket explicitly before their first write and fail on its error; if one
forgot, `GatedIo` records the error under `failed_appends::STAGED`, which the
next `commit_durable` reports, and skips the write.

**Ticket ownership** (P2). There is **one ticket per (family, file set)**,
where a family is a top-level transaction with its savepoints, identified by
its root (`row_locks` `owner_of`; savepoints are separate `TxId`s,
`pgwire.rs:1924-1934`, `:6055`). The ticket is stored on the family's
lock-owner record (§7), keyed by the root, taken at the family's first mutation
of the file set, and reused by every later statement and every savepoint;
`ROLLBACK TO` (which aborts the sub-id and opens a fresh one,
`pgwire.rs:6098-6112`) and `RELEASE` keep it. It is released with the family's
table locks and is the seal's writer count only, and nothing keeps a set of
families per slot, since §7's waits and deadlock edges come from the table
locks. An autocommit statement is its own family. Maintenance batches and
`GatedIo` registrations take transient tickets of their own and are not
families.

**Ticket lifetime.** A family that mutates a partition holds its ticket
until **its outcome is final and its post-commit work is done**:
- the outcome is final after the commit path's last durability step returns
  (`commit_written_transaction`, `flush_deferred`, `sync_own_location`;
  `pgwire.rs:4769-4801`), or after `abort_transaction` on a failure path;
- the post-commit work is the in-place bytes a WAL'd provider applies at
  commit, and `apply_pending_filters` (`table.rs:701-726`).

Otherwise an in-place write logged before a snapshot and committed after it
would be replayed by recovery onto data the snapshot claims to describe, and a
seal could complete between `commit()` recording `Committed` and a failed
flush recording `Aborted` (N26, §3). Holding the ticket makes the seal wait
through both.

**If `DIRTY` cannot be made durable** (ENOSPC, EIO), the gate unlinks
`.ixs` and fsyncs the directory, which needs no free blocks, and the write
proceeds with nothing left to protect. If that fails too, the mutation fails
before anything is issued, through `failed_appends` as above. Readers are never
failed.

**What passes the gate**: everything the snapshot captures. This list is the
census test's checklist (V3), not the mechanism: INSERT, UPDATE, DELETE with
post-commit applies and in-place overwrites; vacuum forgets and filter clears
(`vacuum.rs:981-998`, `:1065-1076`), `neutralize_aborted`, free-space reuse;
VACUUM FULL, ALTER rewrites, CREATE/DROP INDEX builds; sequence
paths writing `_seq.db`; replication apply; `.tmp` landing; reshape copies;
the `VacuumService` of a live or retiring instance.

**Maintenance takes a ticket only when it has a change to make** (RV2). A
reopen restarts vacuum with `cleared_below = 0` (`table.rs:831`, `:1270`), and
its first tick always sweeps (`vacuum.rs:1513-1517`). As written it would
re-clear every tombstone's already-empty codes, take a ticket, and make every
churned partition `DIRTY` with no user write. So `cleared_below` is persisted
in the FILTER section (a FILTER format change, so `persist::FORMAT_VER`
bumps), and `scan_pointers` collects clear and forget candidates outside its
synchronous closure and takes one ticket per batch only if some candidate
still has non-empty codes or `holds(slot)`. A pass with nothing to apply
writes nothing. A clear candidate is a reclaimable slot with `xmax == NO_TX`
**or** `xmax >= previous_bound` (R11): `should_clear` alone
(`vacuum.rs:658-662`) never again selects a rolled-back INSERT or a
neutralized tombstone (`xmax = NO_TX = 0`) once the persisted bound is above 0,
so their codes would stay until VACUUM FULL; the non-empty-codes check keeps
already-cleared ones ticket-free. Two orderings keep the persisted bound
honest (P23):
- **The bound advances only after every clear below it is applied in
  memory**: inside the last clear batch's ticket, or with no ticket when the
  pass found nothing to apply, since every code below it is then already
  empty. Today it advances right after the scan (`vacuum.rs:1345-1353`),
  which is safe only because the clears happen inside the scan; with them
  moved out, a seal between the advance and the clear batches would capture a
  bound above uncleared slots, and `should_clear` (`xmax >= previous_bound`,
  `vacuum.rs:658-662`) would never select them again.
- **Vacuum waits for the filter**: the `VacuumService`'s first tick is held
  until the load or the fallback rebuild has finished; the load restores the
  snapshot's bound with a plain store before vacuum starts; and the bound
  never advances while the filter is unready. Otherwise the first tick sees
  the unloaded filter's empty codes, applies nothing, advances the bound, and
  slots with `xmax` between the snapshot's bound and that horizon stay
  uncleared for good.

**Deferred slot- and offset-addressed work carries the slot epoch** (R2, Q9,
Q10). Three kinds of work choose slot numbers or byte offsets first and apply
them later:
- vacuum's clear and `forget_reclaimed` batches;
- vacuum's free-space output (`publish` and `quarantine`,
  `vacuum.rs:380-443`);
- every `BuildToken` writer outside `compact()`: the open-time filter backfill
  and index build (`table.rs:1150-1240`), the audit's in-place rebuild (§4) and
  the scheduler's rebuild after a failed compaction (§6).

None of them holds `compaction_gate` today. `run_once` (`vacuum.rs:1322`),
`vacuum_now` and the spawned `backfill_into` (`table.rs:1216`, `:4762-4841`)
all run beside `compact()`, which takes only `compaction_gate.write()`
(`table.rs:1592`). So a compaction can move live row R into dead slot S (or
dead extent E) and rebuild the filter, and afterwards:
- a clear batch's `on_delete(S)` (`filter/mod.rs:885-889`) sets `CODE_EMPTY`
  on every column of R;
- a stale backfill batch's `set_reindex(S, …)` (`filter/mod.rs:1094-1107`)
  writes a superseded version's codes over R's, and its closing
  `clear_unindexed_prefix` trusts a prefix that compaction's failed backfill
  left on purpose (§6);
- a pre-compaction extent published to the free list hands the next INSERT
  bytes that now hold R, and the INSERT overwrites them
  (`row_pointer.rs:2356-2361`).

In the first two cases, every pruned scan and the composite-UNIQUE probe miss
R, and the next clean point persists the bad codes. The free-space case is
**today-bug (e)**, fixed in phase 0. It needs no ticket wait: a pass whose
reader drain outlasts a compaction publishes with no check. Separately,
`FreeSpace::clear` (`vacuum.rs:447-450`), the only free-space call
`compact()` makes, clears `extents` but not `quarantined`, so `take_matured`
publishes pre-compaction extents on the next run.

So each table keeps an in-memory slot epoch, mirroring its `rdb.ixe` record
where it has one, and VACUUM FULL bumps it under `compaction_gate.write()`
before its first effect (§6). One rule covers all three kinds of work:
- the work reads the epoch under a brief `compaction_gate.read()` when it
  starts; a `BuildToken` records it at mint;
- each batch takes its ticket if it needs one, then `compaction_gate.read()`
  (the lock order holds), re-reads the epoch, and reads its pointers and rows
  and applies the batch inside that hold;
- **any change discards that batch and the rest of the work**: no bound
  advance, no `clear_unindexed_prefix`, no `set_ready`, no publish, no
  quarantine. Compaction has already rebuilt, cleared the free list and reset
  the bound. A pass with no clear or forget batch, the normal case once
  `cleared_below` persists, re-checks the epoch at its publish too;
- `compact()` clears `quarantined` as well as `extents` under its write hold.

`compact()`'s own rebuild runs inside its write hold, under the epoch it set,
and takes no `read()`, since tokio's `RwLock` is not reentrant. Failpoints
`vacuum_batch_after_compaction` and `backfill_batch_after_compaction`.
Mutations: drop the per-batch re-check; read the start epoch without the gate;
publish without the re-check; `compact()` leaves `quarantined`; a stale
backfill still calls `clear_unindexed_prefix`.

**`neutralize_aborted` takes one ticket per pass, before
`compaction_gate`** (P24). The pass holds `compaction_gate.read()` across its
candidate scan and every batch (`vacuum.rs:1850-1868`), so a ticket per batch
would be taken under that gate, against the lock order: a mandatory seal waits
for the ticket, `compact()` waits for the gate's write behind neutralize, and
readers queue behind compact's pending write. So the pass scans without a
ticket; if it found candidates it drops the gate, takes one ticket, re-takes
`compaction_gate.read()`, rescans, and applies its batches, checking `alive`
between them (§7 step 2).

**Cost:** one fdatasync per writing open session (~2-5 ms on consumer NVMe),
beside partitioning's own durable `DIRTY(pid, floor)` on a cold partition's
first write ([partitioning §7.2](partitioning.md#72-lazy-open-and-idle-close)
point 1); plus one at the clean point that rewrites a snapshot this open
rejected (§3).

## 3. Taking a snapshot

**Clean points.** Two optional ones: partitioning phase 3's exclusive idle
close (strong count 1), and the scheduler, for an open plain table with zero
outstanding tickets and none issued for `index_snapshot.idle_checkpoint_secs`
(default 300), so an idle plain table does not rebuild after a crash. Two
mandatory ones: database shutdown (below), and
`rdb_index_checkpoint(regclass)` (a function, since new grammar has repeatedly
mis-parsed here). A retired instance (§7) never snapshots.

**When to write one.** Every index and the filter must be ready, and one of:
1. the log ends in `DIRTY`;
2. this open did not load the existing snapshot (none, rejected, or `Unknown`);
3. something was rebuilt since the load (an audit mismatch, §4).

`rdb_index_checkpoint(t)` from a transaction whose family holds a ticket on
`t` returns a notice at once instead of waiting `seal_timeout` on its own ticket.

Otherwise the partition closes writing nothing. Rule 2 is what makes "a
version bump rebuilds once, then loads" true for a read-only partition: its
log still ends `CLEAN` after the rejection, and without rule 2 it would
rebuild on every reopen. When the state is not `Dirty`, the snapshot task
first takes and drops a `WriteTicket`, so `DIRTY` is durable before
`.ixs` is replaced.

**The maintenance lock** (P15) is a per-partition lock that no writer path
takes. A seal holds it, and only another seal and reload's retirement (§7
step 2) wait for it. `compact()` does not need it: its ticket cancels an
optional seal and queues behind a mandatory one (§2). Capture takes each
index's read lock for one block at a time and releases it between blocks
(§2 lock order), so a writer that cancelled the seal never waits on a lock
the sealer holds across an fdatasync, a rename or the barrier.

**Steps**, under the maintenance lock:
1. **Seal.** `fetch_or(SEALED)` (with `MANDATORY` for shutdown and
   `rdb_index_checkpoint`). An optional seal that finds outstanding tickets
   clears `SEALED` and skips at once. A mandatory one queues new tickets, which
   do not cancel it, and waits up to `index_snapshot.seal_timeout` (5 s) for
   outstanding ones, then skips, leaving `DIRTY`. While an optional seal
   runs, a ticket request sets `CANCEL` and proceeds (§2): **writers preempt
   optional snapshots**. The sealer checks `CANCEL` before each step below and
   between capture blocks, then abandons and unlinks `.ixs.tmp`; nobody waits
   for it.
2. **Land staged appends.** Through its `SealGuard`, the sealer lands every
   provider's `.tmp` durably (`land_staged_durably`, `file_sync.rs:1054`),
   which waits out a background landing already in flight. `flush_loop`
   (`file_sync.rs:2364-2376`) then has nothing left to land after the publish,
   and the identity's file lengths are final.
3. **Eligibility.** Every index ready; the filter ready and not demoted; no
   bulk or unlogged mode; `outcome_revocations` unchanged since open (below);
   `CANCEL` clear. Otherwise skip.
4. **Barrier: the one poison check** (RV4, P26). Stage outcomes
   (`status.flush()`), then sync the root log and every directory log the
   partition uses with a primitive that checks poison first
   (`commit_through(issued)` behind `is_poisoned()`), never `sync_pending`,
   which returns `Ok` on a poisoned log with nothing owed
   (`wal.rs:959-965`). In-memory forgets and filter clears were decided on
   in-memory outcomes; this makes them durable before anything depending on
   them is persisted. It is finding 1's barrier, once per snapshot. Failure,
   a poisoned log included, skips (failpoints `barrier_root_sync_fails`,
   `barrier_dir_sync_fails`, `apply_held_fails_then_shutdown`,
   `keep_tail_after_rename_fails`, `lazy_open_fails_after_marker`). A `commit_durable` error after
   the commit marker is written leaves memory without a key that replay
   restores (the dropped `pending_filters`, `table.rs:4013-4034`). The barrier
   relies on every such error poisoning the log, and **today one does not**
   (R7, checked at `7fc0dd0`): `finish_txn` (`wal.rs:732`) writes the marker
   (`issued += 1`), takes the held payloads out of `pending`, then calls
   `commit_through`, whose slow path opens `sync_handle` lazily with `?`
   (`wal.rs:916-921`) and returns the error without poisoning; only a failed
   `sync_data` (`:935-937`) and a failed `apply_held` (`:817-821`) poison. The
   keep-tail checkpoint resets `sync_handle` to `None` (`:1603`), so the first
   sync after each one reopens and can fail with EMFILE. A later committer's
   sync, or the barrier's own, then makes the marker durable, replay applies
   the new key, and a snapshot taken meanwhile is missing it. So **phase 0
   changes `wal.rs`, and the barrier keeps assuming it**: every error
   `commit_through` returns is a poisoned log. The handle is opened at
   `Wal::open`. The keep-tail checkpoint installs a `try_clone` of the file it
   reopens after the rename as both writer and sync handle, before it releases
   the writer lock. **Once `rename` has returned `Ok`, any failure before both
   are replaced poisons the log** (Q13): the directory fsync, the open, the
   seek and the `try_clone` (`wal.rs:1771-1777` on `feat/partitioning`, read,
   not run). Otherwise the writer and the cached handle stay on the unlinked
   old inode, so `commit_through` returns a false `Ok` that no assertion
   catches, and acknowledged commits are fsynced to a file the log path no
   longer names. `truncate_locked` (`wal.rs:1368-1385`) gets the same rule
   (Q19): it resets `write_pos` and `bytes_since_checkpoint` right after
   `set_len(0)` succeeds, and a failed `seek` or `sync_all` after that poisons
   (fsyncgate). Otherwise in-place payloads record `log_offset`s past the real
   bytes, and a later cut-back `set_len(write_pos)` leaves a zero gap that
   replay stops at, dropping every later commit. The lazy open, kept as a
   fallback, poisons on failure. A debug assertion checks that
   `commit_through` returns `Err` only when `is_poisoned()`. Hence v1 keeps no
   separate `diverged` flag and no poison check at step 3: either would mask
   this one in mutation testing, and a partition reopen inside the process
   replays nothing that could clear a flag.
5. **Capture.** Sample the identity (§4), then stream every index through C3
   and the filter through `persist::serialize_sealed` into `.ixs.tmp`, checking
   `CANCEL` between blocks and holding each index read lock for one block only
   (P15). Capture issues `sync_file_range(SYNC_FILE_RANGE_WRITE)` every
   `index_snapshot.writeback_mb` (default 64) behind the token bucket, so step
   6's fdatasync has little left to write and a large snapshot never queues
   gigabytes of writeback ahead of a writer's commit fsync on the same device
   (P12). **C3 emits every listed entry and unkeyed slot,
   except the gin entries of `forgotten_slots`, exactly those; nothing filters
   by the presence bit** (owner's invariant 3, de3651a). Exactly, not "at
   most": a forgotten slot whose items were kept would come back after a load
   with its presence bit clear and `forgotten_slots` empty, so `holds` would be
   false, vacuum would never forget it again, and every later snapshot would
   copy its items forward (N37).
6. **Publish (T3).** fdatasync `.ixs.tmp`; rename to `.ixs`; fsync the
   directory; compact the marker log if due (§1), still before `PUBLISHING`;
   re-read `outcome_revocations` and skip if it moved; set `PUBLISHING` by a
   compare-and-swap that requires `CANCEL` clear **and a ticket count of zero**
   (P7). Under an optional seal any count above zero is a writer between its
   two RMWs or already held, so the seal skips as if cancelled; a mandatory
   seal retries while the count reflects only transient back-outs. Then append
   `CLEAN(identity, len, checksum)`; fdatasync.
7. **Unseal.** On success the state becomes `Clean` and the filter's columns
   are `mark_checkpointed()` (§5). A failure before `PUBLISHING` leaves the
   state `Dirty` and unlinks `.ixs.tmp`. **A failure after `CLEAN`'s write was
   issued** (its fdatasync, or anything later) sets the state to `Unknown`,
   never `Dirty`, before any waiter is released: writeback may persist the
   `CLEAN` page now or later, and Linux may keep or drop pages after an fsync
   EIO, which is why the WAL poisons itself on a failed flush
   (`wal.rs:1041-1049`). The next ticket's slow path then rewrites the log or
   unlinks `.ixs`. Then `SEALED`, `MANDATORY`, `PUBLISHING` and `CANCEL` clear and
   waiters wake. Every skip or failure counts `rdb_index_snapshot_failed_total`.
   A cancelled or skipped seal releases queued tickets the same way.

**Outcome revocation (N26).** The commit path can record `Committed`, fail
`flush_deferred` or `sync_own_location`, then record `Aborted`
(`transaction.rs:907-919`, `pgwire.rs:4769-4801`). A forget taken in between
removes a version that is visible again; restart heals that today, a snapshot
would not. The preferred fix is the commit path's owner's: fail-stop once
`commit()` ran, and assert in `StatusLog::record` that `Committed` never
becomes `Aborted`. Until then v1 adds a process-wide `outcome_revocations`
counter, bumped when an abort overwrites `Committed`, checked at step 3 and
re-read at step 6 immediately before `PUBLISHING`. Because a family holds
its ticket until its outcome is final (§2), no seal can complete inside the
window between `Committed` and the revoking `Aborted`, and the flip is seen
before any `CLEAN` (failpoint `seal_between_commit_and_failed_sync`).

**Shutdown.** Sessions go first (RV17): after pgwire's drain grace
(`DRAIN_GRACE`, 10 s, `pgwire.rs:577-612`) the sessions left are terminated
with 57P01 and their open transactions aborted, releasing their tickets,
before any seal. Otherwise a lingering writer is queued, released after
`CLEAN` and re-dirties the partition, and an idle-in-transaction session
makes every partition it wrote wait `seal_timeout` and skip. Today
`shutdown::finalize` runs beside live sessions (`main.rs:1244`). Termination
never interrupts a statement after its `commit()`: it waits for that commit
path to finish, since `outcome_revocations` is process-wide and one
revocation would void every remaining seal. **The other writers stop next**
(P30), because each passes the gate (§2) and a ticket queued behind a
shutdown seal is released after `CLEAN` and re-dirties it: clear `alive` and
join every `VacuumService`; stop replication and follower apply; let
`flush_loop` finish its landing. Then shutdown snapshots every partition the
rule selects, in parallel
up to the device's bandwidth and with no token bucket, ordered by rebuild cost
saved per byte written (rows × indexes ÷ snapshot bytes), under
`index_snapshot.shutdown_deadline` (default 120 s), which includes any
`seal_timeout` waits. That deadline must stay
below the unit's `TimeoutStopSec`; `deploy.sh` should set it to the deadline
plus 60 s, a change for that script's owner. Whatever the deadline leaves is
skipped, counted in `rdb_index_snapshot_skipped_shutdown_total`, and rebuilds
after the restart. At a = 5%, 382 written partitions × ~120 MB is ~46 GB per
process and ~275 GB per machine of six: ~90 s at 3 GB/s of machine write
bandwidth, but ~7.5 min on a 600 MB/s consumer disk, where the deadline skips
most of the set.

**Cost.** An optional seal stalls a writer for its `PUBLISHING` window, one
`CLEAN` append and fdatasync (~2-5 ms on NVMe; one small-record fdatasync on
any disk), plus I/O already in flight that cancellation cannot recall (P12,
P31). A writer arriving earlier cancels the seal and needs no gate wait,
whatever step the sealer is in, but two couplings remain. A staged append
still waits out a landing already in progress: step 2's `flush_temp_to_main`
holds `append_blocked` across `main.sync_all()` (`file_sync.rs:1096-1114`),
and appends yield and reads spin on it (`:1821`, `:1399`), exactly as they do
behind `flush_loop` today. And a writer's commit fdatasync shares the device
queue with step 6's fdatasync of `.ixs.tmp`, which cannot be cancelled once
issued; under ext4 `data=ordered` its journal commit can also have to write
ordered data blocks of the snapshot. Step 5's periodic
`sync_file_range(WRITE)` keeps what is left for that fdatasync small. **RV7 is
closed at the gate and measured, not claimed, at the device.** The
measurement (phase 1, ext4 on the consumer disk at 192.168.1.7 and tmpfs,
both reported): autocommit single-row INSERTs into a bigint-PK table,
`synchronous_commit = on`, on the same device as a 12M-row, three-index plain
table that takes an optional snapshot every 30 s; five alternating 60 s runs
with and without snapshots. Reported: commit p50, p99 and max; the time spent
in `ticket()`'s slow path, separately; and device flushes from
`/proc/diskstats`. It passes when the gate wait's p99 is at most one measured
`CLEAN` fdatasync and the commit p99 with snapshots lies within the
no-snapshot runs' range plus one such fdatasync. Otherwise RV7 stays open and
`writeback_mb` and `max_mb_per_s` are lowered until it passes, or optional
snapshots are disabled on that device class. A
busy plain table that never goes quiet takes no optional snapshot and
rebuilds after a crash, as today. A mandatory seal stalls writers for its whole
run: ~0.2-1 s for a 1.53M-row partition (estimate), and seconds to tens of seconds
for a 100M-row plain table with three indexes (a 6-9 GB snapshot). That is why
only shutdown and an explicit call are mandatory. A per-device token bucket,
`index_snapshot.max_mb_per_s`, bounds optional snapshot I/O. When the bucket
is empty, the evictor prefers clean or read-only victims, which close for
free. It closes a written partition without a snapshot only when no clean
victim exists, and counts that in `rdb_index_snapshot_skipped_evict_total`, a
v2-trigger signal (§16). Otherwise memory pressure would turn cheap reopens
into rebuilds just as the system churns.

## 4. Opening

The snapshot is trusted only if **every** check passes; cheap checks run in
the open future, the rest at load.

| Check | Catches |
|---|---|
| `.ixm` readable; last good marker is `CLEAN`, nothing after it | a write after the snapshot; a crash anywhere in the publish; an unreadable log (state `Unknown`) |
| `CLEAN`'s identity, length and checksum equal `.ixs`'s header and file | a stale or half-renamed snapshot under a newer `CLEAN`, or the reverse |
| no `<t>_flt.db` beside an existing `.ixm` | a pre-v1 binary opened the table since (each recreates it at open, `table.rs:516-520`) and may have written in place, which no length shows (RV5) |
| `incarnation` equals `_schema.db`'s (C7) | DROP + CREATE TABLE, or an ALTER rewrite, under one name |
| `slot_epoch` equals the WAL-recovered `rdb.ixe` record | a renumbering the marker log never saw: point-in-time replay, reseed, a `DIRTY` bug |
| `n_slots` and pointer and rows file lengths equal the recovered files | appended or truncated data, lost unsynced tails |
| `format_ver`, `key::ENCODING_VERSION`, `TEXT_RENDER_VERSION`, `ITEM_DERIVE_VERSION`, `GIN_HASHER_ID`, `FILTER_DERIVE_VERSION` and `persist::FORMAT_VER` equal the binary's (§8) | format, encoding, rendering, navigation, gin item or filter derivation drift |
| `shape_hash` equals `for_schema(schema).shape_hash()` (C6); filter configuration hash equal | changed declarations, dedup survivor, type or scale, filtered columns or width |
| At load: section CRCs, footer xxh64, strictly increasing slots per key and item, well-formed `UNKEYED` and `PRESENT` | torn or rotted bytes; owner's invariant 4 |

**The shape hash** covers, per index in `for_schema` order: kind, method,
unique, implicit, `(write_order, DbType, scale)` per column, path steps and
cast, opclass; never a name. All-or-nothing per partition, v1 needs no
per-index identity, `constraint_epoch` or orphan retirement (17, N38, N39).

**Load at the routed open.** A routed open runs the cheap checks and **starts
the load at once**, single-flight on the scheduler. Routed readers wait for it
up to `index_snapshot.load_wait` (default 0.6 s, twice phase 2's load gate)
and fall back to a scan only on a load error or that timeout. Waiting on first
use instead would cost more: the first reader would scan the partition
unpruned, ~395 MB against a ~120 MB load, above phase 4's first-touch target.
A write waits for the load (the snapshot equals data at open, so it stays a
valid base after `DIRTY`); on a load error it proceeds into the unready
structures while the fallback rebuild runs, as writes do today.

**Fan-out opens read only the filter.** A scan-only fan-out open runs the
cheap checks and, if they pass, `pread`s only the FILTER section, using the
section table's offset and length and checking that section's crc32c, for
read-only pruning. It never reads the index sections. If the proof fails, it
scans unpruned. This keeps the filter pruning that partitioning and sharding
assume now that `_flt.db` is gone (partitioning §7.1, §7.2 point 4; sharding
§11.6).

**The load** streams each section into staging structures, checking each
section's crc32c, and checks the footer xxh64 on the same pass. It never
buffers the whole file, and a failure discards the staging. It builds each
index from the sorted stream (C9), restores `PRESENT` and `UNKEYED`, loads the
filter with its `cleared_below` (§2), and sets readiness. A gin load
recomputes `live` (popcount of `PRESENT`) and `entries` (sum of list
lengths), with `forgotten = 0` and `forgotten_slots` empty: `live` changes
only in `set_present`, so a load writing bitmap words directly would leave it
0, and the first forget would wrap it (`gin.rs:115-200`, RV19). Concurrent loads and audits share a semaphore,
`index_snapshot.max_concurrent_loads`, sized from the resident budget, and
staging bytes count against `partition_resident_budget` (§9). Without that
cap, 382 first touches after a restart would hold ~46 GB of buffers per
process.

**Fallback.** Any failed check or load error rebuilds as today
(`build_indexes`, `table.rs:1136-1160`; filter backfill, `:1173-1240`), under
`BuildToken`s, unique and implicit B-trees first. Queries scan meanwhile.
UNIQUE, PK and ON CONFLICT probes (`find_row_with_value`, `table.rs:2244`)
also scan until their B-tree is ready, since the filter is rebuilding too
(RV18, §5 Cost). The
open future never fails because of the index store and never writes the marker
log (N41). The next clean point persists the rebuild (§3 rule 2).

**Audit.** The proof checks what someone thought of, so 1 in 64 loads
(`index_snapshot.audit_rate`) also builds a scratch copy from rows in the
background and checks **containment in the dangerous direction**, not
equality (RV9, RV11). Live structures legitimately hold more than a rebuild:
zone bounds only widen (`column.rs:638-643`), an in-place gin update keeps the
old items (`index/mod.rs:780-800`), and a rebuild re-adds versions vacuum
already forgot, since their pointers stay un-`deleted`
(`row_pointer.rs:507`). Over slots not reclaimable under the status log:
every rebuilt `(key, slot)` is in the snapshot or the slot is in `UNKEYED`.
**The filter is recomputed, not compared code for code** (P18): a slot's code
is `fingerprint(h, f)`, with `f` and planelessness chosen per zone from
history (`column.rs:706-721`; `gen_index_for`, `:559-590`). The live filter's
HLL counts values in completion order and keeps values an in-place UPDATE
overwrote, and updates widen its bounds, so a scratch backfill in slot order
can pick another width for the same zone and its codes are not comparable.
Instead, for each such slot in the snapshot's trusted range (below its
`valid_upto`, outside its unknown range), the recovered row's value is hashed,
and the snapshot's code must equal the code the snapshot's own width for that
zone gives it (`fingerprint(h, f_snapshot(zone))`, or `CODE_NULL`), unless
that zone is planeless or gated out; the snapshot's SuperBloom must report
`may_contain(superzone(slot), h)`; and its zone bounds must contain the value.
No scratch filter is built. A failure is a **mismatch**; differences in
widths, planes or HLL state are soft divergence. Any other difference is counted in
`rdb_index_snapshot_soft_divergence_total` and nothing else happens.
- It runs **only while the gate stays `Clean`**, meaning no ticket since the
  load, so nothing moves under the comparison. It is abandoned at the first
  ticket, and at retirement, which cancels it before it can drop a verdict
  (§7).
- A pass discards the scratch.
- **A mismatch never swaps the scratch in.** A separately built structure lacks
  the entries that writes add to the live one after the build has passed their
  slots. Instead the audit takes a `WriteTicket`, so `DIRTY` is durable and the
  bad snapshot is never loaded again. Then it demotes (`set_ready(false)`,
  `filters_ready = false`, generation bumped), clears, and rebuilds in place
  exactly as `build_indexes` does today, into the installed structures that
  writes keep feeding. It logs, counts `rdb_index_snapshot_mismatch_total`, and
  the next clean point rewrites the snapshot (§3 rule 1).
- The scratch copy doubles the partition's index RAM while it runs. It takes
  the load semaphore and counts against the resident budget.

`rdb_index_check(regclass)` does the same on demand.

## 5. The filter under the same rule (T4)

Today the sidecar is trusted when its dirty byte is clear. Deletes and updates
set it via `mark_disk_dirty` (`filter/mod.rs:675-691`); the background task
checkpoints on a threshold or request. Three paths break that trust:
- **Bug (c):** vacuum clears codes from in-memory outcomes
  (`vacuum.rs:1065-1076`) and a later checkpoint persists them and clears the
  byte with no root-log sync between. Power loss undoing the deleter leaves a
  trusted sidecar pruning a visible row, on any table whose directory has its
  own WAL.
- **Bug (d), the filter half of (a):** after a failed VACUUM FULL, a checkpoint
  writes old-numbered planes as clean (N5).
- **The checkpoint error bug:** the checkpoint writes through `write_data`,
  then clears `disk_dirty` and sets `has_checkpoint` whatever happened
  (`filter/mod.rs:656-667`). Fixed in phase 0 (`try_checkpoint`).

Until v1 replaces it, the runtime checkpoint logs its whole base image as one
WAL record, so a sidecar over the WAL's 256 MiB `MAX_RECORD_PAYLOAD`
(`wal.rs:144`) fails every checkpoint and is rebuilt at each open. That limit
predates phase 0; `.ixs` sections are not written through the WAL.

v1 makes the filter an `.ixs` section under the one rule. It is written only
at a clean point after seal and barrier, which closes (c) and (d)
structurally. For **every** table, whatever `index_storage` says, the runtime
checkpoint loop (`table.rs:1225-1236`) and the dirty byte go, and `_flt.db` is
never trusted or created again (`table.rs:879`, `:1005`):
- **Every** v1 open where `_flt.db` exists deletes it before any write, not
  only the first. A pre-v1 binary run in between recreates it and checkpoints
  it with the byte clear; v1 does not maintain the byte, so a second downgrade
  would trust stale fingerprints (§10). The delete goes **through the
  provider family** (RV20): a KV delete for a KV table, every location of a
  mirrored pool, not a raw unlink of the primary's copy. **If `.ixm` exists,
  the `_flt.db` is evidence** (RV5) that a non-v1 binary opened the table and
  may have overwritten in place: the proof fails, and `.ixs` is unlinked and
  the directory fsynced **before** `_flt.db` is deleted, so a crash between
  them cannot erase the evidence and leave a stale `CLEAN` trusted (failpoint
  `flt_evidence_before_ixs_unlink`). **If the `.ixs` unlink or its directory
  fsync fails** (EIO, EACCES), `_flt.db` is kept and the gate starts
  `Unknown`, whose slow path makes the log durably `DIRTY` or `.ixs` durably
  gone before it deletes `_flt.db` (§2, R9). If the delete fails, the gate
  starts `Unknown`, whose slow path retries it, and a mutation fails rather
  than write beside a surviving `_flt.db`. The word leaves `Unknown` only
  once that delete returns `Ok` (§2, Q12).
- **`_flt.db` stays reserved** (RV6): it stays in `RENAMED_SUFFIXES`, so
  `table_files_conflict` keeps refusing a table `<t>_flt` beside `<t>`. For a
  legacy pair that older builds accepted, where `<t>_flt.db` is another
  table's rows, v1 never deletes it. That table takes no snapshot (logged
  once per open), since the evidence row cannot be read. **The test uses the
  full catalogue name set, not the live map** (P19): `possible_owners` and
  `table_files_conflict_beside` consult `self.tables` (`database/mod.rs:351-356`,
  `:2489-2497`), but at startup `Database::new_named` opens tables one by one
  in catalogue order and inserts each only after it opened
  (`database/mod.rs:1110-1179`), and skips unopenable backends and unreadable
  schemas entirely. So the open receives the catalogue's whole `tables_names`
  set, unopenable entries included, and a `_flt.db` any catalogue name could
  own as its rows is never deleted, on files and through KV providers alike.
- **Excluded tables** (KV-backed, `wal_enabled=false`) take no snapshot and so
  keep no persistent filter: it is backfilled from rows at every open. They
  have no gate, so a failed `_flt.db` delete sets a per-table pending flag:
  the next write retries the provider-family delete, single-flight, and only
  that write fails if it fails again (P33). Plain KV tables do not idle-close,
  so waiting for "a later open" would leave the table unwritable until a
  restart, against V12.
- **`filter.backfill_on_open = off`** leaves the unknown prefix standing, so
  the filter is never ready and §3 refuses the table's snapshot instead of
  persisting the prefix at every clean point (`persist.rs:209`). Phase 1 warns
  when the setting is used.

**The frontier.** `persist::serialize` writes `persistable_valid_upto() =
min(valid_upto, dirty_min)` (`persist.rs:207`, `column.rs:466-468`).
`dirty_min` is lowered by any vacuum clear, frozen DELETE or in-place UPDATE
(`column.rs:750-753`, `filter/mod.rs:885-889`) and reset only by
`mark_checkpointed()` inside the runtime checkpoint (`filter/mod.rs:629`),
which v1 removes. Reused as it is, every snapshot would record the frontier at
the first such slot. Every clean reopen would then backfill from there
(`resume_slot`, `table.rs:1196-1197`), breaking phase 1's "no backfill after a
clean reopen" gate and §9's reopen cost. So the FILTER section is written by
**`serialize_sealed`**, which records `valid_upto` and `unknown_below` as they
stand: under the seal, memory equals data. A successful publish calls
`mark_checkpointed()`. `dirty_min` and `touched_min` can go with the runtime
checkpoint, which is 99's call.

**Cost** (accepted, decision 12): after a crash, a written partition's filter
backfills from zero, where today an insert-only table resumes. Queries stay
correct, unpruned until done. Writes pay too (RV18): until the unique B-tree
is ready, every PK, UNIQUE or ON CONFLICT probe walks the whole pointer file,
for roughly a full index build where today it is roughly the tail's backfill.
Every open that backfills logs the rows and seconds taken and exports
`rdb_filter_backfill_rows_total` and `rdb_filter_backfill_seconds_total`. If
huge insert-only tables make this painful, "trusted tail resume" becomes a v2
requirement (R14) with its own durability argument. KV-backed and
`wal_enabled=false` tables backfill at every open (§14).

## 6. Renumbering and other invalidations

`compact()` (`table.rs:1581`) renumbers in place, committing phase 2 in
batches (owner's invariant 6). Before its first effect:
1. take a ticket, so `DIRTY` is durable (replacing `mark_disk_dirty`), before
   `compaction_gate.write()` (§2 lock order: readers never queue behind a
   seal through compact's exclusive hold, RV16);
2. under the exclusive gate, append `epoch + 1` to `rdb.ixe` as its own WAL
   transaction and sync the directory log, so any replay containing an effect
   contains the bump, and bump the in-memory epoch that deferred vacuum
   batches, free-space publishes and `BuildToken` batches re-check (§2, R2,
   Q9, Q10); clear `quarantined` along with `extents`;
3. **demote**: `set_ready(false)`, `filters_ready = false`, bump the table's
   in-memory slot generation.

Then today's rebuild and backfill run under the gate, through `BuildToken`s on
the demoted structures. **Success means compaction, rebuild and backfill all
returned `Ok`.** Today `compact()` sets `filters_ready` even when
`backfill_filters_from` failed (`table.rs:1846-1849`). `backfill_into` returns
early on an iterator error and skips rows whose fetch fails (`continue`),
after which `clear_unindexed_prefix` trusts their stale old-numbered codes
(`table.rs:4762-4841`). So, from phase 0:
- the compaction backfill first sets an unindexed prefix `[0, n)`;
- a backfill that skipped a row or failed returns `Err` and leaves the prefix
  in place;
- the demotion clears, and generation-checked readiness is set, only after a
  backfill that returned `Ok`.

A failed backfill leaves the partition demoted, so §3 refuses the snapshot and
the next open rebuilds.

Step 3 is **today-bug (a)**: `compact()` returns `Err` from a later batch
(ENOSPC on a nearly full disk) and the index and filter keep serving old slot
numbers. Three parts close it:
- **Refusal at the source** (N17): the demotion flag moves inside
  `TableFilters`, whose `evaluate` and zone-map reads answer nothing while it
  is set, covering `filter_mask` (`table.rs:3163`),
  `find_row_with_value` (`:2650`), the composite-UNIQUE `find_row_with_tuple`
  (`:2306`) and `monotonic_scan_ceiling` (`:3045`). An unready `TableIndexes`
  already answers `None`.
- **Generation-checked readiness** (N5): an async build or backfill
  (`table.rs:1150-1158`, `:1222`) captures the generation at start and sets
  ready only if it is unchanged. That alone does not stop its writes, so each
  of its batches also re-checks the slot epoch under `compaction_gate.read()`
  (§2 Deferred work, Q9).
- **No snapshot while demoted** (§3 step 3): `DIRTY` stands, the next open
  rebuilds, and the scheduler queues a rebuild under the new numbering.

The rest, each taking the ticket first:
- **TRUNCATE does not exist in the engine** (Q11): no parser or executor arm,
  only a DDL-in-block warning prefix (`pgwire.rs:4570`). v1 specifies no
  TRUNCATE path. When it is added, it is an AE reload (§7), and what protects
  deferred work against it is retirement's `VacuumService` join and the new
  instance's fresh structures, not the epoch.
- **ALTER rewrite** (`rewrite_table_with_columns`, `rql/executor.rs:2129`;
  `retype_column`, `:3079`): one reload whose step-3 work is the whole rewrite,
  under one AE (§7, P3), each version keeping its visibility (R5). `DIRTY` on
  the old table before its drop; the new table has a new incarnation and a new
  gate word; declarations are carried by column name, path indexes included
  (owner's invariant 7), as today.
- **CREATE/DROP INDEX, path-index DDL, other ALTER arms**: reload (§7).
- **RENAME and MOVE TABLE**: `DIRTY` on the source before copying; `.ixs` and
  `.ixm` are not copied, so the target rebuilds and a rolled-back copy leaves
  the source `DIRTY`. They take AE like a reload (§7), so only the caller's
  family can still hold a ticket, and the word moves with the file set to the
  target's slot (§2): its state is `Dirty`, which a target with no `.ixm`
  derives anyway, and its count keeps the caller's ticket counted by the
  target's seals until the outcome is final (N26).
- **DROP TABLE** deletes `.ixs`, `.ixm`, `.ixs.tmp`, `.ixm.tmp`. **Reshape** targets,
  **restores**, **reseeds** and **relocations** have no snapshot and rebuild.

There is no remap in v1 (it needs the order proof and debt handling of N19).

## 7. Reload: DDL takes ACCESS EXCLUSIVE (owner's items 10, 14)

`reload_table` (`database/mod.rs:3153`) removes the map entry and opens a
second `Table` over the same files with no wait. A statement still holding the
old instance, such as a long INSERT … SELECT, writes into indexes nobody reads:
a wrong answer today that restart heals (finding 4), and that v1's snapshot
would persist. So every DDL that reloads, rewrites, renames, moves or drops a
table follows PostgreSQL's lock rules through **one owner-keyed table-lock
manager**: it waits for every transaction holding the table, a transaction
that already holds it goes ahead of a waiting DDL, and a real cycle ends in
40P01 (P1, P17). The previous revision's slot queue with its run-ahead and
own-family exemptions hung every extended-protocol client (re-check R1):
Describe resolves tables with no transaction (`handle_describe`,
`pgwire.rs:2485`; `param_infer::infer`), so it queued behind a drain that
waited on its own block's ticket, and `set_waiting` drops `FROZEN_TX` waiters
(`row_locks.rs:607-611`). The exemptions go. Anchors new in this revision are
from `feat/secondary-indexes` at `7fc0dd0`, where `reload_table` is
`database/mod.rs:2493`.

**Modes.** One lock per table name, in the table space `RowLocks` already keeps
for `LOCK TABLE` (`TableLockMode`, `row_locks.rs:55`; `table_blockers`, `:584`),
widened to four modes. A PostgreSQL mode with no exact match maps to a
stricter one, never a weaker one.

| Mode | Taken by | Conflicts with |
|---|---|---|
| AS (ACCESS SHARE) | SELECT; the FK parent check | AE |
| RX (ROW EXCLUSIVE) | INSERT, UPDATE, DELETE, COPY FROM, `SELECT … FOR UPDATE`, replication apply, VACUUM FULL (which keeps `compaction_gate` inside) | S, AE |
| S (SHARE) | `LOCK TABLE … SHARE` | RX, AE |
| AE (ACCESS EXCLUSIVE) | every reloading DDL: CREATE/DROP INDEX, path-index DDL, every ALTER arm, DROP, RENAME, MOVE, a JSON shadow type widening (below; a shadow ADD COLUMN takes none); `LOCK TABLE` in EXCLUSIVE, SHARE ROW EXCLUSIVE or ACCESS EXCLUSIVE | all |

CREATE INDEX under S, as PostgreSQL runs it, is v2.

**Owner.** A lock is held by the family root (`owner_of`, `row_locks.rs:241`)
and requested through a **`LockOwner` newtype that cannot be built from
`FROZEN_TX`** (Q1). It is minted only at BEGIN, in `execute_recorded_inner`
and for named internal owners, and `table_for` takes nothing else, so
`FROZEN_TX` can never hold or request a table lock. If it could, it would be
one owner shared by every session: rule (b) below would let any `FROZEN_TX`
request jump a waiting AE because another session's `FROZEN_TX` holds the
table, and nothing ever calls `release_all(FROZEN_TX)`. A debug assertion in
`set_waiting` still checks that a `FROZEN_TX` waiter holds nothing, since its
edge would be dropped.

Blocks get a `TxId` at BEGIN, autocommit statements one per attempt
(`execute_with_sync`). New: an autocommit statement, DDL included, gets its
owner O before its first lock, in `execute_recorded_inner`
(`pgwire.rs:4468`). **O is a real top-level id begun in `TransactionManager`**
(`begin_autocommit`; Q5). Every table-lock conflict check filters holders
through `live`, meaning an outcome of `InProgress` (`executor.rs:4644-4649`,
`:4836-4841`; `table_conflict` and `blockers_of`, `row_locks.rs:480-530`). An
id that was never begun, or one the bounded status log has forgotten, may not
read as live, and then every lock the statement holds is invisible: a CREATE
INDEX is granted AE beside a running INSERT … SELECT, which is finding 4
again. So O reads `InProgress` for the statement's whole life, and the status
log never forgets an in-progress id. Each retry attempt's `TxId` maps to O
through `RowLocks::owner_alias`, which `owner_of` consults, so the lock and
its graph node survive retries, and `deadlock_victim` ages the statement by
O. A guard ends O (`release_all`, then its outcome) on every exit path,
cancellation and panic included. When a reload runs on its spawned task
(step 2), the task takes O over and ends it after the swap.

**Every sub-execution of a statement runs under the statement's owner**
(Q1): CTE bodies, derived-table bodies, `catalog_views::materialize` and its
builders, `drop_temps`, `run_returning_rows` and `run_statement`,
`json_default_reference`, `run_ddl` and `substitute_in_subqueries`. Today an
autocommit statement without `WITH` runs derived bodies, catalog relations
and `drop_temps` under `FROZEN_TX` (`pgwire.rs:4540`, `:4612-4729`;
`cte.rs:444-449`). One with `WITH` runs its CTE bodies under a separate
`begin_autocommit()` id, and `run_returning_rows` under `FROZEN_TX` calls
`execute()` with a fresh owner (`executor.rs:546-550`). Under two owners, one
task deadlocks with itself and no edge shows it. For example,
`SELECT * FROM t1 JOIN (SELECT * FROM t2) d ON …` takes AS on {t1, t2} as O,
and a CREATE INDEX ON t2 queues for AE. The derived body's `table_for(t2)`
under another owner holds nothing on t2, so rule (b) does not apply, and it
queues behind the AE. O waiting on its own body is an await inside one task,
not a lock edge, so no cycle is ever found. So the statement's `WITH`
transaction **is** O: one `begin_autocommit`, not two.

**As built (B2).** Owners are threaded; nothing new takes a table lock yet
(only `LOCK TABLE`, in its old two-mode space, until B3b routes lookups
through `table_for`).
- *Where O is minted.* The owner rides in a task-local scope
  (`core/mvcc/owner.rs`), not in parameters, so every sub-execution finds it
  however its call reached it. The wire opens the scope in
  `pgwire.rs:execute_recorded`, around `execute_recorded_inner`: the open
  block's root (`owner::under`), or for an autocommit statement a lazy O
  (`owner::statement`), begun the first time something asks
  (`owner::ensure`: the engine's attempt loop now, every `table_for` from
  B3b). `execute_with_sync` opens a scope only when none is in place, which
  is how the native protocol, login lookups, the importer's DDL and any other
  `execute()` caller get one (finding L3), and `execute_in_txn` falls back to
  its transaction's root. A nested entry point always joins the scope it
  finds. `SET`, `SHOW`, `BEGIN` and other statements answered above the
  engine begin no O.
- *O is an owner-only id.* `TransactionManager::begin_owner` begins O in
  `running` (so it reads `InProgress`, holds `oldest_active`, and the status
  log can neither settle nor forget it: `settle_now` and `try_forget` assert
  the settled horizon stays at or below the oldest running id) and marks it
  owner-only: it never snapshots, reads or writes, so the in-place gate looks
  past it (counting it would close the gate for every autocommit statement).
  `end_owner` commits it with no durable record. A `WITH` statement's
  transaction is adopted as O (`owner::adopt_transaction`), a regular
  transaction the statement ends itself.
- *Release by space (finding L4).* `RowLocks` resolves two identities: row
  and referenced-key locks belong to the **transaction** (`txn_of`, the
  top-level id) and go when it ends (`release_rows`, from
  `Database::commit_transaction` and `abort_transaction`, which no longer
  release table locks); table locks and the wait-for node belong to the
  **owner** (`owner_of`, which consults `owner_alias`) and go with
  `release_owner`. Each autocommit attempt is aliased to O for its life, so
  attempt 1's `FOR UPDATE` and referenced-key locks never carry into attempt
  2, while O's table locks and graph node survive every attempt's abort. For
  a block the two identities are one id.
- *Release points.* An autocommit scope releases O, then ends it, when its
  future is dropped: after `execute_with_sync`'s commit, flush and sync, and
  on error, cancellation, a departed client, a panic or a dropped future
  alike. A block's owner is an `OwnerRelease` in the session's block,
  released by COMMIT, ROLLBACK, a failed block's COMMIT, the idle timeout and
  disconnect after their last durability step and before the reply, and by
  its drop on any other path. Replication apply runs under its
  transaction's root (`OwnerKind::Replication`), now ends a failed apply
  through `abort_transaction` (finding L2, part 1) and releases after the
  outcome is durable; import batches run under theirs
  (`OwnerKind::Importer`), released at the batch's commit or abort.
- *Deviation to settle in B3b.* On the wire an autocommit statement's reply
  is sent inside `execute_single_statement_inner`, before `drop_temps`, so
  O is released after the reply. Harmless in B2, where an autocommit
  statement holds no table lock (`LOCK TABLE` is refused outside a block);
  from B3b its AS/RX locks would outlive the reply by the temp drops, so a
  waiting AE is granted a little later than the client's next statement
  could start. B3b either releases before the reply or accepts that.
- *Representation and cost (Q4).* O is a `TxId` per autocommit statement,
  as decided, behind `owner::StatementOwner` and `LockOwner`; swapping it
  for a tagged registry touches `owner.rs` and `begin_owner`. Measured by
  `owner_tests.rs:owner_cost` (release, in-memory database, load average
  ~18, 20,000 point SELECTs through `execute_with_sync`): ids per autocommit
  statement went from 1 to **2.00**, so the status bitmap grows twice as fast
  between settles (two bits per id; settled blocks compress as before). O's
  own work (begin, alias one attempt, release, end) costs **~210 ns**, about
  **0.3%** of a ~68 µs point SELECT there; one plain autocommit id costs
  ~130 ns. B3b's perf gate on ext4 decides Q4.


**Lifetime.** A family's locks are released with its tickets, once its outcome
is final and its post-commit work is done (§2 Ticket lifetime); an autocommit
statement's at its end. AS is held to the end of a block, which matters at
REPEATABLE READ: a rewrite reinserts only rows visible now
(`visibility_now_for`), dropping versions an older snapshot still reads. DDL
is not transactional here, so an AE taken in a block is released when the DDL
statement ends, and the family keeps what it held before.

**Granting.** PostgreSQL's rule (`ProcSleep` and `ProcLockWakeup`), keyed by
owner. A request is granted when it conflicts with no mode another owner
holds and no request of another owner that conflicts with it waits ahead of
its place in the queue:
- (a) every conflicting waiter counts, weak ones included (PostgreSQL's
  `waitMask`);
- (b) the requester's place is just ahead of the first waiter that a mode it
  already holds conflicts with (that waiter waits for it, so queueing behind
  it would deadlock), and otherwise the end of the queue. Waiters past that
  place never hold it back; waiters before it do when they conflict.

Both clauses are about starvation. Under a rule (a) that counted only S and
AE waiters, S requests (which do not conflict with S) kept passing a waiting
RX, so a stream of CREATE INDEX-style S holders starved writers. Under a rule
(b) that let a holder of *any* mode go ahead, a holder passed waiters it does
not block: AS holders asking for RX passed a waiting S, and the S waited for
as long as that traffic lasted. One owner per statement (above) is what keeps
(b) sound: a task that waits under a second owner for a lock its first
owner's statement needs is on a cycle that no graph sees. In R1's case,
`BEGIN; INSERT INTO t` (RX), a waiting autocommit `CREATE INDEX ON t(x)` (AE),
and the first session's next Execute and COMMIT go through by (b), since RX
conflicts with AE; the AE is granted after COMMIT. An owner's own locks never
conflict: upgrading RX to AE waits only for other owners, which replaces the
drain's own-family exclusion (RV1, P2). A queued request's wait-for edges are
the conflicting holders plus the conflicting waiters ahead of its place.

**Acquisition.** A statement requests its whole known table set at once and
holds none of it while waiting. A table found late (a CTE body, an FK parent, a
JSON shadow read, a subquery) is taken try-or-restart: a reader that cannot
have it at once releases everything and starts again with the larger set,
which is safe because it has written nothing. So **autocommit readers never get
40P01**. Restarts are counted (`rdb_lock_restarts_total`); after
`max_lock_restarts` (default 8) the reader stops restarting and waits holding
its set, with graph edges, so steady DDL churn cannot livelock it. A writer
that finds a table late keeps its locks and waits with edges, as its row-lock
waits do.

**The wait** is one loop, today's `LOCK TABLE` loop (`executor.rs:4646-4690`),
woken by a per-table `Notify` instead of the 5 ms poll:
- it polls `cancel::tripped()`, so `statement_timeout`, `pg_cancel_backend`, a
  departed client and shutdown end it with 57014 or 57P01;
- after `DEADLOCK_TIMEOUT` (1 s, `row_locks.rs:228`) it calls `set_waiting` and
  `deadlock_check` (`executor.rs:1257`) on every poll, with `table_blockers`
  extended to the mode matrix and to waiters ahead of it;
- past the session's `lock_timeout` it fails with **55P03**, replacing the
  fixed `LOCK_WAIT_TIMEOUT` (10 s, `executor.rs:4900`) for table locks. pgwire
  stops reporting `lock_timeout` as a hard-coded `"0"` (`pgwire.rs:6836`), and
  the default stays PostgreSQL's `0`, no bound (decision 12). A server default,
  `ddl_lock_timeout_default`, is opt-in and unset: a bounded default would fail
  long migrations under load.

No statement gets 40001 or 42P01 from a DDL. A statement can get 40P01 on a
real cycle (never an autocommit reader), or 55P03, 57014 or 57P01 on its own
bound or cancellation.

**Victims.** A cycle fails, in order of preference: a DDL that has not begun
retirement (step 2), and so has written nothing (with two such, the younger);
otherwise any other S or AE requester, `LOCK TABLE` included (Q6); otherwise
its youngest member that is not an autocommit reader. A reader past
`max_lock_restarts` waits holding its set with edges, so it can sit on a
cycle. But AS and RX conflict only with S and AE, so every cycle through a
reader contains an S or AE requester, and **an autocommit reader is never the
victim**. Together with all-or-nothing acquisition this extends "never fail
readers" to block readers and to ORM traffic, which does not retry 40P01
reliably.

**An error ends the block** (R3). On any error in a block with no open
savepoint, 40P01 and 55P03 included, `fail_statement` (`pgwire.rs:3870`) runs
`abort_transaction` and `flush_outcomes` at once, releasing table locks,
tickets, row locks and wait edges (`release_all`, `row_locks.rs:507`). The
block stays failed: later statements get 25P02, ROLLBACK is a no-op, COMMIT
reports ROLLBACK, and the COMMIT and ROLLBACK paths tolerate an id that is
already aborted. This is PostgreSQL's TBLOCK_ABORT; before it, a victim kept
its ticket until its client sent ROLLBACK, and the survivor waited for that.
Under an open savepoint only the sub-id is aborted and the family keeps every
lock, which is stricter than PostgreSQL (it releases the subtransaction's
own); per-savepoint release is v2. So under pgjdbc's autosave a victim's block
stalls a DDL until the block ends: a wait, not a cycle.

**Describe and catalog reads take no lock: a deliberate deviation.** PostgreSQL
takes AS at Parse and Describe and holds it to the end of the transaction; R1
does not hang there only because of rule (b). Here
`Database::schema_view(name) -> Arc<SchemaView>` reads the slot's current
instance and exposes the schema plus **a read-only stats accessor** (Q3):
provider lengths, the ids counter (`last_row_id`) and the `owns()` file
lengths, which is what `relation_size` and `tuple_counts` read today
(`catalog_views.rs:1195-1289`, through `table.data()`). It offers no scan, no
mutator and no `Table`. It takes no lock, and during a rewrite it returns the
old instance's figures (the retired instance's last figures once it is
retired), never an error. Without it the catalog builders would need
`table_for(name, owner, AS)` on every user table, since `pg_class` builds a
row per table (`catalog_views.rs:731-760`). Then every JDBC `getTables`,
psql `\d` or ORM introspection would queue behind a pending AE on any table,
and inside a block would hold AS on every table until the block ended.
Describe (`describe_for_wire`, `describe_query`, `describe_resolved`),
`param_infer::infer` (synchronous: it never could await a lock),
`learn_prepared`, `catalog_views.rs` and `introspect.rs` use it. Everything
that executes goes through `Database::table_for(name, owner, mode) ->
TableHandle`, which acquires or confirms the lock, then loads the slot; this
includes `substitute_in_subqueries`, the DML lock check (`executor.rs:4834`,
which fails at once today and now waits) and every `reload_table` caller, and
`reload_table` asserts AE. `Database::tables` becomes private, so a raw lookup
on an execution path does not compile ("a guard is an allowlist"): every lookup
executes with an owner or reads metadata with no lock, so R1's class cannot
come back. The slot is never empty during a rewrite, so a lockless reader sees
the old or the new instance, never 42P01 except after DROP. The deviation is
safe only because Describe reads nothing but metadata and Execute reconciles
the shape:
- **The client's shape changes only at Describe.** Each prepared statement and
  portal records the shape the client was last told by Describe, and Execute
  never updates it. When the columns the statement produced differ (compared
  exactly: the engine builds the result before sending, `pgwire.rs:2855`), the
  session's shape-change reply (next bullet) is sent. In `rowdescription`
  mode, a RowDescription goes ahead of the DataRows **on every Execute** until
  the client describes again. Today's backstop (`pgwire.rs:2758-2801`) records the
  new shape after the first notice (`:2778-2786`), so later Executes send none.
  sqlx refreshes only that execution's metadata from an unrequested
  RowDescription, not its statement cache (the driver gate confirms this), so
  its next Execute decodes against the stale shape: a silent wrong answer in
  today's code, fixed in phase 0. `schema_generation` stays only as the skip
  path: unchanged since Describe means no comparison.
- **The reply is a per-session mode** (Q2), `rdb.shape_change`, set by a
  startup option or GUC (`options=-c rdb.shape_change=…`). In
  `rowdescription` mode, the default, the server sends the unrequested
  RowDescription above. sqlx needs it, and it is what today's code sends. In
  `error` mode it sends PostgreSQL's 0A000 `cached plan must not change result
  type`, which pgjdbc, npgsql and psycopg answer by re-preparing. sqlx never
  re-prepares on 0A000, so every later use of the statement failed
  (`pgwire.rs:2786-2797`). Owner's item 14 ("0A000 only if the shape truly
  changed") therefore holds in `error` mode. A driver that keeps a named
  statement past its Describe (pgjdbc past `prepareThreshold`, npgsql
  auto-prepare, tokio-postgres and deadpool) never re-describes. So in
  `rowdescription` mode it gets the RowDescription on every later Execute,
  and pgjdbc's `processResults` and tokio-postgres's `RowStream` may treat it
  as a protocol error and drop the connection (read from memory of their
  source, not run), where 0A000 would have healed it once. The driver gate
  (below) records which mode each driver needs. Where the startup packet
  names the driver (pgjdbc's default `application_name`), that sets the
  session's default; otherwise the default stays `rowdescription`, because
  sqlx sets no `application_name` by default (from memory, not checked) and is
  the one driver shown to need it. Bind's
  per-column result formats apply positionally when their count matches the
  new columns, a single format applies to all, otherwise text; the
  RowDescription states the formats used.
- **`shape_epoch`**, separate from `schema_generation`, is drawn from **one
  database-wide monotonic counter** (Q7) and stored per table, so a DROP and
  CREATE, or a RENAME and then CREATE, of the same name can never repeat an
  epoch an old instance used. A per-instance epoch starting at 0 would let a
  plan learned on the old `t` (keyed by name, `pgwire.rs:4228-4300`) pass
  against a recreated `t` with another NUMERIC scale and render wrong values
  as success. It is carried on `PreparedRun` and re-checked after `table_for` grants the lock
  (today `execute_watched` loads the generation before any lock,
  `pgwire.rs:4196-4206`). A mismatch falls back to the text path in the same
  Execute and sets `info.prepared` back to `Unknown`. Index-only DDL leaves it
  unchanged, so CREATE INDEX discards no `Ready` plan and sends no
  RowDescription. It also covers a cached plan's NUMERIC scales, which must
  match the instance the rows come from.
- **Generation order.** The end-of-change `schema_generation` bump (the
  `SchemaChanged` drop) happens after the `ArcSwap` store, so a describe-cache
  entry made from the old instance during a DDL is filed under a generation
  that never answers after the swap.

**Driver gate** (phase 0, before W3 merges): sqlx, tokio-postgres, pgjdbc
(past `prepareThreshold`: named statements, binary formats) and npgsql
(auto-prepare) each run describe-before-DDL, execute-after and three
re-executes with values checked, **in both modes**, the non-sqlx ones through
the wire-probe harness. The gate records which mode each driver needs. A
stale shape reaches a client by two routes: a statement described before the
DDL, which no Describe rule can reach and only the mode answers, and, because
Describe takes no lock, any Describe made while an AE is pending or held.
Until the gate passes, this is proven for sqlx only. **If a driver works in
neither mode**, Describe falls back to PostgreSQL's rule for non-holders, for
the second route only: a Describe whose family holds nothing on `t` waits for
a pending or held AE (bounded by `lock_timeout` and cancellation, with a
graph edge when the family holds locks elsewhere) and returns the new shape;
rule (b) keeps holders lock-free.

**Removed**: the reload mutex (AE conflicts with itself), the slot queue and
its `Ref` rule (the lock queue is keyed by name and survives the swap), the
run-ahead exemption (rule (b)), the own-family drain exclusion, the slot's
family set (§2) and two of the three waits.

**JSON shadow DDL** (R4, Q4) exists only in shredded mode
(`--json-shadow-tables`, off by default; otherwise documents are stored as
native JSONB bytes). Shredded documents run `ALTER TABLE <shadow> ADD COLUMN`
(`ensure_shadow_column`, `executor.rs:3025`) and type widenings
(`retype_column`, `:3116`, a rewrite) inside the writer's INSERT or UPDATE,
under its tx (`run_ddl`, `:6047`). As AE callers they would make document
writers wait and deadlock where PostgreSQL never makes a jsonb INSERT or
SELECT wait on other DML:
- Two blocks that each wrote a document hold RX on the shadow until they end.
  When each then inserts a document with a new key, each upgrades to AE and
  waits for the other's RX. One gets 40P01, which aborts its block (R3); ORMs
  wrap every save in a block and do not retry it. In a cycle with ordinary
  DML, the victim rule would always pick the document writer.
- An idle block that read a document holds AS on the shadow. A new-key
  INSERT's pending AE then queues every later SELECT of the column behind it
  by rule (a), with no bound under `lock_timeout = 0`.

So **a shadow ADD COLUMN stays off the user-visible AE path in v1**. Adding a
nullable column to an internal shadow table is metadata-only, with no rewrite
and no instance swap; rows written before it read the column as NULL. That
part of v2 comes forward for shadow tables only. The change is serialised per
shadow table by an internal lock that conflicts only with other shadow DDL and
with AE on the parent, and it takes a ticket like any mutation.

**Keys first.** `store_json_document` collects every new key and widening
across all elements and documents of the statement (`:2689`, `:2750-2822`) and
runs them before it writes any shadow row, so one INSERT of
`[{"a":1},{"b":2}]` never upgrades on itself. Only `retype_column` widenings,
which are rewrites, still take AE on the shadow, requested in name order while
holding nothing on those shadows. A 40P01 or 55P03 there has applied nothing
(the ensures already done are idempotent), so it carries
`CONFLICT_RETRYABLE` and the autocommit loop replays it. In a block it
surfaces: a deviation from PostgreSQL, limited to widening the type of a
shredded key. Internal tables skip `SchemaChanged`, so shadow DDL bumps no
generation.

Phase 0 proves through the twin suites in shredded mode that rows written
before the column read it as NULL. If that cannot land, shredded mode keeps AE
shadow DDL as a stated limitation (risk 12), and V10 case 11 is not claimed.

**Cost and behaviour.** Today DML with no table locks skips the lock mutex
(`table_count == 0`, `row_locks.rs:487`); here every statement takes and
releases a lock per table, readers included. AS and RX therefore take a fast
path in PostgreSQL's style (per-table sharded holder sets plus a "strong lock
pending" bit, moved into the main table only when S or AE is requested), so the
global `RowLocks` mutex is no serialisation point. Phase 0 gates point-SELECT
and bigint-PK INSERT p99 within noise, on ext4. Changes, each as PostgreSQL
behaves: an idle-in-transaction reader now blocks DDL, unbounded under
`lock_timeout = 0`, until `idle_in_transaction_session_timeout` (off by
default) or a cancel; DML waits behind `LOCK TABLE` instead of failing at once;
`LOCK TABLE` waits lose their 10 s bound; CREATE INDEX queues reads for its
whole build.

**Lock order** (RV16) gains one outer level: the table lock, then the ticket,
then the maintenance lock, `compaction_gate`, `write_lock`, stripes and row
locks.

**Steps** of a reloading DDL:
1. **Lock.** `table_for(…, owner, AE)` on the DDL's **complete** lock set, in
   name order, through the wait above (Q8): `t`; for DROP, and for ALTER DROP
   COLUMN of a shredded column, the whole shadow chain
   (`drop_table_to_depth`, `database/mod.rs:1553-1670`); for RENAME and MOVE,
   both names. **No `table_for` wait happens after step 2(a)** (asserted). A
   wait there would be unbounded under `lock_timeout = 0` and uncancellable on
   the spawned task, and a 40P01 there would land halfway through the work.
   Past `lock_timeout` the DDL fails with 55P03 having **written nothing**: no
   sidecar, no `DIRTY`. Once granted, no other owner holds anything on `t`:
   assert that the old instance's statement count is the caller's alone and
   that no family but the caller's holds a ticket on the file set. The caller's
   own ticket stays: the gate is the file set's (§2), so COMMIT or abort drops
   it from the same word, and nothing is handed over. A block leaves no
   post-commit work on the old instance (in-place overwrites never run in a
   block, `transaction.rs:617-620`; `pending_filters` are applied at each
   statement's `commit_durable`, `table.rs:4718-4724`), and the rebuild indexes
   its uncommitted versions like any other.
2. **Retire** the old instance (P5, P25): (a) mark its gate handle retired, so
   it refuses new tickets; cancel an in-flight audit (§4); cancel any seal of
   the old instance by `ticket()`'s compare-and-swap rule, setting `CANCEL`
   only while the word shows an unpublished seal, then wait for the maintenance
   lock (§3), never by `fetch_or`, whose stray `CANCEL` would survive the swap
   and make every later seal skip (R8). Clear `maintenance_alive`. (b) Join its
   `VacuumService` (RV14), bounded by one batch, since every batch loop checks
   `alive` between batches (today `neutralize_aborted` does not,
   `vacuum.rs:1866-1904`, and the forget sweep checks only between ticks,
   `:1511-1563`). That service holds providers, not the `Table`
   (`table.rs:809-836`), so no statement count sees it; its in-flight batch's
   ticket is released when it exits. Close writes nothing. `forget_pause` is
   taken here and held through the open and swap; today it is taken at the top
   of `reload_table`, which would pause status-log forgetting database-wide for
   the whole wait (N31). **Steps 2-4 run on a spawned task that nothing cancels
   once (a) begins**: a cancelled client or the shutdown deadline waits for it,
   and a failure inside it is handled by the fresh-instance rule below, never
   by dropping the future between retirement and swap.
3. **Invalidate** (`DIRTY` if not last), **do the DDL work, including its
   sidecar writes**, and open the new instance, which rebuilds. Every caller
   today writes its sidecar before calling `reload_table`: CREATE INDEX's
   declaration (`executor.rs:8101`), DROP INDEX's tombstone (`:8136`), the ALTER
   constraint arm's `unique_key::remove` (`:7592-7604`), the ALTER rewrite's
   declarations (`:2341`). A 55P03 after that would leave the declaration
   durable: the client sees an error, the next open builds the index or drops
   the constraint, and a retry reports "already exists". So each write moves
   into this step, passed to `reload_table` as the DDL's work.
4. **Swap.** Map entries become slots, one `ArcSwap<Table>` per table plus the
   file set's `WriteGate`, so the swap takes no map shard lock and executor code
   holding a `DashMap` `Ref` across an await cannot deadlock against it (N13).
   The new instance's open keeps the slot's word (§2). Then `schema_generation`
   is bumped (after the store, above) and AE is released.

**ALTER rewrites are one reload** (P3). `rewrite_table_with_columns`
(`executor.rs:2171`; ADD COLUMN at `:7381`) and `retype_column` read the rows
(`visibility_now_for`: committed rows and the caller's own), drop the table,
create it, reinsert, and only then reload for the carried indexes. Run as that
chain, the read precedes any wait: a block's INSERT that commits during the
drop is acknowledged and then deleted with the old files, and readers get
42P01 between the drop and the create. In v1 the whole rewrite is step 3's work
under AE: read, drop, create, reinsert and carry the declarations, then swap.
The read follows the lock, so it sees every write that committed, and waiters
stay queued on the name, so nobody gets 42P01. **Each reinserted version keeps
its visibility** (R5). Rows committed by anyone are written frozen, as today
(the reinsert is `insert_row`, which is `insert_row_in_txn(row, FROZEN_TX)`,
`row/table.rs:2886-2888`). The caller's own uncommitted versions are written
under the `TxId` that wrote them, a savepoint's sub-id included. A committed row
the caller deleted or updated is reinserted frozen with that `TxId` as its
`xmax`, so a ROLLBACK revives it. Under AE no other family has uncommitted
versions, and aborted ones are dropped. Every row under the caller's id would
make `BEGIN; ALTER …; ROLLBACK` erase the table; every row frozen (today) lets
the caller's own rows survive its ROLLBACK. The old word is
detached with the caller's ticket on it, which COMMIT later drops harmlessly;
the reinsert takes a new ticket for the family on the new file set's word, held
like any other.

**DROP, RENAME and MOVE** take AE by the same rules. DROP marks the slot
`Dropped` through its `ArcSwap` and releases AE; queued waiters are granted,
find the slot `Dropped` and get 42P01, as in PostgreSQL after a DROP. Only then
does it remove the map entry. RENAME and MOVE take AE on the source and target
names in name order and carry the word to the target's slot (§2, §6).

**A retired instance is never reinserted** (N14, N23): a drop, rename or move
failing after step 2 opens a **fresh** instance from disk, or removes the entry
if that fails.

## 8. Frozen encodings and derivations

Snapshots store exactly the bytes the evaluator is pinned to:
`key.rs::encode_ordered` (B-tree, path), `encode_canonical` (hash), and
`jsonb::canonical_key` (JSONB). That turns their property tests into on-disk
format tests (owner's invariant 5). Hook C5 enforces it in phase 0:
- **`key::ENCODING_VERSION`** in the identity;
- **golden byte vectors** for every type, covering escapes, NULL order,
  NUMERIC scales, JSONB, `encode_canonical`'s raw-bytes fallback for an
  unreadable document (`key.rs:240-251`), and a path index's empty key for a
  missing or JSON-null path; a byte change without a bump fails CI;
- **coverage**: a test walking `btree_supported` and `hash_supported` fails on
  any type without a vector or an evaluator property test (`CHAR` today).

**Derivations upstream of the encoders** (finding 12):
- **Path keys** are `encode_ordered(STRING, as_text(get_path(doc, steps)))`,
  versioned by `jsonb::TEXT_RENDER_VERSION`, bumped by any change to `as_text`,
  `render_text_into` **or navigation**: `get_path`, `get_key`, `get_index`,
  `Step` parsing, `Step::Either`'s integer parse (N36). Vectors
  `(doc, steps) → key` cover numbers at every scale, huge numbers, escapes,
  JSON null, and step kinds: Key, positive and negative Index, Either on
  object and array, out of range, leading space and sign.
- **Gin items** are versioned by `gin::ITEM_DERIVE_VERSION`: `gin_key_item`,
  `hash_scalar`'s number canonicalisation, the tags, `jsonb_path_ops` folding,
  the byte feed, and the hash under it (`GIN_HASHER_ID`). Version 2 is version
  1's feed under rapidhash instead of xxh64; version 1's vectors stay frozen
  as written, as `ITEM_VECTORS_V1`.

**The filter's derivations** fall under the same rule, since T4 trusts its
section at every clean reopen and a silent change would prune matching rows
(`filter/hash.rs:1-27`). They are `canonical_hash` (`filter/hash.rs:175`) with
its ±0.0, NaN-payload and string normalisation, the in-tree mix and
`hash_bytes` it uses, `fingerprint(h, f)` (`:222`), `CODE_EMPTY`, `CODE_NULL`
and `CODE_FIRST` (`:35-41`), the zone-map bound encoding, padded and
truncated string bounds included, **the comparator that picks zone bounds**
(`zonemap.rs:503-541` `observe` through `cmp_strict`, the evaluator's
`perform_comparison_operation`), and **the span summary's hashing** (SuperBloom
`SALT`, `bloom_hash`'s mix, the sub-block formula and `SUPERZONE_*`,
`bloom.rs:60-102`, `:182`) (RV10), and **the bit-sliced plane layout** that
`write_slot` writes and the SIMD `probe_fn` kernels read: code bit `j` of
zone-local slot `local` at `planes[j * ZONE_WORDS + (local >> 6)]`, bit
`local & 63` (`kernels.rs:175-210`) (P21). Planes are persisted as raw words,
and `kernels.rs` is hand-tuned, so an interleaving change made to writer and
kernel together would pass every value vector and prune matching rows at
every clean reopen. `ZONE_ROWS` and the hash seed are already written and
checked at load (`persist.rs:181`, `:424`, `:436`). Today only a hand-bumped
`persist::FORMAT_VER` guards them (`persist.rs:36-50`), and it has already
had to: PostgreSQL NaN ordering forced 3→4, since older bounds pruned NaN rows
from `f > x`. Phase 0 adds, in `core/filter`:
- `FILTER_DERIVE_VERSION` in the identity, beside `FORMAT_VER`;
- golden vectors `(type, value) → canonical_hash`, `fingerprint` and
  zone-bound bytes for every fingerprintable type;
- **sequence vectors**, since one value's bound is itself and per-value
  vectors cannot see an ordering change: `observe(seq) → (min, max)` bytes,
  and the prune or keep outcome of each `SargOp` against probes, across types
  including NaN, -0.0, truncated strings and NUMERIC scales;
- bloom vectors `(hash, superzone) → block words` and `may_contain` outcomes;
- plane vectors `(f, [(local, code)]) → exact plane words`, and probe vectors
  `(persisted plane words, query fingerprint) → candidate bitmap` run through
  every compiled `probe_fn` kernel;
- the same bump rule: a byte or outcome change without a bump fails CI;
- **coverage, enforced as C5's is** (RV10). The filter classifies a column by
  its declared `field.db_type` (`filter/mod.rs:433`, `:496`), not
  `physical()`. The vector test holds one `match` over every `DbType` variant
  with no `_` arm, so a new variant does not compile until classified:
  **fingerprinted** (the 18 of `is_fingerprintable`: I8-I128, U8-U128, F32,
  F64, STRING, DATETIME, CHAR, DECIMAL, UUID, INET), **bounds only** (BOOL),
  or **excluded** (the other 29, the sentinels, JSON, JSONB, BYTEA, ARRAY and
  the PostgreSQL wide types among them, several physically STRING). It asserts
  the class agrees with `is_indexable`, `is_fingerprintable` and `BoundKind::of`
  (`hash.rs:71`, `:115`; `zonemap.rs`), and requires per class: fingerprinted,
  `canonical_hash`, `fingerprint` at f = 4, 8 and 16, bound bytes and bloom
  vectors; every bounded type, BOOL included, `observe` sequence vectors and a
  `zone_may_match` outcome for each `SargOp` through a second exhaustive
  `match` (`Eq`, `Gt`, `Ge`, `Lt`, `Le`), plus a width-mismatched probe that
  must keep the zone; STRING additionally values longer than
  `STRING_BOUND_PREFIX` (16), an all-`0xFF` prefix (the `UNBOUNDED` sentinel)
  and the `round_up` and `successor` carries; excluded, `ZoneMap::new` returns
  `None`, and a load whose column set disagrees with the binary's is rejected.
  It also pins `CODE_EMPTY`, `CODE_NULL`, `CODE_FIRST`, `HashSeed::default`,
  `ZONE_ROWS`, `ZONE_WORDS`, `SUPERZONE_ZONES`, `BLOCK_BITS`, `LANES` and
  `SALT` to golden values. `choose_f` and the HLL are deliberately outside the
  version: they pick only future zones' widths, so a change costs pruning, never
  rows.

**Gin hashes with one frozen hash everywhere** (decision 9). Items came
from `ahash::RandomState::with_seeds(fixed)` (`jsonb.rs:2003`), not promised
stable across versions, CPU features or pointer width. The in-memory
`GinIndex` **and** the snapshot use one in-tree hash: a lookup probes with one
set of query-item u64s, so two hashers would mean silent misses.
`GIN_HASHER_ID` is in the identity. Pre-images are not stored
(`jsonb_path_ops` items hash whole paths). `gin_items` and
`numbers_hash_alike_exactly_when_they_compare_equal` stay unchanged: they test
relations.

That hash was xxh64 (`GIN_HASHER_ID = 1`, `core/hash/xxh64.rs`) and is now
**rapidhash V3** (`GIN_HASHER_ID = 2`, `ITEM_DERIVE_VERSION = 2`,
`core/hash/rapidhash.rs`): upstream `rapidhash.h` frozen at release tag
`rapidhash_v3`, commit `bc4b4baa48a15ff52ff4725e1ccdcda62815221c`, its
`rapidhash_withSeed` with the default secrets and both compile-time switches
at their defaults (`RAPIDHASH_FAST`, `RAPIDHASH_COMPACT`), ported to safe Rust
with little-endian reads and pinned to vectors from the C reference. A later
upstream version is a new `GIN_HASHER_ID`. The switch was made before
anything persisted a gin item, so it rebuilt nothing. Why gin differs from
everything else:

- **Cost.** An item is about forty bytes, hashed once per document per build
  and once per query. xxh64's serial chain of multiplies made 1M-document gin
  builds 22-25% slower than they were on ahash. On a 40-byte item fed as
  tag, key, length, class, value, length, one-shot xxh64 costs 14.2 ns
  dependent and 6.0 ns independent; rapidhash 8.4 and 3.9 (rustc `-O`, this
  repo's box).
- **Quality only moves false positives.** A gin index is a superset index:
  every candidate is rechecked against the document, so a collision costs a
  recheck, never an answer. Tests still pin that 5.3M distinct items from
  generated documents (200k always on) and the adversarial families (zero
  runs of every length, single-bit flips, the same bytes under two seeds)
  never collide.
- **Routing, shape hashes and the vector-history digests stay on xxh64.**
  Their outputs are already persisted or on the wire (a routed row
  sits in the partition its hash chose), so changing them strands data;
  `ItemFeed::new` is xxh64 and stays so. Only `ItemFeed::rapid`
  (crate-private) is rapidhash, and `core/hash/source_tests.rs` fails if
  anything but `jsonb.rs`'s `item_hasher` builds one, or if anything outside
  `core/hash/` names `rapidhash`.

**The feed is a dedicated type, not a `Hasher`** (N35, N44). `ItemFeed`
gathers an item's bytes in a 64-byte stack buffer and hashes them with one
call at `finish`, with no per-item allocation; past the buffer, xxh64 streams
and rapidhash (one-shot) gathers the rest too. It has only `u8`,
`u64_le`, `i64_le` and `bytes` methods, and it does not implement
`std::hash::Hasher`, whose inherited `write_u16` through `write_i128`,
`write_usize` and `write_isize` all feed native-endian bytes. `hash_scalar`
(`jsonb.rs:2013`), `gin_key_item` (`:2052`) and the path-item feeders take
`&mut ItemFeed`. Today's `h.write_i64(parts.point)` and `h.write_usize(len)`
calls (`jsonb.rs:2020-2058`) then stop compiling and are rewritten as `i64_le`
and `u64_le`. A wrong call cannot compile, so nothing panics in production. A
recording test pins the exact byte feed, and golden vectors pin the hash
(xxh64's and rapidhash's against their C references) and a handful of
`gin_key_item` and `hash_scalar` outputs.

## 9. RAM at the target, with partitioning phase 3

**What decides RAM.** With phase 3, a closed partition costs a ~64 B slot and
no index RAM. An open one holds today's indexes and filter, `rows × (C·f/8 +
k·(70-130)) B` ([sharding §11.2](sharding.md#112-ram-per-machine)): at 1.53M
rows, **141-233 MB at k = 1, 355-631 MB at k = 3**. RAM is open partitions
times that; v1 changes neither term.

**What v1 changes is the price of a reopen**, which decides how closely the
open set follows the active set. Phase 3 alone rebuilds on every reopen
(~395 MB read, ~1.3 s CPU per index at a borrowed rate, queries scanning
meanwhile), so idle close must wait long windows and the open set outgrows the
active set. With v1 a clean reopen reads ~98-144 MB (phase-2 gate: under
0.3 s); a read-only partition closes for free, a written one writes one
snapshot. Short windows become affordable, and partitions touched per window
shrink toward the tenants actually active.

**At the target**, per shard process (4,096 partitions, 7,812 tenants) and
per machine of six, with `a` the row-weighted fraction of tenants active per
idle window and `1 − (1 − a)^1.9` of partitions touched:

| a | Open partitions | k = 1: process / machine | k = 3: process / machine |
|---|---|---|---|
| 1% | 78 | 11-18 GB / 66-109 GB | 28-49 GB / 166-295 GB |
| 2% | 154 | 22-36 GB / 130-215 GB | 55-97 GB / 328-583 GB |
| 5% | 382 | 54-89 GB / 323-534 GB | 136-241 GB / 0.8-1.45 TB |
| all closed | 0 (4,096 slots) | 0.26 MB | 0.26 MB |
| today, all resident | 4,096 | 575-950 GB / 3.5-5.7 TB | 1.45-2.6 TB / 8.7-15.5 TB |

**Which window each replica uses.** The machine column assumes all six
replicas at the leader's `a`. Leaders close on the idle window. Followers
today are exempt from idle close for `shard_follower_warm_secs` (30 min,
[sharding §9.4](sharding.md#94-follower-apply)) after a write, because a
cold first apply stalled on a rebuild; four of the six replicas on a machine
are followers. With v1 a cold first apply loads a snapshot in ~0.2 s, so
sharding ties the follower window to the leader's idle window, and the
per-machine figures hold only then. With a longer follower window, size
followers separately at `a_follower`, the fraction written per follower
window.

**Where v1 is enough.** With half of a 512-768 GB machine for index and
filter, ~43-64 GB per process holds ~68-180 open partitions at k = 3 (a up to
**~1-2%**: 0.9% on 512 GB at the high per-row cost, 2.3% on 768 GB at the
low) and ~180-450 at k = 1 (a up to ~2-6%). Inside that envelope v1 plus phase
3 meets the goal: cold tenants cost nothing; a hot tenant costs its
partition's index.

**Where it stops** (each a v2 trigger, §16): the hot set's index exceeds RAM
(k = 3 above ~1-2% active, or one tenant's partition alone tens of GB that
`ISOLATE KEY` cannot shrink); snapshot rewrite bandwidth (written partitions ×
~120 MB ÷ window: 382 every 10 minutes is ~75 MB/s per process); or post-crash
rebuild of the written set missing the restart target.

**Sizing notes.** There is no block cache or write buffer, so
`machine_index_budget` and `block_cache` are v2 terms. Partitioning's
resident-bytes open-budget term (`partition_resident_budget`, §7.1 there)
caps the open set by evicting on Σ resident bytes against `machine budget ÷
processes_per_machine`. v1 supplies the measure:
- **Hook C11, `resident_bytes()`**, an O(1) atomic counter on `LiveIndex` and
  `GinIndex`, updated on insert, replace, forget, clear and load. Today's
  `sizes()` returns entry counts, not bytes, and walks the whole map under its
  read lock on every call (`index/mod.rs:305-317`);
- plus the filter's `bytes()` (`filter/mod.rs:893`);
- plus in-flight load staging and audit scratch copies (§4).

Net of the `_flt.db` it replaces, a snapshot adds ~64-110 MB per partition on
disk: ~0.26-0.45 TB per shard, ~1.6-2.7 TB per machine of six. One more file
per partition than today (`.ixs` replaces `_flt`, plus `.ixm`), one provider
fewer while open (fd term 10, §1).

## 10. Backup, replication, sharding, migration

- **S3 backup and restore** exclude `.ixs`, `.ixm`, `.ixs.tmp`, `.ixm.tmp`
  (a fuzzy walk could pair a `CLEAN` with later data) and include `rdb.ixe`. Restore
  rebuilds.
- **Replication** (statement or Raft row images) applies through the write
  path and the gate; each replica snapshots at its own clean points, and a
  follower's cold first apply ([sharding §9.4](sharding.md#94-follower-apply))
  loads a clean snapshot, which is why the follower window can follow the
  leader's (§9). A reseed by base copy rebuilds.
- **Sharding relocation and range moves** copy without `.ixs` and `.ixm`; the
  target rebuilds. Adoption of copied snapshots is v2.
- **Enabling.** Cargo feature `persistent_indexes`, default list. The filter
  rule applies to every table from phase 1; index sections need
  `index_storage = snapshot` (default after phase 4's gate).
- **Downgrade.** Phase 0 ships `incompat_features`, a root file of format
  flags; every later binary refuses, loudly, a flag it does not know, and the
  known set is feature-gated. `index_store_v1` is set durably **before the
  first `.ixm` or `.ixs` is created** (RV5), not at a first data write: a
  read-only open can publish under §3 rule 2, and a phase-0 binary, which has
  no gate, must refuse that directory. Pre-phase-0 binaries cannot be fenced.
  But each recreates `_flt.db` at open, which v1 treats as evidence: the
  proof fails and the table rebuilds (§4, §5). `rasterizeddb index-store
  disable` deletes every `.ixs` and `.ixm` and clears the flag.
- **File lists.** `index_store::owns(name)` matches `.ixs`, `.ixs.tmp`,
  `.ixm`, `.ixm.tmp`, for drop's `sweep_files` and the partition reaper (partitioning
  §12); rename and move deliberately do not copy them. `rdb.ixe` is named
  in drop-directory, mirroring and the S3 walk. Guard tests read the module's
  list, never a hand copy, and **reopen** a renamed table.
- **Size reporting and knobs** (RV21), phase 1: `relation_size` and
  `pg_total_relation_size` (`catalog_views.rs:1184-1206`), introspect's
  `filters` column (`introspect.rs:447`) and rebalance's `table_bytes`
  (`database/mod.rs:1584`) stat the `owns()` files instead of `io_filters`.
  `filter.checkpoint_interval_secs` and `--filter-checkpoint-secs`
  (`filter/settings.rs:147`, `main.rs:143`) are accepted with a deprecation
  notice and do nothing. Tests that assert the old path are rewritten:
  `tests/s3_backup_end_to_end.rs:1794-1799` (expects a `_flt.db`),
  `core/downgrade_tests.rs:121-131` (`checkpoint_filters()`), and the
  checkpoint cases of the `rasterizeddb_filter_tests` crate.

## 11. Test harness: power loss, not SIGKILL

SIGKILL keeps the page cache: it cannot lose a staged-but-unsynced outcome
(N7), cannot lose an appended-but-unsynced `DIRTY`, and cannot keep a write
while losing the `DIRTY` before it. v1 tests use **crash images**, built in
process as `core/database/cross_dir_commit_tests.rs` does: data files as of
their last fsync, and each log cut at the length of its last **completed**
`fdatasync`. `sync_audit` records both the issue and the completion of each
sync. Bytes between the last completed sync and an in-flight sync's issued
length are unsynced pages like any other, kept or dropped per seed below.
Cutting at the longest issued length instead would treat a sync still in
flight as done, and a write issued while `DIRTY`'s fdatasync was still running
would always pass. Extended so a "`DIRTY` after first write" bug is caught,
not masked:
- **Unsynced pages randomly kept or dropped, per page, seeded.** At each sync
  of a watched file the harness keeps a shadow copy (bounded by
  `MAX_SCENARIO_BYTES`). A file's image is its shadow with each differing
  4 KiB page independently kept (current bytes) or dropped (shadow bytes, or
  zeros past the shadow's length), and a length chosen between synced and
  current. Logs get the same past their cut, so a kept page after a dropped one
  is a torn tail recovery must stop at.
- **Keep probability per seed**: 0, 1, 0.5, random. Which seeds kill which
  mutation (RV24): "`DIRTY` without fsync" and "first write issued
  concurrently with `DIRTY`'s fdatasync" need a seed that loses the `DIRTY`
  bytes and keeps an acknowledged write: p = 0 or random, or p = 1 with the
  log's length cut at its last completed fdatasync. "`DIRTY` after the write,
  before commit" is invisible to every crash image on WAL'd writes: an
  in-place write is only a log record until commit (`file_sync.rs:1581-1610`),
  and an append changes a length. The census and the `sync_audit` order pin
  it, not the twin.
- **Directory operations** after the directory's last fsync (create, rename,
  unlink) are kept or reverted per seed, recorded by test hooks in the
  publish path, the unlink fallback, the `Unknown` log rewrite (RV12),
  marker-log compaction, the `_flt.db` evidence step and `.tmp` landing. An
  unhooked rename is always kept by images built from `read_dir`.

**Length-preserving mutations are required** (RV26). The identity's length
checks reject a snapshot after any INSERT or relocating UPDATE, so an
append-only scenario passes even with no `DIRTY`. **Every failpoint whose
mutation is "a write without `DIRTY`"** (first-write, `clean_fsync_fails`,
`marker_rot_after_clean`, `ixm_read_eio_at_open`, `unknown_rewrite_after_first_write`,
`ticketless_write_publishing`, the ENOSPC one) runs four variants: an
acknowledged in-place UPDATE of an indexed key (a test knob forces the fast
path), cut after the commit's in-place apply; a vacuum forget with filter
clear; `neutralize_aborted`; and a DELETE as a control.

**Oracles** (RV9, RV11, RV27). Raw entries are not comparable: live
structures hold history a rebuild lacks (§4 Audit), and a rebuild re-adds
versions vacuum forgot. So:
- **Answers** (`synchronous_commit=on`): the twin, every index answer equal. A statement that returned
  an error or was in flight at the cut may survive or not: its `Committed`
  record can be durable through another session's root sync
  (`transaction.rs:907-911`). A large UPDATE or DELETE can also survive in
  part: it commits in batches once the log is due for a checkpoint
  (`should_break_transaction`, `table.rs:4706-4720`, "atomic per batch"). So
  ambiguity is tracked **per committed batch**, and the harness fixes **one
  state per ambiguous batch per run** (P27), read from the recovered rows by
  an unprunable scan and applied to the twin; every index answer must then
  match. A per-query choice would let a stale load pass, `WHERE k = k_new`
  returning nothing matching "not survived" and `WHERE k = k_old` returning
  nothing matching "survived". The strict claims ride on the build counter.
- **Superset**: wherever a snapshot loads, §4's containment check against a
  rebuild from the recovered rows, over slots not reclaimable under the
  recovered status log. It catches the UPDATE variant and any load that drops
  entries a query did not exercise.
- **Forget, filter clear and `neutralize_aborted`** only remove entries or
  codes no snapshot can see, or rewrite rows into what visibility already
  concluded (`vacuum.rs:981-998`, `:1074`, `:1817-1826`). A pre-mutation
  snapshot stays a superset, so no post-crash oracle can see their missing
  `DIRTY`. They are pinned by the census (V3) and **`capture_race`**: capture
  parked between blocks, the mutation run, capture resumed; the journal must
  show the seal cancelled (optional) or the mutation queued (mandatory).
  Mutations: ungate forget; ungate the filter clear.
- **Pruning quality** (R12). Codes left uncleared only cost pruning, so the
  twin and superset pass them. After a clean reopen and one full vacuum tick,
  every slot reclaimable under the recovered status log must read `CODE_EMPTY`
  in every planed column, and a probe for a deleted value must prune as many
  zones as on the never-restarted instance. This kills "advance
  `cleared_below` before the clear batches", "first vacuum tick before the
  load" and the `NO_TX` selection mutant (R11).

**Suites** (RV22): `secondary_indexes.rs` and `json_indexes.rs`, each through
(a) a clean close and reopen from the snapshot (load counter moved, build
counter not) and (b) power loss at every failpoint below, 32 seeds in CI,
1,024 nightly. They are compat integration tests over pgwire, built without
`cfg(test)`, and the main model test runs in memory. So the failpoints and
image builder sit behind a test Cargo feature, `index_store_failpoints`, that
`rasterizeddb_compat` enables as a dev-dependency feature, and the suites run
file-backed. **The twin is a separate, never-restarted database outside the
image**, fed only acknowledged statements, with no index and no filter (or
queried with an unprunable spelling), so a filter fault cannot be shared by
both sides. `candidates_are_always_a_superset_of_the_matches` has no files; it
covers only the C3/C9 round trip. Each suite also runs on a second storage
location with its own WAL (in the root, the root log covers outcomes by
accident). Bug (c)'s scenario needs `synchronous_commit=off`;
its oracle is the recovered table scanned with an unindexable, unprunable
predicate spelling. Index use is asserted from `GIN_LOOKUPS`, `PATH_LOOKUPS`,
`GIN_DENSE_LOOKUPS` and the new `KEY_LOOKUPS`. Tests are bounded, clean up when
aborted, and are judged on exit code.

**v1 failpoints**, each a harness-only `failpoint!(name)` followed by a power
cut unless marked. "Twin" means the answers oracle above, with counters
moving; "superset" means the containment oracle. Reordering failpoints are
anchored where the mutant's moved step becomes visible, normally right after
the first data write (RV25).

| Failpoint | Window | After recovery |
|---|---|---|
| `snapshot_before_fsync` | `.ixs.tmp` written, not fsynced | log ends `DIRTY`: rebuild (build counter moved), twin; temp reaped |
| `snapshot_after_fsync` | `.ixs.tmp` durable, not renamed | rebuild, twin; temp reaped |
| `snapshot_after_rename` | renamed, directory not fsynced (kept or reverted per seed) | rebuild, twin. Defence in depth: with `CLEAN` appended before the directory fsync, a reverted rename leaves the old file under the new `CLEAN`, and the identity and checksum rows must reject it |
| `snapshot_after_dir_fsync` | published, no `CLEAN` | rebuild, twin |
| `clean_append_before_fsync` | `CLEAN` appended, unsynced (kept or dropped) | kept: loaded, no rebuild, twin, superset; dropped: rebuild, twin |
| `clean_after_fsync` | `CLEAN` durable | loaded, build counter unchanged, twin, superset |
| `clean_fsync_fails` | `CLEAN`'s fdatasync returns EIO; p = 1 page keep; a write follows before the cut | state was `Unknown`, so the write rewrote the log or unlinked `.ixs` first: rebuild, twin. *Mutation: set `Dirty` on a `CLEAN` fsync error* |
| `ticketless_write_publishing` (test build, backstop in release mode) | a ticketless `GatedIo` write between `PUBLISHING` and unseal; p = 1 | the write waited and took `DIRTY`: rebuild, twin. *Mutation: a sealed write only marks the seal broken* |
| `held_writer_during_cancelled_seal` (no power cut) | a writer cancels an optional seal parked in step 6's fdatasync of a large `.ixs.tmp`, then writes through `GatedIo` while `SEALED` and `CANCEL` are still set | admitted with no panic within one `PUBLISHING` window plus in-flight I/O; the seal never publishes; clean close, reopen, twin (P8). *Mutations: panic on `SEALED` with `CANCEL`; make the write wait for the seal to end* |
| `ticketless_write_parked_in_provider` (backstop in release mode) | a ticketless `GatedIo` write checked before `SEALED`, parked in the inner provider (`land_staged_durably`'s yield) while a seal runs | the seal counted it: it waited or was cancelled, no `CLEAN` over the write: rebuild, twin. *Mutation: `GatedIo` checks with a load* |
| `barrier_root_sync_fails`, `barrier_dir_sync_fails` | the step-4 barrier's root or directory log sync fails | no `CLEAN`; rebuild, twin. *Mutation: capture after a failed barrier* |
| `apply_held_fails_then_shutdown` | in-place UPDATE of an indexed key on a second location; commit marker durable, `apply_held` EIO poisons that directory log; clean shutdown | the step-4 barrier, the one poison check, refuses the snapshot: rebuild after replay, the new key found, twin, superset (RV4, P26). *Mutation: barrier through `sync_pending`* |
| `keep_tail_after_rename_fails` (sites: directory fsync, open, seek, `try_clone`; variant: `truncate_locked` after `set_len(0)`, failing `seek` or `sync_all`) | a keep-tail checkpoint's rename returned `Ok`, then the site fails; a commit follows; cut | the log was poisoned at the checkpoint: the next commit is refused, no append reaches the old inode, the barrier skips, rebuild after replay, twin (Q13, Q14, Q19). *Mutations: dir-sync failure returns `Err` unpoisoned; the writer is replaced but the sync handle left on the old inode; `truncate_locked` leaves `write_pos` stale*
| `lazy_open_fails_after_marker` | a test hook clears `sync_handle` to `None` after an autocommit in-place UPDATE of an indexed key writes its marker; the lazy open fails (EMFILE); another commit's sync covers the marker; idle checkpoint; cut | the failed lazy open poisoned the log, so the barrier refused the snapshot: rebuild after replay, the new key found, twin, superset (R7, Q14). A test counter also asserts that every keep-tail checkpoint installed the handle and that no lazy open ran outside the hook. *Mutations: the lazy open returns `Err` without poisoning; the checkpoint leaves the handle to a lazy open (killed by the counter)*
| `marker_compaction_after_rename` | compacted log renamed, directory fsync not done (kept or reverted) | either log: rebuild, twin |
| `marker_rot_after_clean` | a durable `DIRTY` after `CLEAN` is corrupted, writes follow; crash after the next append's overwrite began | bytes follow `CLEAN` at every instant: rebuild, twin. *Mutation: cut back to `CLEAN`, then append* |
| `dirty_append_before_fsync` | first mutation after a clean reopen: `DIRTY` appended, unsynced; mutation parked, nothing issued | twin (statement unacknowledged); if `CLEAN` is still last, the snapshot loads and is right |
| `dirty_after_fsync` | `DIRTY` durable, first write not issued | rebuild, twin |
| `dirty_enospc_after_first_write` | `DIRTY` hit ENOSPC, `.ixs` unlinked and the directory fsynced, first write issued | the unlink is durable: rebuild, twin. Correct code issues nothing before the directory fsync, so a cut right after the unlink would see no mutant write, and a reverted unlink there correctly loads the unchanged snapshot. *Mutation: issue the write before the directory fsync* |
| `first_write_after_snapshot` | first post-snapshot write issued, four variants; the UPDATE cut after its commit's in-place apply | UPDATE: rebuild, twin, superset; other variants: rebuild or a superset load. *Mutations (UPDATE variant, seeds losing `DIRTY`, keep-probability bullet): `DIRTY` without fsync; first write concurrent with `DIRTY`'s fdatasync; `DIRTY` taken at the UPDATE call site only with the backstop removed. `DIRTY` after the write is the census's; forget and clear are `capture_race`'s* |
| `seal_between_commit_and_failed_sync` | a seal attempted after `commit_written_transaction`, before a failing `sync_own_location` | the seal waited for the abort; the counter refused `CLEAN`: rebuild (build counter), twin with the failed statement in either state. *Mutations: release the ticket at commit; ignore the counter* |
| `ixm_read_eio_at_open` (no power cut) | EIO reading `.ixm` at open; a length-preserving write; fault cleared; abandoned close (no clean point), reopen | state was `Unknown`, so the write rewrote the log: rebuild, twin. *Mutation: treat an unreadable log as `Dirty`* |
| `unknown_rewrite_after_first_write` | `Unknown` slow path: rewritten log renamed and the directory fsynced, then the first length-preserving write issued (four variants); anchored after the write, where a reordering mutant becomes visible (RV25, P22) | the rename is durable: rebuild, twin (RV12). *Mutation: issue the write before the rewrite's directory fsync; seeds that revert the rename then load `CLEAN` over changed data, which the twin and superset catch* |
| `flt_evidence_before_ixs_unlink` | v1 open finds `_flt.db` beside `.ixm` after an old binary's in-place UPDATE and vacuum forget; `.ixs` unlinked, `_flt.db` not yet deleted | rebuild, twin (RV5). *Mutations: delete `_flt.db` first; trust `CLEAN` when `_flt.db` reappears* |
| `unknown_flt_delete_order` | as above, but the `.ixs` unlink fails (EACCES), so the gate starts `Unknown`; a later ticket runs the slow path; cut after its first step | the log is durably `DIRTY` or `.ixs` durably gone before `_flt.db` is deleted: rebuild, twin (R9). Variant: the rewrite succeeds and the delete fails with EACCES: that mutation fails, and every later ticket fails too until the delete succeeds, the word staying `Unknown` (Q12). *Mutations: delete `_flt.db` before the rewrite; set `Dirty` once the log is durably `DIRTY`* |
| `vacuum_batch_after_compaction` (no power cut) | a vacuum pass parked between its scan and its first clear batch; VACUUM FULL runs to completion and moves a live row into a scanned dead slot; resume. Variants: a pass with no clear or forget candidates parked in its reader drain, publishing after the compaction; a quarantine (drain timeout forced), then VACUUM FULL, then the next tick; then a batch of reinserts | the epoch moved, so no batch applies and the bound does not advance; every live row is found through a pruned query, the filter twin agrees, a composite-UNIQUE duplicate of the moved row is refused (R2). No live row's bytes change after the reinserts (Q10). *Mutations: drop the epoch re-check; read the start epoch without the gate; same for `forget_reclaimed`; publish without the re-check; `compact()` leaves `quarantined`* |
| `backfill_batch_after_compaction` (no power cut) | the post-crash open-time filter backfill parked with a full pending batch that holds a superseded version's slot; VACUUM FULL runs to completion and moves a live row into that slot; resume | the epoch moved, so the task abandons: no batch, no `clear_unindexed_prefix`, no `set_ready`. Every live row is found through a pruned query, the filter twin agrees, and a composite-UNIQUE duplicate of the moved row is refused (Q9). *Mutations: drop the per-batch epoch re-check; the stale task calls `clear_unindexed_prefix` after compaction left a prefix* |
| `staged_tmp_at_snapshot` | staged `.tmp` bytes present when an idle checkpoint seals; clean restart | snapshot loads (lengths match), twin. *Mutation: skip step 2's landing* |
| `invalidate_after_first_compaction_batch` | VACUUM FULL's first move batch committed | rebuild, twin. *Mutations: `DIRTY` and the epoch bump after the first batch; skip the ticket (the backstop panics; without it, the epoch row rejects)* |
| `mid_vacuum_full_phase2` (no power cut) | ENOSPC after compaction's first committed batch; variant: EIO in the post-compaction backfill after a successful compaction | same process: queries scan, no lookup or pruning counted, twin-equal, a composite-UNIQUE duplicate refused; a DELETE, then a clean close writes no snapshot; reopen, twin. *Mutations: skip `set_ready(false)`; demote per caller; build task ignores the generation; snapshot while demoted; set ready after a failed backfill* |

The suites green through (a) and (b) are owner's invariant 8.

## 12. Phased plan (v1)

Each phase is safe alone, and safe beside partitioning phase 3 whenever that
lands. Gates are measured on ext4 at 192.168.1.7, never only tmpfs (which
hides disk cost 200-695×). Partitioning phase 3 waits for none of this, but if
it is live when persistent phase 1 ships, phase 1 must include its idle-close
hook (below). Otherwise removing the runtime checkpoint and `_flt.db` would
make every partition reopen backfill its filter from zero, which is worse than
phase 3 alone.

| Phase | Ships | Gate |
|---|---|---|
| **0** | Nothing new trusted. `incompat_features`; C5 (versions, vectors incl. navigation, coverage, `CHAR` property test, xxh64 move, gin items in memory through `ItemFeed`, under rapidhash V3 since `ITEM_DERIVE_VERSION` 2, `KEY_LOOKUPS`); C6; C7; C10 (`Proof` mutator signatures, `IndexView`, the `test_tokens` mint for benches and `rasterizeddb_filter_tests`); filter derivations (`FILTER_DERIVE_VERSION`, vectors, §8). **Today-bug (a) with (d)**: demotion refused inside `TableFilters`, generation-checked readiness, no runtime checkpoint while demoted, compaction success only on an `Ok` backfill (§6). **The checkpoint error bug**: error-returning I/O, `disk_dirty` cleared only after success. N26: fail-stop or the counter. Reload (§7) on the table-lock manager: four modes with the ProcSleep grant rule and a weak-mode fast path, `table_for`/`schema_view` with a private `tables` map, autocommit owners before the first lock with `owner_alias`, one wait in the wait-for graph with the owner-first victim rule, cancellable and bounded by `lock_timeout` (real, default `0`), abort on error in a block, Execute's shape repair on every Execute with `shape_epoch` and the generation bump after the swap, keys-first shadow DDL, sidecar writes inside the reload, `forget_pause` after the lock, ALTER rewrites as one reload keeping each version's visibility, retirement only after AE, and the retired `VacuumService` joined; one `LockOwner` per statement for every sub-execution, O begun in `TransactionManager` and ended on every exit path, no `FROZEN_TX` lock (Q1, Q5); the owner-first victim rule never picking an autocommit reader (Q6); `schema_view` with its lock-free stats accessor (Q3); the per-session shape-change mode (Q2); `shape_epoch` from a database-wide counter (Q7); the DDL's complete lock set in step 1 (Q8); metadata-only shadow ADD COLUMN (Q4); the driver gate (§7) in both modes before W3 merges. **Today-bug (e)** and the slot epoch on every deferred slot- or offset-addressed writer: vacuum batches, free-space publish and quarantine, `BuildToken` batches (§2, Q9, Q10). `wal.rs`: every `commit_through` error poisons, and any failure after the keep-tail rename or after `truncate_locked`'s `set_len(0)` poisons (§3 step 4). The power-loss harness (issue and completion in `sync_audit`) with the suites through clean restart and crash images against today's rebuild. (e) rebuild rate, cold, for a bigint PK, `(tenant_id, id)` and a hash index at 1.5M and 12M rows | baseline green incl. `mid_vacuum_full_phase2` and its backfill variant; V10's reload cases, extended protocol included, each under a 10 s harness bound; the driver gate; point-SELECT and bigint-PK INSERT p99 within noise of today on ext4; `keep_tail_after_rename_fails`, `lazy_open_fails_after_marker`, `vacuum_batch_after_compaction` with its no-batch and quarantine variants, `backfill_batch_after_compaction`; C10's doctest pair and source test; benches run with `--features test_tokens` |
| **1** | **Gate and filter, every table**: `WriteGate` (packed word), `.ixm`, `Gated<…>`, `BuildToken`, `GatedIo`; FILTER-only snapshots through `serialize_sealed`; seal with writer preemption, `.tmp` landing, barrier, publish; shutdown plan, `rdb_index_checkpoint`, plain-table idle checkpoint; **if partitioning phase 3 is live, its idle close as a FILTER clean point** (otherwise partitions keep a close-time filter checkpoint until this ships); `_flt.db` deleted through the provider family at every open, as evidence beside `.ixm`; `index_store_v1` before the first `.ixm`; `cleared_below` persisted and ticket-free empty vacuum passes; session termination, then vacuum, replication and `flush_loop` stopped, before shutdown seals; `neutralize_aborted`'s one ticket per pass; size reporting and deprecated knobs (§10); excluded tables backfill at open; backfill rows and seconds logged; runtime checkpoint removed | every §11 failpoint for the filter; loom gate model; census test (V3); bug (c)'s scenario on the second location; no backfill after clean reopen, including after vacuum clears and a full vacuum tick, with the pruning-quality check (§11); an open performs zero `.ixm` writes; INSERT p99 on a bigint-PK table during the post-crash rebuild, measured and reported (RV18); one `DIRTY` fsync per writing session (`sync_audit`); writer p99 during an optional snapshot of a large plain table within noise of no snapshot, on tmpfs **and** on ext4 on a consumer disk, both reported (P12) |
| **2** | **Index sections** (B-tree, hash, path) behind `index_storage = snapshot`: C3, C9, full identity, load at routed open, fallback, audit (Clean-only, in-place rebuild on mismatch), `rdb_index_check`. Tables with gin wait for phase 3 | suites through (a) and (b); clean reopen of 1.53M rows × 3 indexes under 0.3 s and under 10% of (e); RSS after load within noise of after rebuild; a version bump followed by two read-only reopens moves the build counter once; audit with concurrent INSERT/UPDATE (mutation: swap the side build); an injected dropped entry, audit, clean close and reopen moves the build counter |
| **3** | **GIN sections** once 99's `gin.rs` work settles | `json_indexes.rs` through (a) and (b), dense gin included; `ITEM_DERIVE_VERSION` bump rebuilds once, then loads; forget, clean close, reopen: the superset check passes and `sizes()` equals the live sizes before the close (a rebuild re-adds forgotten versions, RV11); load, forget, compact: `live` never wraps (RV19) |
| **4** | **Partitioning phase 3 hooks**: index sections on idle close with the bandwidth bucket and the evictor's clean-victim preference; load at routed opens with `load_wait`; fan-out opens read the FILTER section only; C11 `resident_bytes()` feeding `partition_resident_budget`; load semaphore; default `snapshot` | first-touch p99 of a clean partition under 0.5 s cold, asserted so the scan-first behaviour would fail it; 10⁴ open/close cycles, no fd or file leak; RSS tracks the open set by `resident_bytes`; snapshot MB/s at the measured window; shutdown within `shutdown_deadline`; §16 counters exported |

## 13. Invariants and the tests that pin them

Each mutation must fail its test; afterwards grep for `MUTATED` and rerun with
the restore. Fix-confirming tests are first shown failing on the old code. The
full workspace is built before shared suites, not `check --lib`.

| # | Invariant | Enforced by | Test *(mutations)* |
|---|---|---|---|
| V1 | Lookup ⊇ true candidates after any open | proven-clean snapshot or rebuild | suites through (a) and (b), twin plus superset oracle. *(Load when the log ends `DIRTY`; drop a section or `UNKEYED`; skip an identity row; C3 keeps forgotten gin slots)* |
| V2 | `DIRTY` durable before the first post-snapshot write is issued (T1) | single-flight fdatasync; packed gate word; tickets held until the outcome is final and post-commit work done | `dirty_*`, `first_write_after_snapshot`, `unknown_rewrite_after_first_write`, `seal_between_commit_and_failed_sync`; census and `sync_audit` order (the only check for a `DIRTY` after a WAL'd write, §11); loom model of ticket, seal, `CANCEL` and `GatedIo`, asserting no ticket held while `Clean`. *(No fsync; append after the write; write concurrent with the fsync; background `DIRTY`; ticket as a plain load; `PUBLISHING` CAS ignores the count; second RMW ignores the state or `SEALED`; release the ticket before deferred applies or at commit)* |
| V3 | Every mutator passes the gate (T2) | C10 `Proof` in every mutator signature, tokens constructed only inside the three mints (crate-wide source test, R6); `GatedIo` registers each write and panics in tests | **census** (RV23): each mutator of §2, including `VacuumService`'s, runs from its **own fresh clean point** (clean reopen or `rdb_index_checkpoint`); the gate's test journal records ticket acquisition, each in-memory mutation at the raw mutator (which has the proof in hand) and each write issued; assert a ticket with a durable `DIRTY` preceded every mutation and write; a reopen followed by a vacuum pass with nothing new to apply records no ticket (RV2); `capture_race`; suites with the backstop armed; C10's doctest pair and three-part source test; ENOSPC on `DIRTY` with EACCES on the unlink: the statement fails, the data file is unchanged. *(Ungate vacuum forget, filter clear, sequence, replication apply, `.tmp` landing, one at a time; mint a `BuildToken` for a ready structure; drop the proof parameter; add a `pub(crate)` mint in `gate.rs`; derive `Clone` on `WriteTicket`; add a proofless `pub fn` mutator to `LiveIndex`; mint in a `gate` child module; a callback mint (`with_build_token`) in `gate.rs`; a `Deref<Target = WriteTicket>` guard; `ptr::read` of a ticket; a mint inside `tokio::select!` in `vacuum.rs`; a `type` alias or `use … as` rename of `WriteTicket`; a proofless `pub(super)` or trait-impl mutator; a descendant module writing `TableFilters.columns`; an `unsafe` generic `dup<T>` on the stored-ticket field; call `test_mint` from non-test code; re-clear empty codes under a ticket; advance `cleared_below` before the clear batches; first vacuum tick before the load)* |
| V4 | Gate state is the marker log's (N21) | `Clean`/`Dirty`/`Unknown` from the last good record; `Unknown` on unreadable log or failed `CLEAN` sync | reject by version, write in place, revert, reopen: rebuild; `ixm_read_eio_at_open`; `clean_fsync_fails`. *(Start `Dirty` when the proof fails; unreadable log as `Dirty`; `Dirty` after a failed `CLEAN` sync)* |
| V5 | Identity checked against data (T3) | §4 | corrupt each field; DROP + CREATE under one name; ALTER rewrite. *(Skip each row)* |
| V6 | Publish: temp, fsync, rename, dir fsync, then `CLEAN` + fsync (T3); the log never shrinks to end at `CLEAN` | §1, §3 steps 6-7 | `sync_audit` order asserted at every publish; `snapshot_*`, `clean_*`, `marker_*`, `ticketless_write_publishing`. *(`CLEAN` before dir fsync; rename before file fsync; `CLEAN` unsynced; cut back then append)* |
| V7 | Only a quiescent, durable, ready state is captured, and a published one stays unwritten | seal, `.tmp` landing, poison-checking barrier, eligibility, revocation counter, `PUBLISHING`/`CANCEL` with the count in the CAS | open writing transaction skips; writer arriving mid-capture cancels; a writer arriving during step 6's fdatasync of a large `.ixs.tmp` proceeds within one `PUBLISHING` window plus in-flight I/O, and its `GatedIo` writes are admitted without a panic (RV7, `held_writer_during_cancelled_seal`); a mandatory shutdown seal beside a running `VacuumService` and a follower apply: neither re-dirties the published snapshot (P30); `apply_held_fails_then_shutdown`; bug (c)'s scenario; failed VACUUM FULL then close writes nothing; injected `Committed`→`Aborted` skips; `staged_tmp_at_snapshot`; `barrier_*`; `keep_tail_after_rename_fails`, `lazy_open_fails_after_marker` (R7, Q13, Q14); the RV7 device measurement (§3 Cost). *(No seal; no barrier; unready index; ignore counter; no landing; a `CANCEL`-setting writer waits for the seal to end; barrier through `sync_pending`; the lazy open returns `Err` without poisoning; dir-sync failure after the keep-tail rename returns `Err` unpoisoned)* |
| V8 | Filter trusted only under the same rule (T4) | FILTER section via `serialize_sealed` with `cleared_below`; `_flt.db` deleted through the provider family at every open and treated as evidence beside `.ixm`; no runtime checkpoint | filter twin, distinct values, pruning asserted, codes differing both ways, through (a) and (b); vacuum clears then clean reopen and a full vacuum tick: backfill counter unchanged, no `.ixm` append (RV2); downgrade-upgrade-downgrade cycle with an in-place UPDATE and a vacuum forget under the old binary, asserting a rebuild, on a file, a KV and a mirrored table (RV5, RV20); `flt_evidence_before_ixs_unlink`, `unknown_flt_delete_order` (R9); pruning quality after a clean reopen and a full vacuum tick (R11, R12); a table `<t>_flt` refused beside `<t>`; a legacy pair `t` and `t_flt`, created in both catalogue orders and with `t_flt`'s schema unreadable, keeps every row of `t_flt` through reopen (P19); the audit passes after concurrent inserts and in-place updates, with no `rdb_index_snapshot_mismatch_total` (P18). *(Decide legacy ownership from the live map; compare raw filter codes in the audit; Trust `_flt.db`; keep the runtime checkpoint; persist `persistable_valid_upto`; unlink only at the first open; unlink only the primary's copy; ignore a reappeared `_flt.db`; delete `_flt.db` before the `Unknown` rewrite; select clear candidates by `should_clear` alone)* |
| V9 | Renumbering invalidates first; nothing re-enables a demoted structure | ticket, epoch, demotion at source, generation, `Ok`-only backfill (§6) | `invalidate_after_first_compaction_batch`, `mid_vacuum_full_phase2` incl. its backfill-EIO variant; a mandatory seal beside VACUUM FULL beside a `neutralize_aborted` and forget sweep delays no reader beyond the seal's step-1 wait (RV16, P24); `vacuum_batch_after_compaction` (R2, Q10), `backfill_batch_after_compaction` (Q9). *(Apply a deferred clear or forget batch, a free-space publish or a `BuildToken` batch without the epoch re-check; `compact()` leaves `quarantined`; Demote after the first batch; per caller; ignore generation; ready after a failed backfill; ticket inside `compaction_gate.write()`; neutralize ticket per batch under `compaction_gate.read()`)* |
| V10 | One instance per file set; retired never mutates, snapshots or returns; DDL takes AE as PostgreSQL does and writes nothing before it is granted; Describe never waits; a client never decodes against a stale shape | §7, §2 ticket ownership and word continuity | CREATE INDEX during bulk INSERT … SELECT then clean reopen, every row found; concurrent DDLs; 55P03 on timeout, nothing to statements, no declaration after reopen; ADD UNIQUE during a long writer; DROP with queued waiters (42P01, no deadlock); rename failing after retirement; a reload during a long `neutralize_aborted` sweep: no old-instance write after the new instance's `CLEAN` (RV14). **Under `lock_timeout = 0`, each case under a 10 s harness bound:** (1) own family: two INSERTs then CREATE INDEX; INSERT, SAVEPOINT, INSERT, CREATE INDEX; the ROLLBACK TO and RELEASE variants; `BEGIN; INSERT; ALTER TABLE … ADD COLUMN; COMMIT` (RV1, P2). (2) **R1 over the extended protocol**: A runs `BEGIN; INSERT INTO t` through Parse/Bind/Execute; B's autocommit CREATE INDEX, ADD COLUMN, DROP INDEX wait; A sends a new SELECT and a new `UPDATE t SET x=$1 WHERE id=$2` (Parse, Describe 'S' with inferred OIDs, Bind, Describe 'P', Execute): each Describe answers within 100 ms, both finish, COMMIT, B finishes, no 40P01 or 55P03; again pipelined JDBC-style (Parse/Bind/Describe/Execute/Sync in one write) and sqlx-style (Parse/Describe/Sync, then Bind/Execute/Sync). (3) A detached Describe and a block Describe holding nothing on `t`, during a pending AE: both answer within 100 ms with the old shape; Execute waits, and carries a RowDescription only if the shape changed (ADD COLUMN yes, CREATE INDEX no). (4) A prepared statement across ADD COLUMN, DROP COLUMN and INT to BIGINT, run three times after, in both modes: in `rowdescription` mode every Execute carries the RowDescription until the client re-describes and never 0A000; in `error` mode each stale statement gets one 0A000 and the driver re-prepares; decoded **values** (not names) equal a twin's, `PREPARED_RUNS` does not move on the first run after; a real sqlx `query_as` loop across the ALTER (the existing `rasterizeddb_compat/tests/ddl.rs` check, one re-execute and names only, is not enough). (5) Describe during a rewrite (failpoint between drop and create): old shape, never 42P01; a SELECT during ADD COLUMN waits, never 42P01, and a block INSERT that commits while ADD COLUMN waits survives (P3). (6) Catalog views and introspection (sqlx describe, JDBC `getColumns`) during an AE: immediate; `SELECT relname, reltuples FROM pg_class` and `pg_stat_user_tables` during an AE held behind an idle-in-transaction reader answer within 100 ms with the old instance's figures (Q3). (7) `INSERT INTO t SELECT FROM u` against a CREATE INDEX on `u` from a block that holds `t`: exactly one 40P01, to the DDL. (8) Two blocks each write `t` and each run CREATE INDEX, and on two crossed tables: one 40P01; the victim's client then sends nothing; the survivor finishes within `DEADLOCK_TIMEOUT` plus 2 s; the victim's next statement gets 25P02 and its ROLLBACK succeeds (R3). (9) `BEGIN; UPDATE t … WHERE id = 1; CREATE INDEX ON t(x)` against an autocommit UPDATE of row 1: one 40P01, to the DDL, no 40001 (P1). (10) An autocommit join over `t1` and `t2` with crossed DDLs: no 40P01. (11) Shredded mode: two autocommit sessions, and two blocks that have each already written a document, insert documents with new keys into one shredded column: all succeed, zero 40P01; one INSERT of `[{"a":1},{"b":2}]` does not wait on itself; an idle block that read a document, an autocommit new-key INSERT and an autocommit SELECT of the column: the SELECT does not wait (R4, Q4). (12) `BEGIN; INSERT (own row); DELETE a committed row; ALTER TABLE … ADD COLUMN; ROLLBACK`: other families' rows survive, the deleted row is back, the own row is gone, and a reader between the swap and the ROLLBACK sees every committed row (R5). (13) A queued Execute with `lock_timeout = 1s` gets 55P03 while its Describe succeeds; `statement_timeout = 1s` and `pg_cancel_backend` give 57014; a second DDL with `lock_timeout = 5s` behind an unbounded wait gets 55P03; `pg_cancel_backend` ends a DDL stuck behind an idle-in-transaction reader, no declaration after reopen; shutdown during a wait completes within `shutdown_deadline` (P4). (14) The driver gate (§7). (15) One owner per statement: an autocommit derived-table join `SELECT * FROM t1 JOIN (SELECT * FROM t2) d ON …`, a `WITH … SELECT`, and `SELECT … FROM pg_class JOIN t1`, each against a pending CREATE INDEX on the inner table: the statement finishes, then the DDL (Q1). (16) A reader past `max_lock_restarts` in a cycle with a block running `LOCK TABLE t2, t1 IN ACCESS EXCLUSIVE MODE`: the block gets the 40P01, never the reader (Q6). (17) Prepare a NUMERIC SELECT, DROP and CREATE `t` with another scale, re-execute: correct values, `PREPARED_RUNS` unchanged (Q7). (18) DROP of a shredded parent while a block holds its shadow: the DROP waits in step 1, and 55P03 or `pg_cancel_backend` ends it having written nothing; nothing waits after retirement (Q8). *(No AE; reinsert retired; retired snapshots; sidecar before the lock; a per-instance gate; no `alive` check between neutralize batches; Describe through `table_for` or AS under `FROZEN_TX` (case 2 hangs); a held lock re-requested queues behind a waiter; a FROZEN_TX waiter holding a lock; missing `owner_alias` on a retry attempt; readers hold and wait; a reader restart without a bound; youngest victim ignoring the owner-first rule; victim not aborted until ROLLBACK; AS released at statement end at REPEATABLE READ; Execute updates the client's shape (today's `pgwire.rs:2778`); the backstop sends 0A000; `shape_epoch` read before the lock; generation bumped before the `ArcSwap` store; shadow DDL per element; rewrite writes committed rows under the caller's id; rewrite reads before the lock; reload open reinitialises the word; the wait ignores `cancel::tripped()`; retire before AE is granted; retirement sets `CANCEL` with `fetch_or` (loom, R8); a derived body under `FROZEN_TX`; `cte_tx` a second owner not aliased to O; O never begun; O not ended on cancel; an autocommit reader chosen as victim; a catalog builder through `table_for`; `shape_epoch` restarting per instance; a shadow-chain lock requested after retirement; shadow ADD COLUMN under AE)* |
| V11 | Encodings and derivations frozen | C5, filter derivations (§8) | vectors, sequence and bloom vectors, coverage (C5's walk, and the filter's exhaustive `DbType` and `SargOp` matches, RV10), recording feed test. *(Make `DATE` indexable without vectors; change `STRING_BOUND_PREFIX` without a bump; `Ge` zone test through `Greater`; One escape byte; `1.0` rendering; `Either` parse; ahash in memory; `-0.0` or NaN normalisation in `canonical_hash`; NaN order in the zone comparator; bloom `SALT` or sub-block formula; plane word order swapped consistently in `write_slot` and the kernels)* |
| V12 | Failure means rebuild, never a wrong answer or an unavailable table | load checks, error-returning I/O, unlink fallback, audit invalidates durably | bit flips per section; EIO; ENOSPC on `DIRTY` (unlinks, write proceeds); ENOSPC in publish (no `CLEAN`); `dirty_enospc_after_first_write`, `apply_held_fails_then_shutdown`; injected dropped entry found by the audit, then clean close and reopen rebuilds; an excluded KV table whose `_flt.db` delete hits a transient error at open accepts a later write (P33). *(Excluded table refuses writes until reopen; Empty index on checksum error; `CLEAN` after failed rename; fail the open; swap the audit rebuild in memory only)* |
| V13 | Files in the right lists | `owns()`, explicit `rdb.ixe`, names no table file can take, stem from the physical name (§1) | rename and drop guards that reopen; reaper regex; tables `t`, `t_ixs`, `t_ixm`, `t.ixs`, `rdb_ixe`, `a.b` and `ab` side by side through publish, marker compaction, open, drop and reopen, every row of each kept; a crash leaving `.ixm.tmp` is reaped and swept by drop; `relation_size` counts `owns()` files. *(Copy `.ixs` on rename; miss `.ixs.tmp` or `.ixm.tmp` in drop; name the snapshot `<t>_ixs.db`; name the epoch log `rdb_ixe.db`; stem from `schema.name`)* |
| V14 | Downgrade fails loudly | feature-gated `incompat_features` | build without the feature refuses a v1 directory |

### Owner's invariants

| # | Invariant | v1 mechanism | Rows |
|---|---|---|---|
| 1 | Superset with a recovery story | proven-clean snapshot or rebuild; entries added and removed in memory as today; barrier and revocation check before capture | V1, V2, V7 |
| 2 | Unkeyed slots in every lookup | `UNKEYED` per section; rebuild unchanged | V1 |
| 3 | Removal exactly as `forget` recorded | C3 drops exactly `forgotten_slots`; `PRESENT` persisted, not inferred | V1; `gin.rs` compaction tests |
| 4 | Posting lists sorted, distinct | C3 emits so; load rejects otherwise | V12 |
| 5 | Encodings pinned, else versioned and rebuilt | C5; filter derivations; identity versions | V5, V11 |
| 6 | VACUUM FULL: rebuild under the gate, or remap | rebuild under the gate after `DIRTY`, epoch, demotion; no remap | V9 |
| 7 | ALTER carries declarations by name | unchanged; rewrite invalidates, new incarnation | V5; ALTER coverage test (B-tree, hash, composite, implicit unique, gin, path) through (a) and (b) |
| 8 | Twin and randomized tests through restart and crash | §11, power loss not SIGKILL | V1 |
| 9 | Gin one frozen hash everywhere (rapidhash V3, `GIN_HASHER_ID` 2; xxh64 for everything else); hash persists `KeyBuf` | §8 | V11 |
| 10 | Reload: DDL waits, 55P03, readers never failed | §7 | V10 |
| 11 | T1-T4 | §2, §3-§4, §5 | V2, V3, V5, V6, V8 |

## 14. Risks

1. **Divergences restart used to heal become durable.** The five known
   (second-instance reload, failed VACUUM FULL, revoked outcome, undurable
   filter clear, a commit applied by replay after a poisoned log) are closed
   (§3, §6, §7); the audit is for the next, and it invalidates durably before
   rebuilding.
2. **A mutator outside the gate**, if a future field exposes a raw
   `TableIndexes` or a new method skips the proof. C10's `Proof` signatures,
   its source test (a new `pub fn` on the four types must be classified, and
   no token may be constructed or handed out anywhere in the crate outside
   the three mints), the census and hunk review defend. C10
   does not prove the proof is the right gate's; the census does.
3. **Snapshot bandwidth** at short windows (§9), bucketed; an empty bucket
   prefers clean victims.
4. **Crashes still rebuild written partitions**, now including insert-only
   filters, and PK probes scan until their B-tree is back (§5, measured in
   phase 1), and so does whatever the shutdown deadline skips (§3).
5. **The load may not beat a rebuild by much**; then v1 ships only the filter
   rule.
6. **Pre-phase-0 binaries** cannot be fenced (§10); their writes are detected
   only through the `_flt.db` each recreates at open, an assumption the
   downgrade cycle (V8) pins.
7. **An unattended DDL can stall a table without bound**, as in PostgreSQL:
   `lock_timeout` defaults to `0` (decision 12); `ddl_lock_timeout_default` is
   opt-in. An idle-in-transaction session holding any lock on a table, a
   reader included, holds every DDL on it until
   `idle_in_transaction_session_timeout` or a cancel; a failed block no longer
   does (§7), except one with an open savepoint; real cycles end in 40P01.
8. **Excluded tables lose filter persistence**: KV-backed tables and
   `wal_enabled=false` directories backfill their filter at every open (§5).
9. **Mandatory seals stall writers** for a whole snapshot: an explicit
   `rdb_index_checkpoint` on a large plain table is seconds to tens of seconds.
10. **The shape-change reply is the remaining correctness edge** (§7). A
    statement described before a DDL, and any Describe during a pending or
    held AE, is stale, and only the session's mode repairs it: an unrequested
    RowDescription (sqlx) or 0A000 (pgjdbc, npgsql, psycopg). That each driver
    needs the mode named for it is a reading, not a result, until the driver
    gate runs in both modes. A driver that works in neither gets waiting
    Describe for non-holders, which covers only Describes made during the AE.
11. **Table locks on every statement** replace today's `table_count == 0`
    skip; without the weak-mode fast path the `RowLocks` mutex serialises
    readers. Measured on ext4 in phase 0. `owner_alias` is new state: an alias
    that is missing or outlives its statement splits the lock from its graph
    node and hides a cycle, R1's hang shape (pinned by a mutation). So does a
    sub-execution under a second owner (Q1), and an O that is not live hides
    every lock it holds (Q5); `LockOwner` and the O guard are the defences.
12. **Shredded mode depends on a metadata-only shadow ADD COLUMN** (§7, Q4).
    If phase 0 cannot make rows written before the column read it as NULL,
    shredded mode (opt-in) keeps AE shadow DDL: block document writers with new
    keys can get 40P01, and an idle reader of a document can stall reads of
    the column behind a new-key INSERT. That is stated, not claimed away.

## 15. Coordination with rasterizeddb-99 (owner of `core/index`)

New code lives in **`core/index_store/`**. Shared call sites get a message to
the other sessions first: `row/table.rs`, `vacuum.rs`, `database/mod.rs`,
`mvcc/transaction.rs`, `filter/`, `pgwire.rs`. 99 sees every `executor.rs` and
`table.rs` hunk before commit.

**Where the hooks land:** `feat/secondary-indexes`, 99's branch, with 99's
agreement, reaching `feat/partitioning` by merge. Its first commit moves xxh64
to `core/hash/xxh64.rs`, so C5 does not drag `core/partition` onto 99's
branch.

**Hooks inside `core/index`**, each its own small commit with no behaviour
change, in the owner's style, reviewed hunk by hunk:

| # | Change | Phase |
|---|---|---|
| C5 | `key::ENCODING_VERSION`, golden vectors, coverage test, bump rule for `key.rs` and `canonical_key`; `TEXT_RENDER_VERSION` (rendering and navigation) and `ITEM_DERIVE_VERSION` with document vectors; gin items through `ItemFeed` (fixed-width LE, no `Hasher` impl) into a frozen in-tree hash in the in-memory `GinIndex` (xxh64 at first; rapidhash V3 since `ITEM_DERIVE_VERSION` 2, through the crate-private `ItemFeed::rapid` and its source test), `GIN_HASHER_ID` (touches `jsonb.rs`, its own commit); `KEY_LOOKUPS` | 0 |
| C6 | `TableIndexes::shape_hash()` (§4) | 0 |
| C10 | Signatures: every `LiveIndex`, `GinIndex` and `TableIndexes` mutator, `load_sorted` included, takes `Proof<'_>` (a one-byte `Copy` struct of a private `ProofKind`, built only by `From<&WriteTicket>`/`From<&BuildToken>`, no `Default` or conversion from a kind; the tokens, minted only in `core::index_store::gate`, have no `Clone`/`Copy`/`Default`/`From`); test callers use `test_mint` (`cfg(test)` or the dev-only `test_tokens` feature); the three-part source test (`ColumnFilter` included); a read-only `IndexView` for `indexes()` and the catalog views (§2). Not a visibility change: `pub(in crate::core::row)` is E0742 | 0 |
| C3 | `for_each_sorted` on `LiveIndex` and `GinIndex`: entries, `UNKEYED`, `PRESENT`; gin drops exactly `forgotten_slots`; no presence filtering | 2 (gin 3) |
| C9 | `load_sorted` on `LiveIndex` and `GinIndex`: build from a sorted stream, refuse a non-increasing slot, restore present bits; gin recomputes `live` and `entries`, `forgotten = 0`, `forgotten_slots` empty (§4) | 2 (gin 3) |
| C11 | `resident_bytes()`: an O(1) atomic byte counter on `LiveIndex` and `GinIndex`, maintained on insert, replace, forget, clear and load (§9) | 4 |

C7 (incarnation in `_schema.db`) is shared schema code, phase 0. The
`core/filter` changes (`serialize_sealed`, `FILTER_DERIVE_VERSION` and its
sequence, bloom and plane vectors, `TableFilters` `Proof` signatures, `cleared_below`
in the FILTER section, the `Ok`-only backfill) are shared
code in phases 0-1. The previous revision's C1, C2, C4 and C8 move to v2.
Items 1-10 and the traps of item 11 are adopted verbatim as acceptance
criteria (§13), and `core/index_store` reaches into `LiveIndex` only through
these hooks.

## 16. v2 requirements: online persistence

v2 serves a hot set larger than RAM: immutable runs behind today's in-memory
index, a bounded block cache, a paged filter, opens without a rebuild. The
previous revision specified it and two reviews found it unsound: findings 1-18
in `/tmp/claude-1000/-mnt-Volume4-Projects-Rust-rasterizeddb/73fe5448-c3c0-4404-8aec-909577d48a99/scratchpad/partitioning/pidx-review-from-99.md`
and N1-N46 in `…/pidx-rereview-from-99.md` beside it (a session scratchpad;
the numbers are the stable reference, and the old design is in this file's git
history). Its mechanisms are not carried forward. **v2 must meet every
requirement below, each with a named failpoint under the power-loss harness and
a mutation.**

| # | Requirement | Findings |
|---|---|---|
| R1 | No removal (dead-slot delta, flushed forget, filter clear, sealed segment) persists before the outcomes it rests on are durable; a `Committed` outcome is never revoked | 1, N1, N7, N26 |
| R2 | A durable frontier covers only slots with completed hooks, post-commit applies included, across failed flushes and re-streams, debt clears between flushes, and retained deltas of old epochs; no debt slot below a frontier | 2, 3, 5, N2, N3, N20, N27-N30, N32, N42 |
| R3 | A builder never loses `[F_build, F_s)`; seq rules for merge outputs and bases proven against the dead-slot rule and drop-by-seq; builders stop on retirement | 9, N4, N6, N24, N40 |
| R4 | No gate, debt or flush wait inside `compact()`'s exclusive hold or behind a queued gate writer; a remap carries debt rows | 14, N8, N11, N16, N18, N19, N22 |
| R5 | No lost flush trigger or wakeup; no writer stalled to an age trigger | N9, N10, N43 |
| R6 | Nothing a retired instance does reaches disk: canary, sweeps, queued VACUUM FULL (v1's §7 covers reload, its `VacuumService` included) | 4, N12-N15, N23, N31, N46, RV14 |
| R7 | A renumbering demotes every in-memory and on-disk reader at the source, permanently under the old numbering (v1's §6 covers memory) | 10, N5, N17 |
| R8 | One manifest appender, serialized generation appends; no trust-narrowing record lost to a torn tail, mid-log corruption, retry crash or cancellation; index-store errors never make a table unavailable | 11, N21, N33, N41, N45 |
| R9 | Persisted, stable proof inputs: no decaying `freeze_lsn`; identity and constraint state comparable without rebuilding every UNIQUE on any constraint change; epochs reconciled | 8, 13, 17, N34, N38, N39 |
| R10 | Completeness across layers: `[n_slots, now)` kept; debt snapshot ordered against clears; per-index tails | 6, 7, 18, N32, N42 |
| R11 | Read cost: equality re-costing, filter narrowing to candidates' segments, cheap per-run counts for cap checks | 15, 16 |
| R12 | Non-persisted gin and path layers still forget after a restart (v1's C3 drops exactly `forgotten_slots`) | N37 |
| R13 | A failpoint per durability window: barrier sync failure, manifest compaction, generation roll, segment seal, remap publish | N25 |
| R14 | Only if phase 1's logged backfill times make it painful: **trusted tail resume**, so a crashed insert-only filter resumes past a durable frontier, with its own durability argument against bug (c) | decision 12, RV18 |

Frozen encodings (12, N35, N36, N44) are already v1 (§8).

**Trigger.** Build v2 only if, over a representative week of phase 4's
counters and sharding §14's resident-bytes-per-window counter:
1. open partitions' index and filter bytes, at the shortest idle window
   keeping reopen I/O under 25% of measured device bandwidth, exceed the
   per-process index budget (~half of RAM ÷ `processes_per_machine`); at the
   target, k = 3 above ~1-2% row-weighted active (0.9% on 512 GB at the high
   per-row cost) or k = 1 above ~5%;
2. snapshot rewrites exceed 25% of measured device write bandwidth at that
   window, or `rdb_index_snapshot_skipped_evict_total` shows written
   partitions routinely closing without a snapshot;
3. one partition's in-memory indexes exceed 8 GB and `ISOLATE KEY` cannot
   split it;
4. post-crash index readiness of the written set misses the restart target
   (sharding §11.5); or
5. sharding phase 3's follower-apply gate fails on index readiness and the
   shard locator is not chosen instead.

## 17. Review response

For the two reviews of the previous revision, v1 resolves the findings that
touch it. The rest concern machinery v1 lacks: **obsolete in v1, moved to v2
requirements** (§16).

| Finding | v1 status |
|---|---|
| 4 reload opens a second instance; N12-N15, N23, N31, N46 retirement | resolved by the reload rule (§7): DDL sidecar writes after the drain, `Ref` dropped before queueing, DROP marks the slot before removing it, `forget_pause` only after the drain; the canary and sweep parts are obsolete in v1 (R6) |
| 9 build `Ready` transition | resolved by eligibility (every index ready), today's in-place rebuild under `BuildToken`, and an audit that never swaps a side build in (§4); the builder is obsolete (R3) |
| 10 failed VACUUM FULL; N5 checkpoint after it, tasks re-enable; N17 composite-UNIQUE reader | resolved by `DIRTY`, epoch, demotion inside `TableFilters`, generation-checked readiness, success only on an `Ok` backfill, no snapshot while demoted (§6), then T4 (§5) |
| 11 torn manifest appends; N21 proof failure not durable; N33 lost trust-narrowing records | resolved by the marker-log rules (never shrink to end at `CLEAN`), gate state from the log including `Unknown`, unlink fallback (§1, §2) |
| 12 upstream derivations; N35, N44 feed enforcement; N36 navigation | resolved (§8): `ItemFeed` has no `Hasher` impl, so a native-endian write does not compile; filter derivations versioned too |
| 13 epoch reconcile | resolved: a mismatch rebuilds, and §3 rule 2 makes the next clean point write a snapshot recording the current epoch, even on a read-only partition |
| 17 identity and orphans; N38, N39 | resolved by one `shape_hash` in header and `CLEAN`, all or nothing (§4) |
| N1 filter clear before the barrier | resolved by T4 (§5), closing today-bug (c) |
| N7 SIGKILL cannot lose staged data | resolved by the power-loss harness (§11), cut at completed syncs |
| N25 windows without failpoints | resolved for v1's windows (§11: barrier, marker compaction, unlink fallback, TRUNCATE, `CLEAN` fsync failure); the rest are R13 |
| N26 `Committed` flipped to `Aborted` | resolved by fail-stop, or tickets held until the outcome is final plus the revocation counter re-read before `CLEAN` (§2, §3) |
| N37 forgotten slots after a restart | resolved for v1's gin sections by C3 dropping exactly `forgotten_slots` (§3); R12 keeps it for v2 |
| N41 a failed reconcile fails the open | resolved: the open future never writes; failures rebuild (§4) |
| 1 (R1); 2, 3, 5, N2, N3, N20, N27-N30 (R2); N4, N6, N24, N40 (R3); 14, N8, N11, N16, N18, N19, N22 (R4); N9, N10, N43 (R5); 8, N34 (R9); 6, 7, 18, N32, N42 (R10); 15, 16 (R11); N45 (R8) | obsolete in v1, moved to v2 requirements. Finding 1's barrier still runs once per snapshot (§3) |

**2026-10-17 verification round** (43 corrections, all kept): packed gate
word with `fetch_add` tickets; `Unknown` state; `PUBLISHING`/`CANCEL`;
`BuildToken` and clean-point rule 2; `serialize_sealed`; Clean-only audit;
excluded tables keep no persistent filter; crash images cut at completed
syncs; gate failures through `failed_appends`; filter derivation vectors;
`Ok`-only compaction backfill; sidecar writes after the drain; tickets held
until the outcome is final; `ItemFeed`; the log never shrinks to end at
`CLEAN`; C3 drops exactly `forgotten_slots`; `Ref` and `forget_pause`
handling; load at the routed open and fan-out FILTER reads; phase 1's
idle-close hook; shutdown plan; C11; follower windows; `.tmp` landing; fd
term 10; load streaming and semaphore; clean-victim eviction; corrected sizes.

**v1 review** (rasterizeddb-99, 2026-10-17: 27 confirmed, 1 HIGH, 12 MEDIUM,
14 LOW; RVn is that review's Vn, not §13's rows). All are resolved in the
design; none is deferred. The eleven refuted claims are not acted on.

| Finding | Section | Fix |
|---|---|---|
| RV1 HIGH: the reload drain waits on the DDL's own transaction's ticket | §7 step 3, §2, §3, V10 | the caller's statement and its family's one ticket are left out of the drain (exactly, per family: P2); other holders' statements run ahead and real cycles get 40P01 (P1, P17); the gate belongs to the file set, so the ticket stays valid across the swap; in-place and `pending_filters` leave no post-commit work in a block; `rdb_index_checkpoint` on one's own written table returns at once; test under `lock_timeout = 0`, mutation counts the caller's ticket |
| RV2 M: vacuum re-clears after every reopen and dirties churned partitions | §2, §4, V3, V8, phase 1 | `cleared_below` persisted in FILTER and restored; candidates collected outside the closure; one ticket per batch only if a candidate has non-empty codes or is held |
| RV3, RV8, RV13 M: `pub(in crate::core::row)` is E0742; trybuild cannot pin it | §2 C10, §15, V3, phase 0 | `&Proof` (`WriteTicket`/`BuildToken`) in every mutator signature, minted only in `gate`; compile_fail doctests paired with compiling twins (stable does not check error codes); source test against a `pub(crate)` mint |
| RV4 M: a poisoned WAL passes the barrier | §3 steps 3-4, failpoints, V7, risk 1 | barrier syncs through a poison-checking primitive, never `sync_pending`, and is the one poison check (the `diverged` flag was dropped, P26); `apply_held_fails_then_shutdown` |
| RV5 M: an old binary's writes leave `CLEAN` trusted | §4, §5, §10, V8, risk 6 | a `_flt.db` beside `.ixm` fails the proof; `.ixs` unlinked before `_flt.db`; `index_store_v1` set before the first `.ixm`/`.ixs`; downgrade cycle with in-place UPDATE and forget; `flt_evidence_before_ixs_unlink` |
| RV6 M: sidecar names collide with table files | §1, §5, V13; partitioning §12, sharding | `.ixs`, `.ixs.tmp`, `.ixm`, `.ixm.tmp` and `rdb.ixe`, which no table file can be (P20, P29); `_flt.db` stays reserved in `RENAMED_SUFFIXES` and is never deleted when it is a live table's rows |
| RV7 M: writers cannot preempt an optional seal | §2 `ticket()`, §3 step 1 and Cost, V7 | a writer that sets `CANCEL` before `PUBLISHING` proceeds at once; wait bounded by one `PUBLISHING` window; sealer abandons at its next check |
| RV9 M: the audit wants snapshot == rebuild | §4 Audit | containment in the dangerous direction over non-reclaimable slots; other differences counted as soft divergence |
| RV10 M: `FILTER_DERIVE_VERSION` misses the zone comparator and SuperBloom hashing | §8, V11 | both in the version; sequence vectors for `observe` and `SargOp` outcomes; bloom vectors |
| RV11 M: the structural oracle fails correct code and cannot see forget/clear | §11 Oracles, phase 3, V3 | answers plus superset oracles; forget and filter clear pinned by the census and `capture_race` |
| RV12 M: the `Unknown` rewrite has no failpoint | §2, §11 | directory-operation hook; `unknown_rewrite_after_first_write`, anchored after the first write (P22), with its mutation |
| RV14 L: a retired instance's vacuum writes after the new `CLEAN` | §2, §7 step 4, R6, V10 | file-set gate; retirement joins `VacuumService`; `alive` checked between neutralize batches; a ticket per batch; retired handles refuse tickets |
| RV15 L: `GatedIo` checks with a plain load | §2 layer 2, loom, failpoints | each write registers with the ticket RMW until it returns; `ticketless_write_parked_in_provider`; loom covers `GatedIo` |
| RV16 L: `compact()` takes its ticket inside `compaction_gate` | §2 lock order, §6, V9 | ticket before every table lock; reader-delay test |
| RV17 L: shutdown releases queued writers after `CLEAN` | §3 Shutdown | sessions terminated (57P01) before shutdown seals; `seal_timeout` inside the deadline |
| RV18 L: PK probes scan during the post-crash rebuild; `backfill_on_open = off` | §4 Fallback, §5, risk 4, phase 1 | stated; unique B-trees built first; INSERT p99 gate; rows and seconds logged; R14; `off` refuses the snapshot |
| RV19 L: gin load leaves `live` at 0 | §4, §15 C9, phase 3 | load recomputes `live`, `entries`, `forgotten`; load-forget-compact test |
| RV20 L: `_flt.db` deletion misses KV objects and mirrors | §5, V8 | deleted through the provider family; excluded tables refuse writes while it survives |
| RV21 L: size reporting, knobs and tests still use `_flt.db` | §10, phase 1 | sizes from `owns()`; knobs deprecated; three test sites rewritten |
| RV22 L: the twin suites cannot reach crash images | §11 Suites | `index_store_failpoints` feature; file-backed; twin a separate database without filter; superset test limited to C3/C9 |
| RV23 L: the census is vacuous for in-memory mutators | V3 | a fresh clean point per mutator; journal records each in-memory mutation |
| RV24 L: the p = 1 kill claims are wrong | §11 keep probability, failpoint table, V2 | which seeds kill which mutation stated; `DIRTY` after a WAL'd write moved to the census and `sync_audit` |
| RV25 L: reordering failpoints sit where the mutant is invisible | §11 table, §6, V9, V12 | `invalidate_after_first_compaction_batch`, `dirty_enospc_after_first_write`, `truncate_after_first_effect` with an unsafe mutation |
| RV26 L: four failpoints need length-preserving writes | §11 | every "write without `DIRTY`" failpoint runs the four variants; `ixm_read_eio_at_open` closes without a clean point |
| RV27 L: a failed-after-`Committed` statement can survive | §11 Oracles, failpoint table | either state accepted for errored or in-flight statements; strict claims on the build counter |
| Decision 12 | §7, §5, risk 7, R14 | `lock_timeout` default `0`, session value honoured, server default opt-in; backfill rows and seconds logged |

**Verification of the v1-review fixes** (33 problems, P1-P33, 2026-10-17).
Applied except P10, whose claim the owner's review had refuted (tokens are
not bound to a gate, and C10 does not claim they are; §2 now says so).

| Problem | Section | Fix |
|---|---|---|
| P1, P17 (critical): the drain waits on other families' tickets while their next statement queues behind it; crossed DDLs and a row-lock cycle hang under `lock_timeout = 0` | §7, V10, risk 7 | ticket holders run ahead of the queue until retirement (PostgreSQL's ProcSleep rule); the mutex, queue and drain waits enter the `row_locks` wait-for graph, so a cycle gets one 40P01 |
| P2: ticket ownership undefined; savepoints are separate `TxId`s | §2 Ticket ownership, §7 step 3, V10 | one ticket per (family, file set), keyed by root, kept across savepoints, `ROLLBACK TO` and `RELEASE`; the drain exempts exactly the caller family's |
| P3: ALTER rewrites read before any drain and drop the table, losing a committing writer's row and giving 42P01 | §6, §7, V10 | the whole rewrite is one reload's step-5 work, after the drain; no `Dropped` state; caller reinserts under a new ticket on the new word |
| P4: the new waits are neither cancellable nor all bounded | §7 step 3-4, V10 | all three poll `cancel::tripped()`; mutex shares `lock_timeout`; newcomers honour their own `lock_timeout` and `statement_timeout`; steps 4-6 on an uncancelled task; `idle_in_transaction_session_timeout` noted |
| P5, P25: drain and retirement circular | §7 step 4 | retire only after the drain: queue, mark retired, clear `alive`, join (one batch), assert; 55P03 touches nothing |
| P6: word continuity across reload, DROP, RENAME | §2, §6, §7 | first open derives the word; reload keeps it; tickets hold an `Arc`; RENAME and MOVE carry it; no-wrap assertion |
| P7 (critical): two-RMW preemption lets a held writer follow `CLEAN`; stale `CANCEL` | §2, §3 step 6, loom | `PUBLISHING` CAS requires count zero; the second RMW is a CAS on the same optional seal, else back out and retry; same for `GatedIo`; loom assertion and two mutations |
| P8: `GatedIo` panics on a legitimate preempting writer | §2 layer 2, §11, V7 | `SEALED`+`CANCEL` admitted; panic only on `SEALED` without `CANCEL` and a zero count; `held_writer_during_cancelled_seal` |
| P9, P13, P14, P28: C10's source test misses the only possible mint; doctests are external; `Proof` undefined; benches and filter tests need a mint | §2 C10, §15, §12, V3 | `Proof<'a>` enum; no `Clone`/`Copy`/`Default`/`From`; source test over `gate.rs`'s returning items; one doctest pair with a parameter-taking twin; `test_tokens` dev-only feature; full caller list |
| P11: hand-listed mutators | §2 C10, V3 | source test over the four types' impl blocks with a read-only allowlist; v1's new mutators listed |
| P12, P31: preemption ignores the landing and device writeback; compaction inside `PUBLISHING` | §3 steps 5-6, Cost, §1, §12 | bound qualified; `sync_file_range` during capture; marker compaction before the CAS; phase-1 p99 on ext4 consumer disk and tmpfs |
| P15: maintenance lock undefined | §3, §2 lock order | defined; capture holds index read locks per block; no writer-path lock across fdatasync, rename or barrier |
| P16: filter `BuildToken` predicate | §2 | unready per structure; open path mints before installing the filter |
| P18: filter audit compares codes of different widths | §4 Audit, V8 | recompute each slot's code at the snapshot's width; bloom and bounds checked; no scratch filter |
| P19: legacy `<t>_flt.db` deleted before its owner opens | §5, V8 | ownership from the full catalogue name set |
| P20: `rdb_ixe.db` is a legal table's rows file | §1, §6, §10, V13; partitioning §7.1, sharding §11.1 | renamed `rdb.ixe` |
| P21: plane layout outside `FILTER_DERIVE_VERSION` | §8, V11 | layout versioned; plane and probe vectors |
| P22: `unknown_rewrite_after_rename` anchored where the mutant is invisible | §11, V2 | `unknown_rewrite_after_first_write` |
| P23: `cleared_below` orderings | §2, V3 | advance after the clears; vacuum waits for the load; `FORMAT_VER` bump |
| P24: neutralize ticket under `compaction_gate` | §2, V9 | one ticket per pass, taken before the gate, then rescan |
| P26: `diverged` and the poison checks mask each other | §3 steps 3-4, §11 | `diverged` dropped; the barrier is the one poison check |
| P27: ambiguous statements and batch commits | §11 Oracles | one state per ambiguous batch per run, from an unprunable scan |
| P29: unnamed marker-log temp | §1, §10, partitioning §12, sharding §10.1 | `.ixm.tmp` in `owns()`, drop, S3 exclusion and the reaper |
| P30: shutdown stops only sessions | §3 Shutdown, V7 | vacuum, replication and `flush_loop` stopped first; termination waits out a post-`commit()` path |
| P32: index-store stem source | §1, V13 | the physical name, never `schema.name` |
| P33: excluded tables unwritable after a transient delete failure | §5, V12 | retry at the next write, single-flight |

**Re-check of the P-fixes** (rasterizeddb-99, 2026-10-17: R1-R12, 2 HIGH, 5
MEDIUM, 5 LOW, plus four items still open from before; Rn is the re-check's
number). HIGH 1 follows the judged choice among three designs: one owner-keyed
table-lock manager, lockless Describe and catalog reads, with grafts from the
other two. It supersedes the §7 mechanism the RV1, P1, P2, P4, P5 and P17 rows
name (reload mutex, slot queue, run-ahead, own-family exclusion, family set);
their outcomes stand. The re-check's gate-ddl scope is to be re-run against the
new §7.

| Item | Section | Fix |
|---|---|---|
| R1 HIGH: Describe, `param_infer`, the prepared cache and catalog lookups carry no family, queue behind a drain with no edge, and hang every extended-protocol client | §7, §2 Ticket ownership, §12, V10 (2-7, 14), risks 10-11 | four-mode table locks keyed by family root with the ProcSleep grant rule; `schema_view` for Describe, `param_infer`, `learn_prepared` and catalog views (a stated deviation: PostgreSQL takes AS at Describe); `table_for` with a private map for execution; an owner before the first lock, `owner_alias`, no `FROZEN_TX` holder waits; owner-first victims; mutex, queue, run-ahead and family set removed; weak-mode fast path, ext4 budget |
| (graft) Execute's shape repair stops after the first notice, so sqlx decodes later Executes against a stale shape, today | §7, V10 (4), phase 0 | the shape the client was told changes only at Describe; RowDescription on every Execute until re-described, compared against the produced columns; never 0A000 (item 14 corrected); `shape_epoch` re-checked after the lock; generation bumped after the `ArcSwap` store; driver gate, with waiting Describe for non-holders as the fallback |
| R2 HIGH: deferred clear batches apply pre-compaction slot numbers after VACUUM FULL or TRUNCATE and prune a live row | §2 Deferred batches, §6, §11, V9 | slot epoch read under `compaction_gate.read()` at pass start; each clear and forget batch re-reads it under ticket then gate, held through the batch; any change discards the rest of the pass, no bound advance; `vacuum_batch_after_compaction`; mutations: no re-check, start epoch read without the gate |
| R3 M: a 40P01 victim keeps its ticket until ROLLBACK | §7 An error ends the block, V10 (8) | any error in a block with no open savepoint aborts at once (`abort_transaction`, `flush_outcomes`), releasing locks, tickets and edges; 25P02 after, ROLLBACK a no-op; under a savepoint stricter than PostgreSQL (v2) |
| R4 M: JSON shadow DDL inside INSERT/UPDATE drains writers on each other, non-retried 40P01 | §7 JSON shadow DDL, V10 (11) | shadow DDL listed as AE callers; keys first, AE in name order while holding nothing on the shadows; that phase's 40P01/55P03 retried in autocommit; blocks surface one 40P01 |
| R5 M: rewrite text said rows go under the caller's id; code writes them frozen | §7 ALTER rewrites, §6, V10 (12) | committed rows frozen; the caller's uncommitted versions under the `TxId` that wrote them; committed rows it deleted keep that `xmax` |
| R6 M: C10 source test reads only `gate.rs` return types | §2 Pinned by (1), V3, risk 2 | `syn` over the whole crate: literals only in the three mints, wherever the module (child modules and `#[path]` targets scanned); no macro mint; no hand-out by return, closure bound, `Deref`, field or static outside an allowlist; mint callers pinned; no `ptr::read`/`transmute`/`zeroed`/`forget` near tokens; five new mutations |
| R7 M (V4): a WAL error after the commit marker need not poison | §3 step 4, §11, V7, §12 | confirmed in `wal.rs` (`commit_through_slow`'s lazy open returns `Err` unpoisoned); phase 0 makes every `commit_through` error a poisoned log (eager handle, checkpoint installs it, reopen and lazy open poison, debug assertion), so the barrier keeps its assumption; `commit_through_open_fails_after_marker`, split by Q14 |
| R8 L: retirement sets `CANCEL` without the CAS rule | §7 step 2, §2 loom | CAS only on an unpublished seal; retirement in the loom model; mutation `fetch_or` |
| R9 L (V5): `Unknown` slow path deletes `_flt.db` before `DIRTY` is durable | §2 `Unknown`, §5, §11, V8 | log durably `DIRTY` or `.ixs` durably gone first, one rule for every path; a failed `.ixs` unlink keeps `_flt.db` and starts `Unknown`; `unknown_flt_delete_order` |
| R10 L: `test_tokens` reaches `target/release` | §2 Callers without `cfg(test)`, source test | `main.rs` refuses to start with either test feature; `test_mint` callers pinned; sentence corrected |
| R11 L: persisted `cleared_below` skips `xmax = NO_TX` | §2, §11 | candidates are reclaimable with `xmax == NO_TX` or `xmax >= bound`; pruning-quality check |
| R12 L: two P23 mutations survive every oracle | §11 Oracles, V8 | pruning-quality oracle: `CODE_EMPTY` on every reclaimable slot after reopen and a tick; prune count equals the never-restarted instance's |
| RV7 (open): device-level coupling under an optional seal | §3 Cost, V7 | closed at the gate; at the device measured, not claimed: commit p99 and gate wait during 30 s snapshots of a 12M-row table, ext4 and tmpfs, with a stated pass rule and remedy |
| RV10 (open): no `FILTER_DERIVE_VERSION` coverage enforcement | §8, V11 | exhaustive `DbType` match (18 fingerprinted, BOOL bounds-only, 29 excluded, by declared type) and `SargOp` match, each class's vectors required; constants pinned; `choose_f` and HLL excluded as pruning-only |

**Verification of the HIGH 1 revision** (19 problems, Q1-Q19, 2026-10-17;
Qn is that verification's number). All are applied. Q2 is modified: the
default stays `rowdescription`, not 0A000, because sqlx is the one driver shown
to need a mode and it cannot be identified at startup. Q4 is applied with a
stated fallback if the NULL decode cannot land in phase 0 (risk 12).

| Problem | Section | Fix |
|---|---|---|
| Q1 critical: a statement's CTE, derived-table, catalog-view and `drop_temps` sub-executions run under `FROZEN_TX` or a second `begin_autocommit` id, so one task deadlocks with itself and no edge shows it | §7 Owner, Granting, V10 (15), risk 11 | every sub-execution under the statement's owner; the `WITH` transaction is O; `LockOwner` newtype that cannot be built from `FROZEN_TX`; mutations: derived body under `FROZEN_TX`, `cte_tx` not aliased |
| Q2: never-0A000 removes what pgjdbc and npgsql heal from; the fallback misses statements described before the DDL | §7 shape bullets, driver gate, V10 (4), risk 10 | per-session `rdb.shape_change` (`rowdescription` default, `error` = 0A000); per-driver default where the startup packet names it; fallback scoped to Describes during the AE; gate and case 4 run in both modes |
| Q3: catalog builders read instance stats that `schema_view` did not expose | §7 Describe and catalog reads, V10 (6) | `schema_view` gains a lock-free stats accessor (provider lengths, `last_row_id`, `owns()` lengths), old figures during a rewrite; `pg_class` within 100 ms behind an idle reader's pending AE |
| Q4: shadow DDL as AE makes block document writers deadlock and SELECTs stall | §7 JSON shadow DDL, Modes, V10 (11), risk 12 | metadata-only nullable shadow ADD COLUMN under an internal per-shadow lock; AE only for widenings, retryable in autocommit; case 11 requires zero 40P01 and a non-waiting SELECT; "as PostgreSQL would" removed |
| Q5: O's liveness undefined; `live` filters ignore non-`InProgress` holders | §7 Owner, risk 11 | O is begun in `TransactionManager`, `InProgress` throughout, never forgotten while held, ended by a guard on every exit path (the reload task takes it over); mutations: O never begun, O not ended on cancel |
| Q6: a capped reader can be the youngest victim against `LOCK TABLE` | §7 Victims, V10 (16) | preference: un-retired DDL, then any S/AE requester, then the youngest non-reader; every cycle through a reader holds an S/AE requester |
| Q7: a per-instance `shape_epoch` can repeat after DROP/CREATE | §7 `shape_epoch`, V10 (17) | drawn from one database-wide monotonic counter |
| Q8: DROP's shadow chain is locked after retirement, uncancellable | §7 step 1, V10 (18) | step 1 takes the complete lock set in name order (shadow chain, both names); no `table_for` wait after 2(a), asserted |
| Q9 critical: async backfill and builds write pre-compaction slots after VACUUM FULL | §2 BuildToken, Deferred work, §6, §11 | `BuildToken` records the epoch; each batch re-checks under `compaction_gate.read()` and abandons with no `clear_unindexed_prefix` or `set_ready`; `backfill_batch_after_compaction` |
| Q10 critical: vacuum publishes or matures pre-compaction free space, overwriting moved rows (today-bug (e)) | §2 Deferred work, §0, §6, §11, phase 0 | publish and quarantine re-check the epoch under the gate, the no-batch path included; `compact()` clears `quarantined`; no-batch and quarantine variants |
| Q11: TRUNCATE does not exist in the engine | §2, §6, §7 Modes, §11 | marked future work (an AE reload, protected by retirement's vacuum join); TRUNCATE variant and `truncate_after_first_effect` removed |
| Q12: the `Unknown` word's state after a successful rewrite and a failed `_flt.db` delete | §2 `Unknown` and layer 1, §5, §11 | leaves `Unknown` only after the delete returns `Ok`; EACCES-on-delete variant |
| Q13: failures after the keep-tail rename leave writer and sync handle on the old inode with a false `Ok` | §3 step 4, §11, phase 0 | any failure after `rename` returned `Ok` and before both are replaced poisons; handle installed before the writer lock is released; `keep_tail_after_rename_fails` |
| Q14: `commit_through_open_fails_after_marker` cannot happen under the eager handle | §11, V7, phase 0 | split into `keep_tail_after_rename_fails` and `lazy_open_fails_after_marker` (test hook); a counter kills "leaves the handle to a lazy open" |
| Q15: C10 part (2) checks only `pub`/`pub(crate)` inherent fns; descendants write private fields | §2 Pinned by, V3, §15 | every fn of every impl, any visibility, `ColumnFilter` added; part (3) bans `ExprField` on mutable private fields outside their impls |
| Q16: syn leaves macro bodies unparsed | §2 Pinned by, V3 | token scan of every invocation plus a parse attempt run through the visitor |
| Q17: aliases, renames and tuple-struct construction defeat name matching | §2 Pinned by, V3 | no `type` alias, `use … as` or renamed re-export of the tokens; both are braced named-field structs |
| Q18: enumerated duplication and leak lists miss forms | §2 Pinned by, V3 | no `unsafe` in `index_store`, token-naming fns or fns touching ticket fields; leak list by path or method |
| Q19: `truncate_locked` leaves `write_pos` stale after `set_len(0)` | §3 step 4, §11 | reset at once after `set_len(0)`; later failure poisons; variant of `keep_tail_after_rename_fails` |
