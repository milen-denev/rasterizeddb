# Hash partitioning

Status: **phases 0, 1, 2a and 2b built** (2b through S20, 2026-10-24); **not
advertised**, and **phases 3 to 7 designed, not built**, except phase 3's P3
(composite identifying keys, [as built](#phase-3-p3-as-built)), P1
(metadata DDL, [as built](#phase-3-p1-as-built)) and P4 (foreign keys both
ways, [as built](#phase-3-p4-as-built)). Phases 0 and 1 are
the trailer scan and the pure `core/partition` modules; 2a the seam, the
table, `CREATE`, catalogs, routed `INSERT`, single-snapshot fan-out
`SELECT`/`COUNT`, routed `UPDATE`/`DELETE` and `VACUUM` over the partitions;
2b prepared `SELECT` from the learned parse with late routing, a cached route
derivation for full-key point reads, `RETURNING`, `ON CONFLICT` and joins.
What is still refused, and which phase lifts it, is §2's table; the summary
is [Phase 2b as built](#phase-2b-as-built). Left from 2a: S12 (a cheap
cross-partition `ORDER BY` merge and the statement worker budget, §8), which
waits on the scan-concurrency owner, and §15's ext4 measurements. "Advertised"
waits on one idle-box wall-clock A/B (§15). Line anchors were read on 2026-10-16 at
`61e9200` and are relative to `rasterizeddb_core/src/`. `core/row/table.rs`,
`core/vacuum.rs` and `core/filter/sarg.rs` are being edited by another session,
so anchors there will drift; the function names are the stable part. Siblings:
[Persistent indexes](persistent-indexes.md), whose v1 replaces phase 3's
rebuild on open after a clean close (§7.2), and [Sharding](sharding.md), which reuses the hash, range map
and router across nodes and fixes the deployment layout (sharding §11).

```sql
CREATE TABLE assets (
  tenant_id      BIGINT NOT NULL,
  asset_group_id BIGINT NOT NULL,
  asset_id       UUID   NOT NULL,
  body           TEXT,
  PRIMARY KEY (tenant_id, asset_group_id, asset_id)
) PARTITION BY HASH (tenant_id, asset_group_id);

SELECT * FROM assets WHERE tenant_id = 42;                        -- one partition
SELECT * FROM assets WHERE tenant_id = 42 AND asset_group_id = 7; -- one partition
ALTER TABLE assets ISOLATE KEY (tenant_id = 42) TO '/mnt/nvme2';
ALTER TABLE assets SPLIT KEY (tenant_id = 42) INTO 8;          -- by asset_group_id
```

A partitioned table is one logical table whose rows are spread over many
physical file sets, **partitions**, chosen by a frozen hash of the row's
**partition key**. A statement that fixes the key, or its leading column,
touches only the partitions that can hold matching rows; any other reads all of
them under one snapshot. Each partition has its own files, locks, indexes and
vacuum state (§5), and can sit on its own disk.

## What was asked, and what this gives

| Ask | What this design does |
|---|---|
| A distinct subtable per tenant | **Not by default.** Tenants map many-to-one onto 1024 partitions by default from phase 3 (64 in phase 2a, §1.2); a tenant's rows are always in one partition. `ISOLATE KEY` gives one tenant a partition of its own. Isolating every tenant, or `PARTITIONS` near the tenant count, gives the literal version at 3 files and, when open, 15 fds per tenant, affordable to about 10⁵ partitions per node (§7); phase 3 caps a node at 65,536 (§15). |
| 100 tenants at once touch 100 files | **Mostly.** 100 random tenants land on about 95 distinct partitions of 1024, about 91 of them alone. 64 partitions give about 51 distinct, 256 give 83, 4096 give 99. Isolated tenants always get their own. |
| No contention | **Partly.** Today one tenant's `DELETE` blocks every `UPDATE` and `DELETE` of the table for its whole scan and rewrite (not `INSERT` or `SELECT`); here it blocks only its own partition. `INSERT` and point-`SELECT` throughput should not change: they never took that lock. Until phase 4 all partitions share one disk's log and fsync stream, and every commit shares the transaction manager and root outcome log (§5). |
| Practically infinite scale | **No, not in one process.** One outcome log and one transaction manager per database bound commit throughput. "Infinite" means sharding across nodes, a separate design ([Sharding](sharding.md)) that reuses this one's frozen hash, range map and statement-level routing. |
| Balance subtables across disks | **Yes**, per partition, chosen at first write and recorded in the table's own map (§6.1). First for databases without S3 backup; with backup it waits for snapshots covering pool locations (phase 4b). Gated on a crash test of a durability gap that multi-disk transactions make routine (§12). |
| Several columns in one key | **Yes, hierarchically.** `(tenant_id, asset_group_id)` behaves like `tenant_id` until a large tenant is sub-split by `asset_group_id`; a query fixing only `tenant_id` always routes to that tenant's contiguous sub-range (§1b). |

**Why not a subtable per tenant by default.** A table today costs about 25
file descriptors, 5 mmaps, 30 files and 3–4 background tasks waking about 5.6
times a second idle, and opens eagerly at startup with a full index rebuild.
`deploy.sh:213` sets `LimitNOFILE=65535`, capping a process near 2,600 tables,
and provider opens `.unwrap()` and panic on EMFILE, in
`LocalStorageProvider::new` (`core/storage_providers/file_sync.rs:643-717`) and
in `create_new` (`file_sync.rs:2054-2124`), which opens every table file. So
tenants hash into a bounded set of partitions, made cheap enough that the set
can be large, and the tenants that matter are isolated; the long tail pays no
per-tenant cost.

**The "index" framing.** An index here is a superset accelerator: in memory,
rebuilt at open, droppable for free ([Indexes](indexes.md)). A partition key
decides where bytes live, so creating one rewrites the table, as removing it
would. `CREATE INDEX … USING partition` is accepted as the user's spelling of
`ALTER TABLE … PARTITION BY` (§2); it replies that rows were moved, is not
listed in `pg_indexes`, and refuses `DROP INDEX`.

## What it is not

- **Not PostgreSQL's hash partitioning.** The hash is ours, so a PostgreSQL
  `MODULUS 4, REMAINDER 0` child would hold different rows; children are never
  addressable by name.
- **Not a value directory.** A subtable per key value routes exactly, but
  unrouted queries fan out over every tenant (minutes at 10⁶), a mistyped
  tenant id creates a subtable, and a per-tenant drop unlinks files under open
  snapshots. The hash map gives the same prefix routing and sub-splitting
  without those costs.
- **Not global uniqueness** (§1b), **not automatic** (splits and moves are
  statements, like `STORAGE REBALANCE`), **not multi-node sharding**
  ([Sharding](sharding.md) builds that on top), and not a
  way around the shared commit points of §5.

## Phase 2b as built

Built on `feat/partition-2b` (S15–S20, 2026-10-24), over phase 2a. Not
advertised: §15's exit criterion is met on instructions per op, and the
idle-box wall-clock A/B is still to run.

**What works on a partitioned table**, answering like a plain twin (I6):

- `CREATE TABLE … PARTITION BY HASH` (one to three key columns, default 64
  partitions, at most 2,048 declared per database), `DROP`, the catalogs and
  `SHOW PARTITIONS` (2a).
- `INSERT` in every form, routed per row; `SELECT` and `COUNT` routed or
  fanned out under one snapshot, with aggregates, `GROUP BY`, `DISTINCT`,
  windows, `UNION`, CTEs, `IN (SELECT …)`, `ORDER BY … LIMIT` and empty
  routes; `UPDATE` and `DELETE` routed or fanned out, all or nothing across
  partitions; `VACUUM` and `VACUUM FULL` (2a).
- Prepared `SELECT` and `COUNT` from the learned parse, routed per execution
  (S15, I11), under one snapshot on the fast path (I7); a full-key point read
  names its partition from its bound key (S16b, I2). Prepared writes take the
  text path every execution.
- `RETURNING` on `INSERT`, `UPDATE` and `DELETE`, from the pre-images the
  write itself read, never re-read (S17).
- `ON CONFLICT DO NOTHING` and `DO UPDATE` in every form: targetless, by
  columns, `ON CONSTRAINT`, with `WHERE`, `EXCLUDED` and `RETURNING`,
  multi-row and `INSERT … SELECT`, simple and prepared; each key unique under
  concurrency (S18, I10).
- Joins: inner, `LEFT`, self-joins, partitioned × plain and × partitioned,
  with CTEs, derived tables and aggregates, every relation and partition
  under the join's one snapshot, each routed on its own predicate; `UPDATE …
  FROM` and `DELETE … USING` reading a partitioned source into a plain target
  (S19, I3, I7).
- A key column assigned the value its `WHERE` pins (an ORM's every-column
  `UPDATE`, `SET tenant_id = EXCLUDED.tenant_id`) is an ordinary update and
  is eligible for the in-place overwrite, like a plain table (97e187f, §4.5).

**Still refused** (0A000, before anything is written; the full table is §2):

| Refused | Phase |
|---|---|
| ~~`UPDATE … FROM` / `DELETE … USING` with a partitioned target; `FOR UPDATE`/`SHARE`, `SKIP LOCKED`, `NOWAIT`~~ | built in phase 3 P3 ([as built](#phase-3-p3-as-built)) |
| ~~`ALTER TABLE` in every form, `RENAME`, `CREATE INDEX`~~ (metadata forms built in phase 3 P1, below); ~~FKs onto or from a partitioned table~~ (built in phase 3 P4) | physical-rewrite `ALTER` 6 |
| A key change: an `UPDATE` or `DO UPDATE` assigning a key column anything its `WHERE` does not pin to the same value | 6 (row moves, §9) |
| Conversion (`ALTER … PARTITION BY`, `CREATE INDEX … USING partition`) and reshapes (`SPLIT`, `ISOLATE KEY`, `SPLIT KEY`, `MERGE`, `MOVE PARTITION`) | 5 |
| `CREATE … PARTITION BY` as a temporary or unlogged table, in an in-memory database, in a mirrored pool, on KV storage, or with shredded JSON columns | not scheduled (KV until measured; off-root placement 4) |

**Known limits** (correct answers, not refusals, unless stated):

- **Joins do not infer a key route transitively.** `t JOIN d ON t.k = d.k
  WHERE d.k = 5` routes `d` but fans out over `t`: each relation routes on its
  own pushed-down predicate only (§4.1).
- **`k = 1 AND k = 2` is not proved empty.** Two equalities on one key level
  are unioned, never intersected (§4.2, step 3), so the statement reads both
  partitions and the scan returns no rows. Only a clause
  `clause_is_constant_false` proves false (a comparison of constants, such
  as `1 = 0`) routes to none.
- **A partitioned target of `UPDATE … FROM` / `DELETE … USING` was refused**
  in 2b, as was any statement that addresses rows by one identifying
  column: a partitioned table has only composite unique keys (§1b). Phase 3
  P3 built them ([as built](#phase-3-p3-as-built)).
- **The WHERE-arithmetic gap** reported with S18 is a plain-table evaluator
  limit and the owner's (rasterizeddb-47) to fix:
  `… ON CONFLICT … DO UPDATE SET … WHERE t.n IS NOT NULL AND EXCLUDED.n > t.n - 100`
  fails with 42601 "expected closing parenthesis" on both table kinds, while
  the same predicate without the subtraction runs. Routing never narrows on
  arithmetic (it widens, §4.2), so a partitioned table answers exactly as its
  plain twin does.
- **No statement worker budget and no cheap cross-partition `ORDER BY`
  merge** (S12, §8): each partition's scan starts its own workers, at most
  `FANOUT_IN_FLIGHT` = 8 partitions at once, and `ORDER BY` decodes each
  partition's top `k`.
- **A signed numeric literal is never learned** (`k = -1`): such a prepared
  read runs from its text and fans out (§4.1).
- **Phase 2a's resource limits stand**: every materialized partition opens at
  startup (lazy open is phase 3), every partition sits in the root location
  (placement is phase 4), and the default stays 64 partitions (§1.2, §7).

**Final twin pass (S20, at 97e187f):** `tests/partition_twin.rs` green in all
four configurations at the default seeds (0x5eed0001, 0x5eed7a11, 300
statements), at 0x5eed2b20 and at 0x5eedf1a1 with 1,500 statements each;
`KNOWN_DIVERGENCES` is empty, and `REFUSED` holds only the phase 3 and 6
kinds above. Every `partition_*` suite, core and compat, passes.

## Phase 3 P4 as built

Built on `feat/p4-fks` (from `116533d`, 2026-10-27), wave 2 of the phase 3
plan: **foreign keys to and from hash-partitioned tables**, answering like
a plain twin (I6, I24). Not advertised.

**What works**, each as on a plain table (the plain path's own changes are
listed at the end):

- **From a partitioned table**: `CREATE TABLE … PARTITION BY` with
  `REFERENCES` on a column or a table-level `[CONSTRAINT n] FOREIGN KEY`,
  and `ALTER TABLE … ADD [CONSTRAINT n] FOREIGN KEY`, onto a plain or a
  partitioned parent. The key lives in the shell's FK sidecar, so every
  partition shares it (§11's one schema). `ADD` validates the rows already
  there in **every partition** (23503 naming the first value with no
  parent; nothing is written), under ACCESS EXCLUSIVE on the child and
  SHARE on the parent, the locks a plain table's `ADD` takes
  (`alter_lock_set`).
- **Onto a partitioned table**: `REFERENCES p (k)` where `k` is a `UNIQUE`
  or `PRIMARY KEY` of `p`. By §1b such a key holds every partition key
  column, and a foreign key is one column (composite keys are refused on
  every table), so `k` **is** `p`'s whole partition key: `PARTITION BY HASH
  (k)`. Anything else is PostgreSQL's 42830 ("there is no unique constraint
  matching given keys for referenced table …"), at CREATE and ADD
  (`foreign_key::referenced_key_refusal`), and so is a partitioned table
  created under a name an existing key already names by another column.
- **Every action**, `NO ACTION`, `RESTRICT`, `CASCADE`, `SET NULL`, `SET
  DEFAULT`, on `DELETE` and `UPDATE`, in every direction the engine runs
  them for plain tables, routed (`WHERE k = v`) and fanned out, in
  autocommit and in blocks (a cascade lands in the deleting transaction
  and goes with its rollback), and after a restart. A partitioned parent's
  referenced key is its partition key, which no `UPDATE` may change before
  phase 6, so its `ON UPDATE` never fires (a same-value assignment changes
  nothing a child sees). An `ON UPDATE CASCADE`, `SET NULL` or `SET
  DEFAULT` that would write a partitioned child's **partition key** column
  is a key change, refused with 0A000 (phase 6) and failing the parent's
  `UPDATE`, as `ON DELETE SET NULL` of a key column fails on its NOT NULL.
- **DDL around them**: `DROP CONSTRAINT` of a key by its name (42704 when
  nothing matches), `DROP TABLE` of a referenced parent (below), `RENAME
  TO` of either side (a parent's children's records follow it,
  `retarget_parent`, and are reloaded), `RENAME COLUMN` of either side's
  column, `DROP COLUMN` of the child's key column (the key goes with it) or
  of a referenced column (2BP01 while a key names it; a partitioned
  parent's referenced column is its partition key, which is never dropped,
  0A000). The row-lock orphan check (55006) applies as before.

**How a check routes and locks.** One function reads either side,
`referenced_values_now(table, column, literals, tx, limit, side)`, as of
now on `tx`'s behalf, under ACCESS SHARE on the table's **logical** name
(`relation_for`):

- *The parent side* (`KeySide::Parent`: an insert's or update's existence
  probe and its recheck under the key lock, a multi-row insert's batched
  probe, `ADD`'s validation, `ON UPDATE`'s "is the old key still held"):
  on a partitioned parent the probe is `k = v` or `k IN (…)` in chunks of
  at most `POINT_CAP` (256) values, so it routes to the partition of each
  value. Before reading, the column must be the whole partition key, or the
  check is refused with "cannot be checked … not supported", never read in
  every partition and never skipped; only a record written behind the DDL
  could get there (`fk_partition_tests::a_check_that_cannot_route_is_refused_never_skipped`).
  Where `route_where` reads no key point in the predicate (a signed number:
  `k = -7` routes everywhere, §4.1's "signed literal" limit), each value is
  routed by its own key bytes instead (`parent_key_ranges`: the value
  encoded by `lexer_ir::encode_column_value`, the encoder an `INSERT` of it
  stores and routes by, so `-7::bigint`, `CAST(-6 AS BIGINT)`, `'x'::text`
  and a UUID's spellings route as the parent row did; then `keyed_range`),
  and only those partitions are read (`RowSource::partitioned_in`), the
  probe's predicate still applied in each. A value that encodes as no key of
  the column (`7.5` for a BIGINT) is read in every partition, its predicate
  deciding, as a plain parent's scan does (round 2; the first fallback
  refused it). The twin found the signed case at 0x5eedf1a1 with 1,500
  statements: the first build refused every child of a negative tenant
  (`fk_partition_tests::a_key_the_router_cannot_read_still_routes_by_its_bytes`).
- *The child side* (`KeySide::Child`: a parent `DELETE`'s or `UPDATE`'s
  "does any child reference these keys", before and after its action): the
  child's fan-out, routed when the key column is the child's partition key,
  else every partition, all under one as-of-now snapshot. The action itself
  is SQL (`DELETE FROM c WHERE col IN (…)`, `UPDATE c SET col = …`) through
  `execute_with_context`, so it routes as any statement does.
- *The referenced-key lock* is the plain path's (`RowLocks::acquire_referenced`):
  a child's check takes it shared on (parent's logical name, column, the
  parent row's stored key bytes) between its probe and its recheck; a
  parent's `DELETE` or key-moving `UPDATE` takes it exclusive on the same
  triple, its bytes read from the partition rows it is about to change,
  before it looks at any child. The partition never enters the key, so a
  child insert racing a parent delete meets the lock whichever partitions
  hold the two rows: the second to arrive waits, then refuses (23503), and
  no orphan is left (`partition_fk::a_child_insert_racing_a_parent_delete_leaves_no_orphan`,
  all four configurations, both orders).
- *One snapshot per statement.* A partitioned `DELETE` that some key
  references, and an `UPDATE` whose keys need it
  (`update_needs_statement_snapshot`), take the statement's `Visibility`
  first, read the keys it will remove or move over the routed partitions
  under it (`keys_about_to_be_deleted`, `keys_about_to_change`,
  `relation_rows_as_of`), and hand that same `Visibility` to every
  partition's write, a lone partition's too
  (`row_source::write_partitions_as_of`), as the plain arms give theirs to
  the table's write: the keys acted on are exactly the rows written.

**`Database.fk_reverse`** (`slots::FkReverse`): every foreign key by the
table it references (ASCII-lowercased, as every FK lookup compares names),
with the child that declares it. `Database::referencing_keys(parent)`
answers from it in child-name order, and the executor's `referencing_keys`
(every `DELETE`, every parent-key `UPDATE`, the CASCADE lock sets) and
`fk_children_on` (the DDL lock sets, `column_declarations`,
`retarget_parent`) read it; the O(tables) walk per `DELETE` is gone, for
plain tables too.

- *Maintenance.* It mirrors the schemas of the instances in the slots, so
  it is changed exactly where an instance is: the slot primitives
  `install_table`, `swap_instance`, `take_table` and `take_partitioned` set
  or remove the child's entries **under the index's write lock, together
  with the slot change**. Every DDL that changes what references what ends
  by putting an instance in a slot: CREATE (plain, and a partitioned
  shell's install), DROP (both kinds), RENAME TO (the old slot taken, the
  new installed; a partitioned rename's `reinstall_partitioned`; the
  children's reload after `retarget_parent`), ADD and DROP CONSTRAINT,
  DROP and RENAME COLUMN (`reload_with`, `reload_atomic_with` and the
  children's reloads), `DROP TABLE … CASCADE` (each child's reload), an
  ALTER rewrite, and every partitioned reload (`swap_partitioned`). At open
  it is rebuilt from every slot after the partitioned tables install
  (`rebuild_fk_reverse`): the plain tables enter the map before the
  `Database` exists.
- *Consistency check.* In debug and test builds every lookup compares its
  answer with a walk of every slot's schema, as multisets of (child, key),
  **under the index's read lock**, so no mutator is between its slot change
  and its index change; a disagreement panics. Every compat and core test
  that deletes from any table runs it. `assert_fk_reverse_complete`
  compares the whole index (every parent either side names, and no entry
  for a name with no slot) after each step of
  `fk_partition_tests::the_reverse_index_agrees_with_the_schemas_after_every_ddl`
  and after a reopen; the crash test's fingerprint reads it at every image.

**Names.** An FK's constraint name, column and both referenced names are
stored as **UTF-8** in their 128-byte slots (`foreign_key::write_name`, the
cut of `unique_key::stored_name`), as P1 stores UNIQUE and CHECK names;
they used to keep only ASCII letters, digits and `_`, so `"my fk"` was
stored as `myfk` and a DROP by the given name matched nothing. **No format
change**: the reader always decoded UTF-8, a record written filtered reads
back as written, and an older binary reads the new names. A table or
column name over 128 bytes is refused where the key is defined (0A000),
never cut (a cut name would name another table). A name given is read bare
or quoted (`split_constraint_name`, spaces, `""` and non-ASCII kept, cut
to its stored form); an unnamed key is PostgreSQL's `<table>_<column>_fkey`
(`foreign_key::default_name`, built by `name_with_label` so the label
survives a long table name), numbered `…_fkey1`, `…_fkey2` past every other
constraint name of the table (`choose_name`, bounded), at CREATE and ADD. A
named ADD whose name another constraint of the table has is 42710. `DROP
CONSTRAINT` matches the name in that stored form, and a name matching no
constraint is 42704 (P1's rule, which now reaches FKs stored by name).

**`DROP TABLE` of a referenced table** (plain or partitioned, the same
code, `drop_dependent_foreign_keys`): without `CASCADE` it is refused with
2BP01 ("cannot drop table p because other objects depend on it"), before
anything is dropped; with `CASCADE` each other table's keys naming it are
tombstoned (`foreign_key::remove_referencing`) and that table reloaded
under ACCESS EXCLUSIVE (taken with the statement's set, `ddl_lock_extras`),
then the table is dropped: the constraints go, never the tables holding
them, as in PostgreSQL. A self-reference never blocks. The drop used to
leave every child's key in place, naming a table that did not exist, so
each later insert of a non-NULL value failed, and a table created under the
name became their parent.

**Tests.** `tests/partition_fk.rs` runs every scenario on four
configurations, plain/plain (the twin), plain parent with partitioned
child, partitioned parent with plain child, both partitioned, and requires
each answer (rows, columns, tags) to be the twin's or the same SQLSTATE:
inserts and updates that break and keep each key (a multi-row insert, an
expression assigned to the key column), each `ON DELETE` action routed and
fanned out with what both tables hold compared, through a restart; blocks
rolling a cascade back; each `ON UPDATE` action from a plain parent; `ADD`
validating every partition, unnamed and numbered names, a quoted non-ASCII
name, `DROP CONSTRAINT` and 42704, through restarts; the 42830, 42703 and
42P01 refusals, a partitioned table created under a name a key already
names, a self-referencing partitioned table; `DROP TABLE` refused and
`CASCADE`; `RENAME` of either side and `DROP COLUMN` of either column; the
insert/delete race in both orders. `tests/partition_twin.rs` gains three
round-trip kinds (`ForeignKeyFromT` to a plain parent, named or unnamed;
`ForeignKeyOntoPartitioned`, the table under test referencing `{k}`,
partitioned by its key on the partitioned side; `ForeignKeyFromSink`, a
plain child of `{k}`), each with a random `ON DELETE` action, a cascade in a
rolled-back block, a referenced parent key moved, and the key dropped twice;
`AddForeignKey` leaves `REFUSED`, and the three are in `SUCCEEDING_DDL`.
`core/rql/fk_partition_tests.rs` counts the partitions each check reads
(the probe and its recheck in the key's one partition; ten keys at most
ten; a parent `DELETE`'s key read on its own route). The partitioned DDL
crash test (`pddl_crash_tests`) gains `ADD CONSTRAINT … FOREIGN KEY` and its
`DROP CONSTRAINT`, cut at every reload step under every seed kind, its
fingerprint reading the keys and `fk_reverse`: every image is the old table
or the new one. `partition_ddl`, `partition_shell_sites` and
`partitioned_tests` turn their FK refusals into the built behaviour.

**Mutations** (each made alone with a `// MUTATED` marker by a script,
its test failed by assertion, the file restored byte-exact and compared;
`grep -rn MUTATED` clean at the end):

| Mutation | Where | Test that fails |
|---|---|---|
| the parent probe's whole-key check off | `referenced_values_now` | `fk_partition_tests::a_check_that_cannot_route_is_refused_never_skipped` (the route check's message instead) |
| a partitioned parent probed in chunks of 512, past the route cap | `referenced_values_now` | `fk_partition_tests::add_names_the_one_value_without_a_parent` (the route check refuses the 512-value probe) |
| the partitioned probe planned on no `WHERE` (every partition) | `referenced_values_now` | `fk_partition_tests::a_check_of_a_partitioned_parent_reads_one_partition_per_key` |
| a statement's key read planned on no `WHERE` | `relation_rows_as_of` | the same (the parent `DELETE` reads every partition) |
| a partitioned `DELETE` skips its children's actions | `execute_partitioned_delete` | `partition_fk::inserts_updates_and_every_delete_action_answer_like_the_plain_twin`; the twin (`ForeignKeyOntoPartitioned`: the partitioned side deleted a referenced parent) |
| a partitioned `UPDATE` skips its FK work | `execute_partitioned_update` | `partition_fk::inserts_updates_…` (`pq`: a child moved to a missing parent) |
| a parent `DELETE` takes no referenced-key lock | `apply_on_delete` | `partition_fk::a_child_insert_racing_a_parent_delete_leaves_no_orphan` |
| a child's check takes no referenced-key lock | `check_foreign_keys_of` | the same |
| `swap_instance` leaves `fk_reverse` | `slots.rs` | `fk_partition_tests::the_reverse_index_agrees_with_the_schemas_after_every_ddl` |
| `take_table` leaves `fk_reverse` | `slots.rs` | the same |
| no rebuild at open | `Database::new` | the same (after a plain child was added; it survived while every child at the reopen was partitioned, whose shells install through `install_table`) |
| `set_child` keeps a child's old entries | `FkReverse::set_child` | the same |
| FK names ASCII-filtered again | `foreign_key::write_name` | `foreign_key::tests::names_are_stored_as_given` |
| an unnamed key named `fk_<col>_<parent>` again | `foreign_key::parse_clause` | `foreign_key::tests::an_unnamed_constraint_gets_postgres_name` (**survives** `partition_fk`: CREATE and ADD name an unnamed key themselves, so the parser's default only reaches the lock-set reader) |
| CREATE's unnamed keys not numbered | `lexer_ct` | `lexer_ct` tests |
| ADD's unnamed key not numbered | `add_foreign_key` | `partition_fk::add_and_drop_foreign_key_answer_like_the_twin` |
| ADD skips validation | `add_foreign_key` | the same |
| validation names a chunk's first value | `existing_rows_break_foreign_key` | `fk_partition_tests::add_names_the_one_value_without_a_parent` |
| a chunk with any parent proves all its values | `existing_rows_break_foreign_key` | the same |
| a non-unique column of a partitioned parent accepted | `referenced_key_refusal` | `partition_fk::a_partitioned_parent_is_referenced_by_its_partition_key_only` |
| a partitioned CREATE ignores the keys already naming it | `create_foreign_key_refusal` | the same |
| `DROP TABLE IF EXISTS` of a name only a dangling key names refused | `drop_dependent_foreign_keys` | the same |
| `DROP TABLE` of a referenced table not refused | `drop_dependent_foreign_keys` | `partition_fk::dropping_a_referenced_parent_is_refused_or_cascades_to_the_keys` |
| `CASCADE` keeps the dependent keys | `drop_dependent_foreign_keys` | the same |
| 42830 not mapped | `pgwire::sqlstate_for` | `partition_fk::a_partitioned_parent_is_referenced_…` |
| 2BP01 not mapped | `pgwire::sqlstate_for` | `partition_fk::dropping_a_referenced_parent_…` |
| a key the router reads no point in (`k = -7`) refused | `parent_key_ranges` | `fk_partition_tests::a_key_the_router_cannot_read_still_routes_by_its_bytes` |
| that key probed in every partition | `referenced_values_now` | the same (a negative key read more than its partition) |
| FK children's schema files held aside by their logical names | `reload_atomic_with` | `fk_partition_tests::a_failed_parent_column_rename_puts_a_partitioned_childs_key_back` |

*Round 2* (every mutation above run again over the round-2 code, killed by
the same tests, but the one marked; and these):

| Mutation | Where | Test that fails |
|---|---|---|
| a probe in chunks of 512 (past the route cap) | `referenced_values_now` | **survives, equivalent since round 2**: a 512-value `IN` routes everywhere, so each value is routed by its key bytes, and still only its partition is read |
| a routed probe reads every partition | `referenced_values_now` | `fk_partition_tests::a_check_of_a_partitioned_parent_reads_one_partition_per_key` |
| a value the router cannot read gets no key range (read everywhere) | `parent_key_ranges` | `fk_partition_tests::a_key_the_router_cannot_read_still_routes_by_its_bytes` (`-8::bigint` read more than its partition) |
| a partitioned write takes its own snapshot | `write_partitions_as_of` | `fk_partition_tests::a_partitioned_delete_writes_the_rows_its_keys_were_read_from` (the late parent deleted, its child orphaned) |
| `NOT VALID` still validates | `add_foreign_key` | `partition_fk::add_and_drop_foreign_key_answer_like_the_twin` |
| `VALIDATE CONSTRAINT` checks nothing | the ALTER arm | the same |
| the import aborts on a source orphan | `migrate::run::add_foreign_key` | `load_tests::a_foreign_key_broken_by_the_source_rows_is_added_not_valid_with_a_note` |
| an unvalidated source key planned as validated | `migrate::plan` | `plan::tests::a_source_key_never_validated_is_added_not_valid` |
| validation reads deleted and invisible versions | `existing_rows_break_foreign_key` | `partition_fk::add_and_drop_…` (a deleted orphan refuses VALIDATE) |
| validation reads a NULL as its zero bytes | the same | the same |
| the last chunk never probed | the same | the same |
| an inline `CONSTRAINT n` ignored | `lexer_ct` | `lexer_ct` tests |
| every pair of key types compatible | `foreign_key::compatible_key_types` | `partition_fk::a_key_between_incompatible_types_is_42804` |
| ADD skips the type check | `add_foreign_key` | the same |
| 42804 not mapped | `pgwire::sqlstate_for` | the same |
| *(round 3)* the implicit-cast pairs refused again | `compatible_key_types` | `partition_fk::cross_type_keys_compare_as_postgres_compares_them` |
| *(round 3)* no value coerced across types | `coerce_key_literal` | the same |
| *(round 3)* the parent side's keys not coerced | `parent_literals_for_child` | the same (deleting `TIMESTAMP` 10:00 refused by the `DATE` midnight child); it survived until parents off the integer and off midnight were added |
| *(round 3)* a `DOUBLE` narrowed to `REAL` through text | `coerce_key_literal` | the same (`DOUBLE` 0.1 found `REAL` 0.1) |
| *(round 3)* a lossy move kept | `coerce_key_literal` | the same |
| *(round 3)* validation probes the child's values unmoved | `first_value_without_parent` | `…::adding_a_cross_type_key_validates_as_the_insert_checks` (`TIMESTAMP` 10:00 validated against `DATE`; it survived until that case was added, since the scan reads `7` at `NUMERIC` by itself) |
| *(round 3)* VALIDATE of an unknown name succeeds | the ALTER arm | `partition_fk::validating_an_unknown_constraint_is_42704` |
| *(round 3)* the ALTER arm, CREATE's `foreign_key_clause`, the partitioned refusal, `parse_clause`: `REFERENCES` read as a substring (each alone) | those four | `partition_fk::a_name_holding_references_is_no_foreign_key` (the partitioned refusal's: a UNIQUE without the key accepted) |
| the table dropped before its dependent keys | `execute_drop_table` | `pddl_crash_tests::a_cascading_drop_…` (images at the log's syncs hold a key naming a dropped table) |

Not pinned: the child-side chunk of 512 for a child not partitioned by its
key column (a cost, measured above, not an answer); `apply_on_update` after
a partitioned `UPDATE` cannot run before phase 6: a partitioned parent's
referenced key is its partition key, which no `UPDATE` may change.

**Round 2** (2026-10-28), from two reviews of `b00d905` (no merge blocker;
no orphan in six configurations, races both ways, a self-referencing
cascade; `fk_reverse` reached only through the slot primitives):

- **A cast negative key routes.** `-7::bigint` and `CAST(-6 AS BIGINT)`, an
  ORM's `$n::bigint` with a negative id, were refused with 0A000 on a
  partitioned parent; the fallback read only a bare integer or a quoted
  string. It now encodes each value through `lexer_ir::encode_column_value`,
  extracted from `insert_row_with_defaults` so the two cannot drift (above).
  Pinned on four configurations by
  `partition_fk::every_literal_form_of_a_key_answers_like_the_plain_twin`
  (negative ints alone, cast, quoted and in `CAST`; `'x'::text`,
  `'x'::varchar`, `CAST('a' AS TEXT)` and a doubled quote on a TEXT key; a
  UUID in upper case, cast, in `CAST` and without hyphens; a NUMERIC child
  column on a BIGINT key; prepared negative ids), and by the reads test.
  Braced UUIDs are refused on both kinds alike (pre-existing, left).
- **`NOT VALID` and `VALIDATE CONSTRAINT`.** `ADD … FOREIGN KEY … NOT VALID`
  enforces new writes and leaves the rows already there unchecked;
  `VALIDATE CONSTRAINT n` checks them then (23503 for the first orphan),
  on both table kinds. No record says "not valid" (the sidecar has no spare
  byte), so validating a valid key checks again and passes; a CHECK's,
  UNIQUE's or primary key's name is a no-op, and since round 3 a name no
  constraint has is 42704, as in PostgreSQL (it was accepted). The
  import reads `convalidated` and adds a key its source never validated
  `NOT VALID`, with a note; a validated key whose source rows break it
  (rows loaded with `pg_restore --disable-triggers`) is retried `NOT VALID`
  with a note naming the table, so the import no longer ends at its last
  step (`migrate::run::add_foreign_key`; pinned by
  `load_tests::a_foreign_key_broken_by_the_source_rows_is_added_not_valid_with_a_note`
  and `plan::tests::a_source_key_never_validated_is_added_not_valid`).
- **Validation in bounded memory.** `existing_rows_break_foreign_key` walks
  each table's pointers as a scan does (a reclamation read ticket,
  `Visibility::visible`, one reused `Row` holding the column and its NULL
  companion), probes chunks of 512 distinct values and drops them, with a
  dedupe set capped at 64 Ki values. Measured on a 200,000-row child with
  200,000 distinct values (`fk_partition_tests::validation_peak_memory`,
  debug build, `VmHWM` reset before the ADD): **+133,044 kB** over the
  starting resident set before, **+18,608 kB** after; 3.2 s and 4.1 s.
- **The statement snapshot is pinned**: a parent committed between a fan-out
  DELETE's key reads, and a child of it, survive the DELETE
  (`fk_partition_tests::a_partitioned_delete_writes_the_rows_its_keys_were_read_from`).
- **`DROP TABLE … CASCADE` through a power cut**
  (`pddl_crash_tests::a_cascading_drop_cut_anywhere_leaves_no_key_naming_a_missing_table`,
  a plain parent with a partitioned child and the reverse, cut between the
  two steps, `test.rewrite.fkdrop_after_keys`, inside the child's reload and
  at the log's syncs): every image is nothing dropped, keys gone with the
  table there (the DROP then runs again), or both gone; all three occur in
  both cases, and none holds a key naming a missing table.
- **Child-side probes**: a child not partitioned by its key column (its
  leading key column) reads every partition whatever the chunk, so it is
  probed in chunks of 512, not 256, and its predicate is not tokenized for
  a route at all; a routed probe reads its route once
  (`RowSource::partitioned_routed`). A 10,000-key parent DELETE over a
  40,000-row child in 8 partitions: **320 partition scans before, 160
  after** (`fk_partition_tests::child_probe_chunk_timing`); wall-clock on
  this box (load 15 to 33) was 1.5 to 3.6 s before, 1.3 to 2.7 s after,
  noise either way.
- A column's own `CONSTRAINT n REFERENCES …` is named `n`.
- **A key between incompatible types is 42804**, PostgreSQL's "foreign key
  constraint … cannot be implemented: key columns … are of incompatible
  types", at CREATE TABLE and ADD CONSTRAINT, on both table kinds
  (`foreign_key::compatible_key_types`). A `TEXT` column referencing an `INT`
  key used to be accepted. Pinned by
  `partition_fk::a_key_between_incompatible_types_is_42804` on the four
  configurations; no existing test declared a cross-family key. Round 2's
  rule was stricter than PostgreSQL's; round 3 replaced it (below).
- The release note for databases written by older binaries is in
  `docs/postgres-compatibility.md` ("Release note: foreign keys from older
  binaries").

**Round 3** (2026-10-28), from the round-2 re-reviews (nothing blocking):

- **Every type pair PostgreSQL accepts is accepted** (round 2 refused `INT`
  onto `NUMERIC`, `INT` onto `DOUBLE`, `DATE` onto `TIMESTAMP`, which a
  PostgreSQL schema may hold). `compatible_key_types` follows
  `ATAddForeignKeyConstraint`: an equality in the referenced key's operator
  family (the integer widths, `REAL`/`DOUBLE`, `DATE`/`TIMESTAMP`, the text
  types, each any way round), or an implicit cast child to parent (an
  integer to `NUMERIC`, `REAL` or `DOUBLE`; a `NUMERIC` to `REAL` or
  `DOUBLE`); any other type only with itself. The reverse casts stay 42804,
  as in PostgreSQL (a `NUMERIC` or `DOUBLE` child of an integer key, a
  `DOUBLE` child of a `NUMERIC` key).
- **Each value is compared in the other side's type**
  (`coerce_key_literal`): a child's value is moved to the parent column's
  type before it is looked for (the insert and update checks, the
  multi-row batch, ADD's and VALIDATE's validation), and a parent's value
  to the child column's type before its children are (each DELETE and
  UPDATE check, CASCADE, SET NULL, SET DEFAULT, ON UPDATE). A move that
  changes the value finds nothing, as PostgreSQL's comparison in the wider
  type finds nothing: `INT` 7 finds `NUMERIC` 7.00 and no `INT` is 7.50; a
  `DATE` is its `TIMESTAMP` midnight and a 10:00 has no `DATE`; a `REAL`
  widens to `DOUBLE` exactly and a `DOUBLE` has a `REAL` only when it is one
  (0.5, not 0.1; through text both read `0.1`, the reviewer's permissive
  case). Without the parent side's move, deleting `TIMESTAMP` 10:00 was
  refused (or, under CASCADE, deleted the children) of the `DATE` child
  that references midnight. One divergence, at the edge: an `INT8`
  2⁵³+1 finds no `DOUBLE` parent here, where PostgreSQL rounds it to 2⁵³
  and finds that one; accepting it would let the parent 2⁵³ go while the
  child's own value matches nothing. Pinned by
  `partition_fk::cross_type_keys_compare_as_postgres_compares_them`
  (`INT`→`NUMERIC(10,2)` with 7 / 7.00 / 7.50 / 6.50 / -3 / -3.4,
  `BIGINT`→`DOUBLE` with 2⁵³ and 2⁵³+1, `DATE`→`TIMESTAMP` and
  `TIMESTAMP`→`DATE` at and off midnight, `DOUBLE`→`REAL` with 0.1 and 0.5,
  `SMALLINT`→`BIGINT` and `VARCHAR`→`TEXT` on partitioned parents too; each
  insert, each RESTRICT and CASCADE delete, no orphan after) and
  `…::adding_a_cross_type_key_validates_as_the_insert_checks`.
- `VALIDATE CONSTRAINT` of a name no constraint has is 42704 (above);
  compat `ddl.rs` expects it.
- **A foreign key is told by whole words.** The ALTER arm, the lock set's
  `fk_parent_of_clause`, the partitioned refusal, `lexer_ct`'s
  `foreign_key_clause` and `inline_constraint_name`, and
  `foreign_key::parse_clause` tested `contains("REFERENCES")`, so pg_dump's
  `ADD CONSTRAINT user_preferences_pkey PRIMARY KEY (id)` was read as a
  foreign key and failed to parse, a CREATE TABLE holding it likewise, and
  `FOREIGN KEY (references_id) REFERENCES …` found its `REFERENCES` in the
  column list. Each now uses `join::find_keyword` (whole words, outside
  quotes and parentheses), as fix/index-ddl does (aa2d345). Pinned by
  `partition_fk::a_name_holding_references_is_no_foreign_key` (a
  `user_preferences` table with a primary key, a UNIQUE, a CHECK named
  `references_check` and a key on `references_id`, plain and partitioned).

**Behaviour changes for plain tables** (the same code serves both kinds):

- `ADD CONSTRAINT … FOREIGN KEY` validates the rows already there (23503,
  nothing written), as PostgreSQL does; it used to store the key
  unvalidated, so a table could hold a constraint its rows broke. `NOT
  VALID` (round 2) skips that, and `VALIDATE CONSTRAINT`, an accepted no-op
  before, now checks a foreign key's rows. It needs
  the parent to exist (42P01) and the column on both sides (42703), and a
  name another constraint of the table has is 42710.
- `CREATE TABLE` refuses a key whose referencing column, or whose existing
  parent's referenced column, is not there (42703).
- A key between columns of incompatible types is refused with 42804, at
  CREATE and ADD (round 2).
- `DROP TABLE` of a referenced table is refused (2BP01) unless `CASCADE`,
  which drops the dependent keys.
- An unnamed key is `<table>_<column>_fkey` (it was `fk_<column>_<parent>`;
  stored names do not change). Names are stored as given (above).
- `DROP COLUMN` of a referenced column is 2BP01 (was XX000; the refusal
  itself is older).
- Foreign keys are found through `fk_reverse`, in child-name order, so a
  parent's children are checked and acted on in that order (they were in
  hash-map order).

**Divergences from PostgreSQL kept as they were** (noted, not copied into
anything new): a **plain** parent's referenced column need not be unique
(PostgreSQL: 42830; refusing it would break schemas the plain path has
always accepted, and two parents sharing a value make a delete of one act
on children the other still covers); `CREATE TABLE` may name a parent that
does not exist yet (PostgreSQL: 42P01; each insert's check names it, and a
partitioned table later created under the name is held to the keys naming
it); identifiers are not case-folded, so `REFERENCES P` is not `p`; keys are
one column; checks run per statement, never deferred; `DROP TABLE …
CASCADE` drops the keys and then the table, two steps, so a crash between
them leaves the table with the keys already gone (DDL is not transactional
here).

**Found, not fixed:**

- An `ON UPDATE CASCADE`, `SET NULL` or `SET DEFAULT` that writes a
  partitioned child's partition key column is a key change: refused with
  0A000 until phase 6 moves rows. PostgreSQL moves the row.
- Keys are one column on every table, so a table partitioned by two or
  three columns can be referenced by no key at all; composite keys would
  lift that.
- ~~By reading: a column-level `CONSTRAINT n REFERENCES p (k)` gave its key
  no name of its own~~ (fixed in round 2, `lexer_ct::inline_constraint_name`).
- Seen while pinning round 3, not investigated (CHECK code P4 does not
  touch): `CHECK (x IS NULL OR x > 0)` refuses a row whose `x` is NULL
  (23514), on a plain table too; PostgreSQL accepts it.
- The insert-time check finds its parent by exact name (`has_table`), as
  every lookup here does (identifiers are not case-folded, an open finding):
  a key on `P` never matches `p`.

## Phase 3 P3 as built

Built on `feat/p3-identifying-keys` (2026-10-27), over a9854ec: **composite
identifying keys**, so every row-addressing statement works on a table whose
only unique keys span several columns, which is every partitioned table and
a plain table with a composite `PRIMARY KEY` (§1b).

**What works, on plain and partitioned tables alike:**

- `SELECT … FOR UPDATE`, `FOR NO KEY UPDATE`, `FOR SHARE`, `FOR KEY SHARE`,
  with `SKIP LOCKED` or `NOWAIT` (55P03), routed or fanned out, with `ORDER BY
  … LIMIT`, in blocks and prepared. The engine maps the four strengths onto
  two, as it did on a one-column key (`NO KEY UPDATE` is `UPDATE`, `KEY
  SHARE` is `SHARE`). The select list need not name the key: the scan
  fetches the key columns (and a nullable one's NULL companion) for the lock
  and never returns them, so `SELECT email … FOR UPDATE` on a table keyed by
  `id` locks by `id`. A table has one key, whatever a statement selects, so
  every locker and writer of a row names it alike.
- A locking read that took a lock it did not already hold, after a
  transaction its scan could not see has committed (the holder it waited
  for, or one that finished between the scan and the lock, as `SKIP LOCKED`
  meets without waiting), **scans again at a fresh snapshot**, at most four
  times and then 40001: a row gone or no longer matching drops out, a
  changed one comes back as it is now (PostgreSQL re-reads a locked row's
  latest version). A claim query no longer hands out a job its holder has
  finished, so a queue may write its claim back by key alone. Under
  `REPEATABLE READ` or `SERIALIZABLE` the snapshot is pinned and a scan
  again would see the same versions, so instead a row newly locked whose
  latest committed version is not the one the snapshot saw (changed or
  deleted) fails the statement with 40001 ("could not serialize access due
  to concurrent update"), as PostgreSQL's does. The row's version is
  compared (read by key at the pinned and at a fresh snapshot, by
  position), so a commit elsewhere in the table is no conflict.
- A row lock on rows that are not one table row each is refused with
  0A000 and PostgreSQL's message ("FOR UPDATE is not allowed with DISTINCT
  clause"; likewise `GROUP BY clause`, `HAVING clause`, `aggregate
  functions`, `window functions`, `UNION/INTERSECT/EXCEPT`, naming the
  clause as written: `FOR NO KEY UPDATE`, `FOR SHARE`, `FOR KEY SHARE`).
  `SELECT DISTINCT state … FOR UPDATE` used to lock one row per value,
  `SELECT COUNT(*) … FOR UPDATE` nothing, and a lock on a union's last
  branch that branch's rows alone.
- A row is locked by the framed tuple of its key under the table's
  **logical name**: one `RowLocks` entry whichever partition the row is in
  and whichever route found it, so a lock taken through a routed read blocks
  a fanned-out writer and the reverse (§6.2's last paragraph).
- A writer (`UPDATE`, `DELETE`, the per-row write of `UPDATE … FROM` /
  `DELETE … USING`) of a row another transaction holds **fails at once**
  ("row … is locked by transaction N"), unlike PostgreSQL's, which waits and
  re-evaluates. A writer may only wait if it re-reads the row and recomputes
  what it writes afterwards, and `UPDATE … FROM` does not: it computes its
  `SET` values from the join first, so the first P3 build, whose writers
  waited, stored `v + w` computed before the holder's commit over that
  commit (100 where 105 was right). The per-row write-back never retries by
  key alone after a lock, deadlock, timeout or cancellation error, which
  would drop its recheck. A locking read still waits (bounded, an edge of
  the wait-for graph table locks share, keyed as the lock is), so a cycle
  through a row lock and a table lock ends in exactly one 40P01.
- `UPDATE … FROM` and `DELETE … USING` with a partitioned **target** (plain
  or partitioned source, derived tables, `RETURNING`, expressions over the
  target): the join reads every relation and partition under its one
  snapshot (S19), then each target row is written back by its key, which
  pins every partition-key column and so routes to one partition, with the
  join's target terms rechecked as before (2c10025). Every row is named
  before any is written.
- **Each target row is written once**, however many source rows it joins,
  as PostgreSQL does. `UPDATE … FROM` used to write once per joined row:
  `SET n = n + 1` applied twice, and the count and `RETURNING` held the row
  twice (plain tables too; `DELETE … USING` already deduplicated).

**The identifying key** (`identifying_key`, `core/rql/executor.rs`), chosen
as PostgreSQL names a row:

1. the primary key: `schema.primary_key` (one column), or the table-level key
   named `<table>_pkey` (a composite one, which `lexer_ct` records only as a
   unique key);
2. else a `UNIQUE` (inline or table-level, not partial) whose every column is
   `NOT NULL`, the fewest columns first, then the first declared;
3. else, as before composite keys counted, a one-column `UNIQUE` that may hold
   NULL.

A row whose key holds a NULL names no row (a unique key admits any number of
NULLs): `FOR UPDATE` and the write-back refuse it with an error rather than
name every NULL row by the bytes a NULL travels as, which used to write a
NULL-keyed row back by `= NULL` and silently skip it. A one-column key locks by
the column's stored bytes; a composite one by each component's
`encode_canonical`, which is length-prefixed, so `(1, 23)` and `(12, 3)`
cannot meet. A decimal is keyed by its value (unscaled with trailing zeros
stripped, and its scale), and **no key cell is ever rescaled**: the locker
reads the select pipeline's cells, the writer check and both join paths
read with `run_subquery_scaled(normalize = false)`, all at the column's
declared scale, and the write-back renders a key, and every value the join
carries, at its stored scale with every place kept (`joined_literal`). The
first build keyed `NUMERIC(10,2)`'s 1.50 as 150 on one side and the
subquery's normalised 1500000000 on the other, so a writer never met the
lock; normalising also rounded a `NUMERIC(20,12)` key to nine places, so
1.000000000001 and 1.000000000002 deduplicated into one row written back by
a value matching neither (`UPDATE 0` where 2 were due), and a value past
about 1.7·10²⁹ overflowed the rescale and was read as nine places.
A primary key declared with a `CONSTRAINT` name of its own is not
recognised as the primary key: the schema records a composite key only as a
`UniqueKey` (name, columns, predicate), with no constraint kind, and only a
one-column primary key in `schema.primary_key`. It is still chosen by rule 2
when its columns are `NOT NULL`.

**Pinned by** `tests/partition_twin.rs` (`ForUpdate`, `SkipLocked`,
`ForUpdateNoWait`, `UpdateFrom`, `DeleteUsing` compared with the twin, in
blocks too; `REFUSED` holds only phase 6 kinds and `KNOWN_DIVERGENCES` stays
empty; green in all four configurations at the default seeds, 300
statements, and at 0x5eed2b20 and 0x5eedf1a1 with 1,500 statements each),
`tests/partition_shell_sites.rs`, `tests/partition_write.rs` and
`row_source_join_tests.rs` (a partitioned target written like its twin),
and `rasterizeddb_compat/tests/composite_row_locks.rs` over pgwire on a plain
composite-key table and a partitioned one: a `SKIP LOCKED` job queue whose
claim writes back by key alone claiming every job exactly once, `NOWAIT`
55P03, a holder refusing a writer until `COMMIT`, one 40P01 across a row and
a table lock, `UPDATE … FROM` under one snapshot with a concurrent writer,
`DELETE … USING` returning each row once under a concurrent inserter,
write-once, the key choice, the two lost-update shapes of the first build
(a writer's join values over a holder's commit; a write-back without its
recheck after a lock timeout), `NUMERIC(10,2)`, `TEXT` and `UUID` keys, a
locking read without the key in its select list, and a waited read
returning what the holder left. Each invariant was mutation-verified.

**Found, not fixed** (engine behaviour outside P3, on plain tables too):

- A failed autocommit `UPDATE … FROM` keeps its earlier per-row writes: with
  a `CHECK` violated by the third joined row, the first two rows stay
  updated and the client gets 23514.
- A column of a table-level `PRIMARY KEY (a, b)` written without `NOT NULL`
  accepts NULL (PostgreSQL makes it `NOT NULL`).
- `amt::text` on a `NUMERIC(20,12)` column prints 1.000000000000 for both
  1.000000000001 and 1.000000000002 (reported by the P3 review; not
  investigated).
- A predicate literal past about 29 integer digits is "numeric literal … is
  out of range" (the evaluator holds decimals at nine places), so no
  `WHERE` can name a 35-digit `NUMERIC(38,0)` key; and a 38-digit value is
  refused by `NUMERIC(38,0)` itself with 22003 ("does not fit the column's
  precision").
- Left for the correctness slice: `refuse_write_to_locked_rows` returns
  early while no row lock exists anywhere, a window between that check and
  a `FOR UPDATE` taken after it; locks from a locking read's earlier round
  on rows a later round drops are kept until the transaction ends;
  `committed_since` is conservative past 4,096 ids (it rescans).

## Phase 3 P1 as built

Built on `feat/p3-ddl` (from `a9854ec`, 2026-10-27), wave 1 of the phase 3
plan: metadata-only schema changes on a partitioned table, answering like a
plain twin (I6). Not advertised.

**What runs on a partitioned table**, each as on a plain table:

- `CREATE [UNIQUE] INDEX` (btree, hash, gin, a path index; named or not;
  `IF NOT EXISTS`, read wherever the rewrite puts it:
  `unique_key::strip_if_not_exists`, which matches keywords byte-safely, so
  a constraint name starting with a multi-byte character is a name; the
  name itself read bare or quoted, spaces, `""` and non-ASCII included, by
  `unique_key::split_constraint_name`) and `DROP INDEX [IF EXISTS]`, the partial unique index's
  drop included. A UNIQUE index, like `ADD CONSTRAINT … UNIQUE`/`PRIMARY KEY`,
  must contain every partition key column (§1b): otherwise 0A000 with
  `CREATE … PARTITION BY`'s wording ("does not contain partition key column
  …, as in UNIQUE (…)"), from one function (`unique_key_refusal`).
- `ALTER TABLE … RENAME TO`, `RENAME COLUMN` (a key column too: the map
  header keys by write order, so the hash never sees names), `ALTER COLUMN
  SET`/`DROP DEFAULT` (a key column's too, §10), `ADD`/`DROP CONSTRAINT`
  (CHECK, validated over every partition; UNIQUE/PRIMARY KEY, validated per
  partition, exact by §1b), `SET NOT NULL` (validated), `DROP COLUMN`
  (marking; never a key column), and the verbs a plain table takes as
  no-ops.
- `ADD COLUMN` in every form the plain path writes as metadata: when no
  open partition's rows file holds a byte (the plain arm's `row_count == 0`,
  summed over the partitions), with a `DEFAULT`, `NOT NULL`, a NUMERIC
  modifier or `GENERATED`. `DROP NOT NULL` of a non-key column on such a table
  adds its `__null_` companion as metadata (the plain path rewrites zero rows
  there; see deviations).

**Still refused**, 0A000 with "not supported" and the reason, before any
lock and before anything is written (`Database::partitioned_alter_refusal`;
the executor's `refused_on_a_partitioned_table` skips the statement's lock
set for these, as 2b did for every ALTER):

| Refused on a partitioned table | Reason | Lifted by |
|---|---|---|
| `ADD COLUMN` on a table whose partitions hold rows; `ALTER COLUMN … TYPE` to another type; `DROP NOT NULL` of a NOT NULL column on such a table | the plain path rewrites every row (`rewrite_table`), which per partition is not all or nothing (§11) | phase 6 (generational physical ALTER) |
| `DROP COLUMN`, `ALTER COLUMN … TYPE` or `DROP NOT NULL` of a partition key column | §1.5: the key decides where every row lives and must be NOT NULL | never |
| A UNIQUE or PRIMARY KEY (constraint or index, partial or not) without every key column | §1b | never (an opt-in is §17.6) |
| ~~`ADD CONSTRAINT … FOREIGN KEY` (from), and `REFERENCES` a partitioned table (onto)~~ | §10 | built in P4 ([as built](#phase-3-p4-as-built)) |
| `ADD COLUMN … JSON` (shredded) | its shadow table would be unpartitioned | not scheduled |

`rewrite_table` itself refuses a partitioned name too, as a backstop.

**How it runs.**

- *Lock.* The DDL takes ACCESS EXCLUSIVE on the **logical** name through the
  one table-lock manager (persistent-indexes §7), exactly as a plain
  table's: at the statement's entry (`lock_statement_set`) and again in the
  arm (`table_for`). Every statement on the table already locks the logical
  name before it reaches a partition (`relation_for`), so the DDL queues
  behind open blocks that read or wrote it, a statement arriving after it
  queues behind it (PostgreSQL's rule, the "Superseded" paragraph of §11
  taken as written: queueing, not try-acquire), `lock_timeout` gives 55P03,
  `statement_timeout` and cancellation 57014, a real cycle 40P01; each with
  nothing written. No statement can reach a partition while its schema
  changes: the only lockless readers (Describe, catalogs) read the shell's
  schema, never a partition.
- *Reload* (`Database::reload_partitioned`, `core/database/reload.rs`; the
  plain `reload_with_instance` dispatches to it): (2) every open partition is
  retired (maintenance joined, seals cancelled); (3) the DDL's own work,
  the plain arm's closure unchanged, writes the shell's sidecars once,
  through the shell; the schema is read back from them under the
  catalogue's name (`load_shell`), and `PartitionedTable::reopened` builds
  the table again: the same uid, map log and map state (shared, one writer
  under the lock), the same epoch, the row-id source reopened from its
  durable reservation as a start does, and every open partition opened again
  over its files with the new schema clone, its indexes built from its rows;
  (4) the new table goes into the partitioned map and its shell into the
  name's slot (`swap_partitioned`). An unmaterialized partition takes the
  new schema when it materializes. A failing work still reopens from disk; a
  reopen that fails installs the table broken with its reason, as a start
  would, and a DROP still reaches it.
- *One schema.* A partition has no schema file: every partition's
  `TableSchema` is a clone of the shell's, read from the shell's sidecars.
  So no two partitions can hold different schemas, in memory (one reload
  swaps them all under one lock) or after a crash (they all read one file
  set). `schema_generation` moves by one reload's two bumps per DDL,
  however many partitions; the shape epoch moves when the result columns do
  (`same_result_shape` on the shell, as a plain table's slot), so prepared
  statements re-describe (`rdb.shape_change`), and an index-only change
  keeps it, as on a plain table.
- *The shell's sidecars, all or nothing.* The atomic arms (`reload_atomic`:
  ADD/DROP/RENAME COLUMN, SET NOT NULL, a NUMERIC modifier, the metadata
  DROP NOT NULL) hold the schema files aside under the shell's stem
  `__pt_<uid>` (`schema_files_stem`), with the rewrite intent, exactly as a
  plain table's: a crash restores them at the next open before anything
  opens. The single-sidecar arms (`reload_with`: an index declaration, a
  CHECK, a UNIQUE key, a default) append one logged record.
- *Index snapshots* (`index_storage = snapshot`). A partition's snapshot
  identity carries the shape hash of its index declarations, read from the
  shell's schema at open, so a snapshot sealed before a CREATE or DROP INDEX
  (or a dropped column's index) fails its proof at every later open and the
  indexes are rebuilt; sealed again under the new shape, it loads. No write
  ticket is taken on the partitions' gates: the shell's files attach to no
  word (`gated_io`), and the identity is what invalidates.
- *RENAME TO* (§3.1, `rename_partitioned`): no file is copied. (1) An intent
  of its own, `__prename.intent` (uid and both names, checksummed, renamed
  into place whole); (2) a second catalogue row, the new name with the same
  uid: the commit point; (3) the shell's header renamed, under a schema
  backup, best effort; (4) the old row tombstoned and made durable, then the
  intent removed. Startup (`partitioned_ddl::recover_interrupted_rename`,
  before the catalogue is read) settles it from the catalogue alone: both
  rows with the uid, tombstone the old; otherwise drop the intent. It deletes
  no file. While an intent is left (step 4 failed), the process refuses a
  second partitioned RENAME, a DROP of that uid, and a CREATE of either name
  (`unfinished_partitioned_rename`, `hold_intent`) until the next open settles
  it. The header step retires the shell before its backup (a shell runs no
  maintenance; the partitions are retired already), so a failed header write
  is put back under `backup_restore`'s precondition. An intent that does
  not parse (renamed into place whole, so not one this engine wrote) is
  moved aside at the next open to `__prename.intent.bad-<n>`, logged at
  error level; until then RENAME and DROP name the file and the manual
  step. The catalogue row names the table: a header recording another
  name is read under the row's (`load_shell`). Two rows still sharing a uid
  after recovery are refused at startup, as before.
- *Replication.* Each DDL ships as its logical statement (§12). The refusals
  read the statement, the replicated schema, and the rows-file lengths the
  plain table's own ALTER already branches on (metadata vs rewrite).

**Crash safety, step by step** (`core/power_loss/pddl_crash_tests.rs`,
cut images under every seed kind; every image must open, with every
partition, and read as the old table or the new one, the same in every
partition):

| Step | A crash there leaves | Cut point |
|---|---|---|
| lock taken, partitions retired | nothing written: the old table | `test.pddl.before_work` |
| shell sidecars being written (atomic arm) | the intent restores the old schema files at open: the old table | `test.rewrite.metadata_after_work`, `sync:rewrite.intent`, `sync:file.data` |
| shell sidecars written | the new schema files, which every partition reads: the new table | `test.pddl.after_work` |
| partition k reopened (in memory only) | the new table: nothing on disk is per partition | `test.pddl.partition_reopened` #1, #2 |
| index build (in memory, spawned) | the new table; a snapshot sealed before fails its identity and rebuilds | `partitioned_ddl_tests::a_partition_snapshot_from_before_a_schema_change_is_never_loaded_after_it`, `…::a_swapped_index_never_loads_the_old_index_sections` |
| RENAME intent written | recovery drops the intent: the old name | `test.pddl.rename_after_intent` |
| RENAME new row written | recovery tombstones the old row: the new name | `test.pddl.rename_after_commit`, `test.pddl.rename_after_header` |

The kinds cut: CREATE INDEX (btree and hash), DROP INDEX, ADD and DROP
CHECK, ADD UNIQUE, SET and DROP DEFAULT, RENAME COLUMN (a value column and
the key), DROP COLUMN, SET NOT NULL, RENAME TO, and on a table with no row
ADD COLUMN with a DEFAULT and the metadata DROP NOT NULL, and an index
swapped for another with the count kept (`DROP INDEX; CREATE INDEX`, the
state between the two accepted). Each image's indexes are compared by
contents too: an equality an index serves answers as the same predicate it
cannot serve. Snapshots are sealed before each change, and every image is
opened twice: rebuilding its indexes from rows, and with `index_storage =
snapshot`, loading the sealed INDEX sections wherever the open's proof
accepts them, so a stale persisted section would answer from stale
entries (the test asserts that images did load persisted indexes). A first run of that test found the plain rename intent unsafe for this
(below), which is why RENAME has its own.

**Tests.** `tests/partition_twin.rs` gains twenty-one DDL kinds (indexes,
unique keys, CHECKs, defaults, NOT NULL, three RENAME round trips run as
sequences with reads and writes under the change, a marking DROP COLUMN,
a prepared SELECT across a RENAME COLUMN, and the refused kinds), with a new
nullable column `junk` to drop; `KNOWN_DIVERGENCES` stays empty, and
`REFUSED` gains only the permanent and phase 6/P4 kinds above.
`tests/partition_alter.rs`: every kind against a plain twin over pgwire
through restarts, the empty-table ADD COLUMN forms, prepared statements
across each DDL (values compared), the DDL queueing behind a reader and a
writer (55P03, 57014, a reader arriving behind it), four writers through
thirty DDLs (every acknowledged row stored once, after a restart too).
`core/database/partitioned_ddl_tests.rs`: one schema in every partition,
retirement, the generation and shape epoch; validation reading every
partition; the uniqueness rule; snapshots. `pgwire::ddl_lock_tests`,
`tests/partition_shell_sites.rs` (DDL sites in pairs, the shell's file and
memory agreeing and every partition holding the shell's schema after each),
`tests/partition_ddl.rs`.

**Mutations** (each made alone, its test failed by assertion, the file
restored byte-exact from git; `grep -rn MUTATED` clean at the end):

| Mutation | Where | Test that fails |
|---|---|---|
| the reopen keeps the first open partition's old instance | `PartitionedTable::reopened` | `partitioned_ddl_tests::a_schema_change_reopens_every_open_partition_under_one_schema` |
| the shell swapped in without the shape check (shape epoch not bumped) | `Database::swap_partitioned` | the same |
| statements lock a partition's name, DDL the logical one | `Database::lock_table` | `pgwire::ddl_lock_tests::partitioned_ddl_waits_for_readers_and_its_refusals_take_no_lock` |
| the partitions reopen before the shell's sidecars are written | `Database::reload_partitioned` | `…::a_schema_change_reopens_every_open_partition_under_one_schema` |
| the snapshot identity unchanged by an index change (index list out of the shape hash) | `TableIndexes::shape_hash` | `partitioned_ddl_tests::a_swapped_index_never_loads_the_old_index_sections` (it survived the first snapshot test, whose CREATE INDEX changes the index count, which the load plan refuses on its own; the swap keeps the count) |
| a fan-out skips one partition (the CHECK and NOT NULL probes) | `FanOut::planned` | `partitioned_ddl_tests::validation_reads_every_partition` |
| a UNIQUE without a key column accepted | `partitioned_alter_refusal` | `partitioned_ddl_tests::a_unique_key_without_every_key_column_is_refused` |
| *(rework)* `IF NOT EXISTS` after `CONSTRAINT` left in place | `unique_key::strip_if_not_exists` | `partitioned_ddl_tests::if_not_exists_unique_forms_are_refused_or_enforced`; `tests/partition_shell_sites.rs` |
| *(rework)* an unread UNIQUE accepted as unenforced | the ADD CONSTRAINT arm's fall-through | `…::if_not_exists_unique_forms_are_refused_or_enforced` |
| *(rework)* no unfinished rename seen, names not held | `unfinished_partitioned_rename`, `rename_partitioned` | `…::an_unfinished_rename_blocks_a_second_rename_and_a_drop` (the second rename succeeds) |
| *(rework)* a DROP ignores an unfinished rename | `drop_partitioned` | the same (the DROP succeeds) |
| *(rework)* a failed header write not put back | `rewrite_partitioned_header` | `…::a_failed_header_rewrite_puts_the_schema_files_back` |
| *(rework)* recovery skips the schema intents | `recover_interrupted_rewrites` | `pddl_crash_tests::a_partitioned_ddl_cut_anywhere_leaves_the_old_table_or_the_new` |
| *(rework)* an index build skips rows | `build_indexes` | the same (the index-contents check) |
| *(round 2)* the keyword match slicing statement text by bytes | `unique_key::starts_with_words` | `strip_if_not_exists_tests`, `partitioned_ddl_tests::non_ascii_and_quoted_constraint_names_on_both_table_kinds` (a char-boundary panic) |
| *(round 2)* the unread-UNIQUE scan reading the name too | the ADD CONSTRAINT arm | `…::non_ascii_and_quoted_constraint_names_…` (`"unique" EXCLUDE` refused) |
| *(round 2)* the constraint name read up to whitespace | `parse_clause` | the same (`"my key" UNIQUE` refused) |
| *(round 2)* a malformed rename intent left in place at open | `recover_interrupted_rename` | `…::a_malformed_rename_intent_is_set_aside_at_open` |
| *(round 2)* the snapshot identity unchanged by an index swap | `TableIndexes::shape_hash` | `pddl_crash_tests` (the swap case under `index_storage = snapshot`: `s = 'w0'` answers nothing, its scan the rows) |
| *(round 3)* the orphan check off in ALTER | `execute_alter_table_action` | `tests/partition_alter.rs::a_ddl_that_would_orphan_the_blocks_row_locks_is_refused` (RENAME TO succeeds under the block's lock) |
| *(round 3)* the orphan check off in DROP TABLE | `execute_drop_table` | the same (DROP TABLE succeeds) |
| *(round 3)* the identifying key never compared | `orphans_row_locks` | the same (DROP CONSTRAINT `_pkey` succeeds) |
| *(round 3)* every key-touching ALTER refused | `orphans_row_locks` | the same (ADD UNIQUE that does not win refused) |
| *(round 3)* 55006 not mapped | `pgwire::sqlstate_for` | the same (XX000) |
| *(round 3)* any session's row locks counted | `RowLocks::txn_holds_rows_on` | **survives, equivalent:** the check runs after ACCESS EXCLUSIVE is granted, which another transaction's row locks (and the table lock they come with) prevent, so only the issuer's locks can be there. The wait itself is pinned (B's RENAME: 55P03) |
| *(round 3)* a UNIQUE name ASCII-filtered again | `unique_key::write_name` | `partitioned_ddl_tests::constraint_names_are_stored_as_given_and_dropped_by_them`, `unique_key::stored_name_tests` |
| *(round 3)* a CHECK name ASCII-filtered again | `CheckConstraint::into_vec` | `…::constraint_names_are_stored_as_given_and_dropped_by_them` (`価格_chk` stored as `_chk`) |
| *(round 3)* the DROP name read by whitespace | `unique_key::dropped_constraint` | the same, `stored_name_tests` |
| *(round 3)* a DROP matching nothing reported as done | the DROP CONSTRAINT arm | the same (a second drop succeeds); `tests/partition_alter.rs::dropping_a_constraint_that_is_not_there_is_42704` |
| *(round 3)* 42704 not mapped for "of relation" | `pgwire::sqlstate_for` | `…::dropping_a_constraint_that_is_not_there_is_42704` (42P01) |
| *(round 3)* the CHECK clause read across a quoted name | the ADD CONSTRAINT arm | `…::constraint_names_are_stored_as_given_and_dropped_by_them` (`"my check"` not stored) |
| *(round 3)* the CHECK name read by whitespace | `add_check_constraint` | the same (stored as `my`) |
| *(round 3)* a UNIQUE INDEX name not re-quoted | `lexer_alter::unique_index_as_constraint` | `lexer_alter` tests, the same |
| *(round 3)* the import's primary key named `<table>_<cols>_key` again | `migrate::plan` | `migrate::run::load_tests` (42704 on the drop; this is the state the fix found) |
| *(round 4)* a one-column ADD PRIMARY KEY not put in the header | `add_primary_key` | `tests/import_primary_key.rs` (`users` reads unlike its twin) |
| *(round 4)* the key's columns not made NOT NULL | `add_primary_key` | `partition_alter::add_primary_key_makes_the_tables_primary_key` (a NULL `b` inserted) |
| *(round 4)* the NULL probe ignored | `add_primary_key` | the same (ADD over a NULL succeeds) |
| *(round 4)* a second primary key accepted | the ADD CONSTRAINT arm | `import_primary_key` (42710 on the name) |
| *(round 4)* DROP leaves the header | the DROP CONSTRAINT arm | `import_primary_key` (`indisprimary` during the load) |
| *(round 4)* a composite primary key not seen | `has_primary_key` | `import_primary_key` (a second PK on `pairs` accepted) |
| *(round 4)* 42P16 / 23502 not mapped | `pgwire::sqlstate_for` | `import_primary_key`, `add_primary_key_…` (XX000) |
| *(round 4)* names not cut where read | `unique_key::stored_name` | `partition_alter::long_and_unnamed_constraint_names` (the 131-byte DROP: 42704), `stored_name_tests` |
| *(round 4)* a one-column CHECK named `<table>_check` | `check::unnamed_name` | `long_and_unnamed_constraint_names` |
| *(round 4)* a CHECK name collision not numbered | `check::unnamed_name` | the same (the second `v` CHECK: already exists) |
| *(round 4)* string literals read as columns | `check::columns_read` | `check::tests::an_added_unnamed_check_is_named_as_postgres_names_it` |
| *(round 4)* an unnamed UNIQUE's collision not numbered | the ADD CONSTRAINT arm | `long_and_unnamed_constraint_names` (42710) |
| *(round 5)* the original spin: the label cut after joining, the search unbounded | `unique_key::name_with_label`, `choose_name` | `partition_alter::long_names_number_repeated_unnamed_constraints_and_return` (no return within its 120 s bound) |
| *(round 5)* the label cut after joining, the search bounded | `unique_key::name_with_label` | the same (the bounded error: "could not choose a name"), `a_numbered_name_keeps_its_label_and_number` |
| *(round 5)* a derived `<table>_pkey` cut after joining | `unique_key::parse_clause` | `a_numbered_name_keeps_its_label_and_number` |
| *(round 5)* DROP `<table>_pkey` matches a key added as `k1` | the DROP CONSTRAINT arm | `partition_alter::a_primary_key_answers_to_its_own_name_only` |
| *(round 5)* an inline key matched as `<table>_<col>_key` | the DROP CONSTRAINT arm | the same |
| *(round 5)* a named composite primary key accepted | the ADD CONSTRAINT arm | the same (0A000 expected) |
| *(round 5)* a one-column key always taken as `<table>_pkey` | `primary_key_constraint` | the same |
| *(round 6)* `identifying_key` compares the uncut `<table>_pkey` | `identifying_key` | `partition_alter::a_long_table_names_its_keys_as_it_compares_them` (55006 dropping the narrower UNIQUE) |
| *(round 6)* `primary_key_constraint` compares the uncut name | `primary_key_constraint` | the same (42710 for 42P16) |
| *(round 6)* the named-composite refusal compares the uncut name | the ADD CONSTRAINT arm | the same (0A000 on an unnamed key) |
| *(round 6)* ON CONFLICT ON CONSTRAINT compares the uncut name | `constraint_columns` | the same (DO UPDATE on the inline key; it survived DO NOTHING, which never reads the columns) |
| *(round 6)* the DROP INDEX refusal compares the uncut name | `constraint_index` | the same (on the inline key; it survived on a sidecar key, found by name) |
| *(round 6)* `implicit_unique_column` without the cut name | `implicit_unique_column` | the same (the inline UNIQUE's DROP: 42704) |
| *(round 6)* the implicit index named uncut | `index::for_schema` | the same (`pg_indexes`) |
| *(round 6)* `parse_clause` names the key uncut | `unique_key::parse_clause` | the same |
| *(round 6)* the import names the key uncut | `migrate::plan` | `plan::tests::a_long_tables_primary_key_is_named_as_the_engine_names_it` |
| *(round 6)* a derived CHECK / index name cut after joining | `check::derived_name`, `CreateIndex::name_or_default` | `check::tests`, `index::ddl` tests |
| *(round 3)* one case's snapshot pass run in rebuild mode (test-side: a production mutation that drops every load of one case alone was not found, since a case's images from before the DDL load whatever the DDL does) | `pddl_crash_tests::run_case` | `pddl_crash_tests` (RENAME TO: `loaded: 0`; the summed check alone passes) |

**Twin runs:** all four configurations green at the default seeds
(0x5eed0001, 0x5eed7a11, 300 statements) and at 0x5eed2b20 and 0x5eedf1a1
with 1,500 statements each, after rounds 2 to 6.

**Round 3** (2026-10-27), three items from review:

- **A DDL that would orphan the block's own row locks is refused** with
  55006 (object_in_use: "cannot … because it is being used by active
  queries in this session: the transaction holds row locks on the table,
  which this would orphan"). A row lock is keyed by (table name,
  identifying-key bytes), so a `RENAME TO` in the same block left the lock
  under a name no row has, and a change to the key that identifies a row
  made every later lookup frame a different key: a second session could
  update the "locked" row before the block ended. Refused, when the issuing
  transaction holds row locks on the table (`RowLocks::txn_holds_rows_on`):
  `RENAME TO`, `DROP TABLE`, and any ALTER whose predicted schema
  (`schema_after_for_identity`: ADD/DROP of a UNIQUE or PRIMARY KEY,
  SET/DROP NOT NULL, DROP COLUMN, built in memory) makes `identifying_key`
  answer differently, compared as columns and scales
  (`identifying_shape`), not by statement kind. ADD CHECK, a UNIQUE that
  does not become the identifying key, and CREATE INDEX keep the locks and
  go through. Another session's locks are not this check's: its DDL waits
  for ROW EXCLUSIVE and times out with 55P03, as in PostgreSQL. Plain and
  partitioned alike; checked under the DDL's lock, before a sidecar is
  written.
- **Constraint names are kept as written.** UNIQUE and CHECK names are
  stored as UTF-8, cut at a char boundary within the 128-byte slot.
  **No format change:** the readers (`read_name`, `CheckConstraint::from_vec`)
  already decoded UTF-8, so a binary from before reads the new names, and a
  record written filtered reads back as it was written (a DROP by the
  original, unfiltered name of such a record now gets 42704 rather than a
  silent no-op; dropping it by the stored name works). `DROP CONSTRAINT [IF
  EXISTS]` reads its name with `split_constraint_name`
  (`unique_key::dropped_constraint`), and a name matching neither sidecar is
  42704 ("constraint "x" of relation "t" does not exist") unless `IF
  EXISTS`, on both table kinds; it used to report success. Two of the same
  shape, found by the pins: `ADD CONSTRAINT "my check" CHECK (…)` was not
  stored (`check::split_clause` found CHECK inside the quoted name; the
  clause is now read after the name), and `CREATE UNIQUE INDEX "mix é 1"`
  lost its quotes in the rewrite to ADD CONSTRAINT (`lexer_alter`
  re-quotes a name that is not bare). The studio sources on this branch
  issue no `DROP CONSTRAINT`; `fc3849c`'s pins (`constraint_drops.rs`) and
  every compat test that drops a constraint drop a name that exists or use
  `IF EXISTS`. An FK's name is still stored filtered (findings), so `DROP
  CONSTRAINT "my fk"` is now 42704 where it was a silent no-op that kept the
  key.
- **The crash test's snapshot pass loads an image in every case** that has
  rows (`loaded > 0` per case, not only summed), so one case whose images
  all rebuild cannot hide behind the others.

The 42704 found one caller that relied on the silent success: the import
(`migrate::plan`) dropped and re-added the primary key as
`<table>_<cols>_key`, a name no primary key has, so its DROP matched
nothing and every bulk load ran with the key in place. It uses
`<table>_pkey` now, the engine's and PostgreSQL's name
(`load_tests` failed with 42704 before the change).

The twin's `DropUniqueIndex` now fails 42704 on both sides when its name is
gone, and gains a round trip (add, drop, drop again) so every run has a drop
that took effect.

**Round 4** (2026-10-27), three items from the re-review:

- **`ADD [CONSTRAINT n] PRIMARY KEY (…)` makes the table's primary key**, as
  in PostgreSQL (`add_primary_key`). Its columns become NOT NULL, and a
  NULL in one refuses it with 23502 ("column "c" of relation "t" contains
  null values"). A second primary key is 42P16 ("multiple primary keys for
  table "t" are not allowed"), checked before the name; it used to be
  42710 on the `<table>_pkey` name. A one-column key is recorded in the
  schema header as `CREATE TABLE` records it. A composite one is the
  sidecar named `<table>_pkey`, as at CREATE; the header holds one column,
  so a composite key under another name is not recognised as the primary
  key without a format change (not made). Before, ADD stored a UNIQUE only.
- **The review's reading was not what happened, by experiment at
  `dd542ac`.** After the import's drop and re-add, `pg_index` showed
  `indisprimary = t` and `SHOW COLUMNS` `is_primary_key = t` for `users.id`,
  equal to a table created with the key. The cause was the opposite bug:
  `DROP CONSTRAINT users_pkey` of a table-level one-column key removed its
  sidecar and **left the header**. Through the load, and for good after a
  DROP not followed by an ADD, `pg_index` and `SHOW COLUMNS` called `id`
  the primary key while duplicates went in, and the row locks framed keys
  from it. The header now goes with its constraint
  (`primary_header_goes`): dropped as `<table>_pkey`, or when what kept its
  column unique goes. With both halves, the import ends where `CREATE
  TABLE … PRIMARY KEY` does (`tests/import_primary_key.rs`, which compares
  it with a twin created with the key, across a restart). A composite key
  is in neither `pg_index` nor `is_primary_key`, created or imported: older
  than this slice, and the two agree. The reviewer's
  `pg_index … WHERE indrelid = 'users'::regclass` fails because `::regclass`
  is not implemented. The literal stays text, and comparing text with the
  BIGINT `indrelid` fails at plan compile ("literal cannot be compared
  against a I64 column", XX000). Joining `pg_class` on `relname` works.
- **Constraint names are cut to their stored 128 bytes where they are
  read** (`unique_key::stored_name`, in `split_constraint_name`,
  `dropped_constraint` and the derived names). The ADD's duplicate check,
  the DROP's lookup and the record now hold one string. A 131-byte name
  could be created and never dropped (42704), and two names sharing 128
  bytes were both accepted and stored alike.
- **Unnamed ADD CHECKs and UNIQUEs are named as PostgreSQL names them.** A
  CHECK is `<table>_<column>_check` when it reads one column (outside
  string literals) and `<table>_check` otherwise, numbered from 1 on a
  collision (`check::unnamed_name`). A UNIQUE whose `<table>_<cols>_key`
  is taken is `<table>_<cols>_key1`, …, where it was an error. Stored names
  do not change. `CREATE TABLE`'s table-level unnamed CHECK is still
  `<table>_check<n>` (`lexer_ct.rs`, left alone for the correctness slice).
- SET NOT NULL over a NULL now answers 23502, as the finding below asked.

**Round 5** (2026-10-27), from the round-4 re-review:

- **Choosing a name could spin forever (merge-blocker, fixed).** Round
  4's numbering cut `<stem>_check<n>` to 128 bytes after joining. Once the
  stem neared the limit, the cut took the number off, every candidate was
  the same taken name, and the loop spun under ACCESS EXCLUSIVE; a new
  connection's startup then hung. Repro: a 125-byte table name and `ADD
  CHECK (a > 0)` twice (`ADD UNIQUE (b)` likewise). Names are now built as
  PostgreSQL's `makeObjectName`/`ChooseConstraintName` build them
  (`unique_key::name_with_label`, `choose_name`): the stem is cut at a
  character boundary so the label and its number stay whole, and the
  search errors after 10,000 candidates rather than spin. The derived
  `<table>_pkey` and `<table>_<cols>_key` are built the same way. The
  partitioned rename intent's set-aside search (`__prename.intent.bad-<n>`)
  is bounded too: its suffix could not be cut, but the search had no end.
  No other loop here builds a name and tests it. Unnamed `CREATE INDEX`
  derives `<t>_<cols>_idx` without numbering (by reading).
- **The primary key answers to its own name only**
  (`primary_key_constraint`, derived from what the schema records; no
  format change):
  - An inline key is `<table>_pkey`. `DROP CONSTRAINT <table>_<col>_key`
    no longer drops it (42704, or a no-op under IF EXISTS); it used to, and
    duplicates went in.
  - `ADD CONSTRAINT k1 PRIMARY KEY (a)` is `k1`, so `DROP CONSTRAINT
    <table>_pkey` is 42704. It used to clear the header while `k1` went on
    enforcing.
  - A composite key is the sidecar named `<table>_pkey`. The sidecar has no
    "primary" field, so a composite key named otherwise could not be told
    from a UNIQUE. `ADD CONSTRAINT k PRIMARY KEY (a, b)` is therefore
    refused (0A000, naming the rule) rather than accepted beside a second
    PRIMARY KEY. `CREATE TABLE … CONSTRAINT k PRIMARY KEY (a, b)` goes
    through `lexer_ct.rs`, left alone: it is still accepted and not seen as
    the primary key.
  - When a table-level one-column key has two one-column sidecars on its
    column, the one named `<table>_pkey` wins, else the first declared. The
    schema cannot say which one the PRIMARY KEY was.

**Round 6** (2026-10-27): **one spelling for every derived name.** On a
table name over 123 bytes, `parse_clause` named an unnamed primary key
`name_with_label(table, "_pkey")`, cut so the label survives, while every
comparison built `format!("{table}_pkey")` uncut:

- an unnamed `ADD PRIMARY KEY (a, b)` was refused 0A000 as "named";
- `CREATE TABLE … PRIMARY KEY (a, b)` was not seen as the primary key, so a
  second ADD got 42710 rather than 42P16;
- `identifying_key` missed it and framed row locks from another key.

Every derived name is now built by one helper and compared by the same:

- `unique_key::pkey_name`: the named-composite refusal and its
  prediction, `primary_key_constraint`, `identifying_key`, `ON CONFLICT ON
  CONSTRAINT` (`constraint_columns`), the DROP INDEX refusal
  (`constraint_index`), the implicit index (`index::for_schema`) and the
  import's plan;
- `unique_key::implicit_key_name` (`<table>_<col>_key`): the implicit index
  and `implicit_unique_column`, which also matches the name as this table
  cuts it;
- `UniqueKey::default_name`, `check::derived_name` and
  `CreateIndex::name_or_default` (`_idx`).

No `format!` of a derived name is left in core or the migrator.
`src/executor.rs`, which nothing compiles, was left as it is.
`partition_alter::a_long_table_names_its_keys_as_it_compares_them` pins it
with a 125-byte name on both kinds:

- composite and one-column keys by CREATE TABLE, ADD and inline, each the
  primary key (42P16 on a second; `pg_index` and `is_primary_key`; not for
  a composite, as before);
- dropped by the cut name;
- the implicit index in `pg_indexes`;
- `ON CONFLICT ON CONSTRAINT` DO UPDATE and the DROP INDEX refusal on an
  inline key;
- an inline UNIQUE dropped by its cut `<table>_<col>_key` (plain only);
- the row locks' key: dropping a narrower UNIQUE under a lock is not
  refused (55006), since the primary key is the key.

**Deviations from the plan.**

- The shape epoch does not move for an index-only DDL, as on a plain table
  (persistent-indexes §7 "shape_epoch"); `schema_generation` moves once per
  DDL. The brief's "shape_epoch bumps once per logical DDL" is read as "at
  most once".
- No per-partition write ticket before the shell's sidecars: the snapshot
  identity's shape hash is the invalidation (above).
- RENAME TO keeps its own intent rather than the plain one; the plain one's
  legacy two-line parse accepted a torn intent (finding below).
- The reload reopens the row-id source from its reservation, so ids jump
  as at a start; `pg_stat_user_tables` then counts as the plain twin's
  does after the same DDL (the twin caught the difference).
- `DROP NOT NULL` of a non-key column on a table with no row bytes is a
  metadata change (its companion added), where the plain path rewrites
  zero rows; the answers are the twin's.
- CHECK and NOT NULL validation run as the plain arms' fan-out `SELECT`
  under the DDL's lock; UNIQUE validation reads every open partition
  (`row_tables_held`).
- A backup copied before its intent was durable (`__rwb___pt_<uid>_schema*`)
  is left after a crash, discarded by the table's next schema backup, as a
  plain table's; a DROP of the table does not reap it (a leak of a few
  sidecar copies).

**Findings outside the slice** (reported, not fixed):

- **The plain RENAME intent parses a torn record.** `parse_intent`
  (`database/mod.rs`) accepts the legacy form `old\nnew` without a checksum,
  and `record_rename_intent` writes the intent with `truncate(0)` and an
  append, not by renaming a whole file into place. A power-cut image of a
  partitioned rename routed through it held `pt\npt` (a prefix of
  `pt\npt2\n<sum>\n`); recovery read it as "rename pt to pt, both rows
  exist", tombstoned the only row, and the reaper deleted every partition
  file (`Completing an interrupted rename of pt to pt` in the log). On a
  plain table the same parse would tombstone the table's row and discard
  its files; a torn new name that is another table's name would discard
  that table's files.
- **`CREATE UNIQUE INDEX IF NOT EXISTS` enforced nothing** (fixed in the P1
  rework, `3fa8b1a`). Its rewrite, `ADD CONSTRAINT IF NOT EXISTS name UNIQUE
  (…)`, reached the arm as `CONSTRAINT IF NOT EXISTS …`, which
  `unique_key::parse_clause` read as a constraint named `IF`: "accepted but
  not enforced", on plain and partitioned tables alike.
  `unique_key::strip_if_not_exists` now takes it out for the arm and the
  partitioned refusal, and a UNIQUE or PRIMARY KEY that still does not parse
  is an error (0A000), never that success.
- **Constraint names were stored ASCII-filtered** (fixed in P1 round 3 for
  UNIQUE and CHECK, below). `unique_key::write_name` and the CHECK sidecar
  kept only ASCII letters, digits and `_`: `名` was stored as an empty name,
  `"my key"` as `mykey`, `価格_chk` as `_chk`, and a DROP by the name given
  matched nothing and reported success. Still filtering, and not changed
  here: `foreign_key::write_name` (an FK's constraint, table and column
  names; UTF-8 since [phase 3 P4](#phase-3-p4-as-built)), and the column name in the DEFAULT, GENERATED and JSON-map
  sidecars (`column_default`, `generated`, `json_map`) and
  `migrate::plan`. A non-ASCII column name is refused by `rename_field`, so
  the column sidecars may be unreachable that way; an FK named `"my fk"` is
  not, and would be stored as `myfk`. Older than this slice, on both table
  kinds.
- **Reported in round 4, not fixed:**
  - A record an older binary wrote ASCII-filtered (`mykey`) cannot be
    dropped by its original name (`"my key"`: 42704), only by the stored
    one.
  - `EXCLUDE` is accepted and not stored, so a DROP of it gets 42704.
  - After `RENAME TO`, a one-column primary key is matched as
    `<new>_pkey`, where PostgreSQL keeps the constraint's name
    `<old>_pkey`. A composite one keeps its sidecar name `<old>_pkey`, so
    it is no longer seen as the primary key (`has_primary_key`,
    `identifying_key`), and a second PRIMARY KEY would be accepted (by
    reading, not run).
  - A composite primary key appears in neither `pg_index` nor
    `is_primary_key`.
- `SET NOT NULL` over a NULL answered XX000 (PostgreSQL: 23502; 23502 since
  P1 round 4), and a
  missing column in a select list 42601 (42703), on both table kinds.
- Once, in the first twin run at 0x5eed2b20, a debug build's tokio worker
  panicked with "attempt to add with overflow" at
  `core/row/row_pointer.rs:2094` (a string header read as an offset), and
  the run's log ended there although the run exited 0; two reruns of the
  seed passed whole and the panic did not recur. Unconfirmed reading: a
  retired instance's index build (spawned at open, checking `alive` only
  before it starts) still reading rows after a reload while the new
  instance's `VACUUM FULL` rewrote the files. A plain table's reload retires
  the same way, so this would not be partitioned-only.

## 1. Model

### 1.1 What a partition is

**Logically**, the rows whose **routing point** (a `u64`, §1.3) lies in a
half-open `[lo, hi)`. The **range map**, a sorted `Vec<(lo: u64, pid: u32)>`,
covers `[0, 2⁶⁴)` with no gap or overlap, checked by `RangeMap::validate` on
every load and new map epoch. Partition ids are monotonic and never reused.

**Physically**, three files in one pool location, named from a random nonzero
64-bit table id `uid` fixed at `CREATE`, each with today's `.tmp` sibling:

```
__pt_<uid:016x>_<pid:08x>.db            row bytes
__pt_<uid:016x>_<pid:08x>_pointers.db   row pointers (xmin, xmax live here)
__pt_<uid:016x>_<pid:08x>_flt.db        filter checkpoint
```

Names use only `[A-Za-z0-9_]`, well under 128 bytes, to fit the schema
header's name filter (`core/row/schema.rs:540-555`) and avoid the `$` trap. No
table name, so `RENAME` touches no partition file; no tenant value, so no
customer data reaches a file name. Flat in the location directory, since a
subdirectory has no write-ahead log and falls back to an fsync per append,
about ten times slower ([storage pools](storage-pools.md), "Write-ahead logs").
No schema or sequence file: a partition opens with a clone of the shared schema
and a handle on the shared row-id sequence (§3.4, §3.5).

**The logical table** is a `PartitionedTable` in a new `Database.partitioned:
DashMap<String, Arc<PartitionedTable>>` (schema, key spec, map with each
partition's placement and slot, row-id sequence), plus a **shell** in
`Database.tables` (`core/database/mod.rs:671`): a `Table` with the shared
schema that **refuses every data access with an error** (§3.3). About 120
non-test uses of `tables.get`, `iter`, `contains_key` and `remove` span 18
files, 49 in `executor.rs` (a few name other maps). The shell keeps schema-only
sites working (pgwire describe, `param_infer`, `index_name_taken` at
`core/rql/executor.rs:7939`, `referencing_keys` at `executor.rs:1236`, which
`ON DELETE CASCADE` uses; since phase 3 P4 it reads `Database.fk_reverse`) and makes unconverted data sites fail loudly; absent,
some would miss silently: `DROP TABLE IF EXISTS t` would succeed dropping
nothing (`drop_table`'s `Ok(false)` becomes success,
`executor.rs:6626-6629`), and `ALTER TABLE IF EXISTS t …` would report
"relation absent, nothing to do" (`executor.rs:7066-7082`).

### 1.2 How tenants map to partitions

A tenant, one value of the leading key column, occupies one contiguous
**block** of 2¹⁶ points (§1.3). Every boundary that `CREATE`, an ordinary
split, a merge or a move sets is a multiple of 2¹⁶, so a tenant not sub-split
by `SPLIT KEY` is always in exactly one partition. With N partitions the
initial boundaries are `lo_i = ⌊i · 2⁴⁸ / N⌋ << 16`.

**Phase 2a defaults N to 64**, since its cap of 2,048 declared partitions per
database (§7.1) would admit only two tables at 1024. **From phase 3 the proposed
default is 1024**, for the arithmetic above; an unused partition costs a map
entry, since files and location come at first write (§3.2, §6.1). The price,
unrouted fan-out, is unmeasured, so phase 2a's fan-out and phase 3's open-cost
measurements fix that default (§8). `RangeMap::initial` lays out at most
65,536 (`MAX_INITIAL`, `core/partition/map.rs:13`).

### 1.3 The hash

Version `route_v1`, frozen and recorded in the map header; a binary that does
not know a table's version refuses to open it.

```
frame(f, b)  = f:u8 ‖ len:u32 LE ‖ b
H_i          = XXH64(frame(family_i, norm_i(v_i)), seed = 0x5244_4250_5254_0000 + i)   // "RDBPRT"
point, 1 col = top48(H_0) << 16
point, 2 col = top48(H_0) << 16 | top16(H_1)
point, 3 col = top48(H_0) << 16 | top8(H_1) << 8 | top8(H_2)
```

- **XXH64 is in-tree**, about 80 lines, so no crate update can move data,
  pinned by reference vectors and framed golden vectors per family and level:
  `0`, `-1`, `i64::MIN`, `u64::MAX`, `''`, non-ASCII text, a fixed UUID. It is
  `core/hash/xxh64.rs` (persistent-indexes §8, §15), neutral ground so their
  gin item hash and sharding's digests use it without depending on
  `core/partition`; it started as `core/partition/xxh64.rs`, and the move
  changed no output (the same reference vectors, I1, and the route_v1 golden
  vectors pin it). `core/partition/hash_source_tests.rs` refuses any other
  hash on the routing path, rapidhash included.
- **Not ahash**, which `KeyLocks::stripe_of` (`core/row/key_locks.rs:53-62`)
  uses with `write_usize`: its output can change with crate version, target
  features and pointer width, harmless for lock stripes, catastrophic for
  persisted placement.
- **Seeds depend on the level, not the table**, so `tenant_id` gives the same
  block in every table leading with it, all co-location needs. Levels hash
  separately, so `('ab','c')` and `('a','bc')` differ at level 0; the length
  prefix also keeps frames self-delimiting.
- **The input is column-typed bytes after coercion, never literal text**: at
  insert, `RowWrite.columns_writing_data[k].data` (`core/row/row.rs:29-35`)
  after defaults and generated values; at query, `SargPredicate.value`
  (`core/filter/sarg.rs:57`), coerced by `conjunct_to_pred` (`sarg.rs:158`) with
  the evaluator's own `coerce_literal` (`core/value_codec.rs:1453`). Both go
  through one `route_bytes(family, &[u8])`, the pairing the hash index relies
  on and property-tests (`core/index/key.rs:434`). Normalisation is per family,
  unlike `encode_canonical`'s per-width bytes (`key.rs:222`).
- **Collisions are harmless.** Two tenants share a 48-bit block with expected
  count n²/2⁴⁹, about 0.002 pairs at 10⁶ tenants. Routing is a superset and
  rows are re-evaluated against the whole `WHERE`, so colliding tenants merely
  cannot be isolated from each other.

### 1.4 Allowed key types

An allowlist, not `hash_supported` (`core/index/key.rs:83`):

| Family | Types | Normalised bytes |
|---|---|---|
| `0x01` integer | I8–I128, U8–U64 | the value as i128, 16 bytes LE, so `INT 42` and `BIGINT 42` hash alike |
| `0x02` uuid | UUID | the 16 stored bytes |
| `0x03` text | TEXT, VARCHAR(n) | the UTF-8 bytes, in the evaluator's binary order |

All else is refused at DDL time with its reason, by one rule: if the engine's
`=` for a type may yet change to match PostgreSQL, rows placed under the old
semantics would sit in the wrong partition.

| Refused | Reason |
|---|---|
| U128 | does not fit in i128 |
| F32, F64 | NaN, −0, lossy coercion |
| NUMERIC | bytes depend on scale; `parse_decimal_scaled` (`value_codec.rs:500`) truncates |
| JSONB | its `=` is not PostgreSQL's, and is not property-tested |
| CITEXT, BPCHAR, CHARACTER, CHAR(n > 1), NAME | map to STRING without their semantics (case folding, blank padding, NAME's 63-byte truncation; `core/rql/lexer_alter.rs:527-532`). Bare `CHAR` and `CHAR(1)` map to `DbType::CHAR`, not on the allowlist. |
| INTERVAL, TIMETZ, geometric and range types | equality is not byte equality |
| BOOL | too few distinct values |
| a non-default `COLLATE` | the rule above |
| date, time, INET, MAC | v1 only: sound, but unasked for |

`CREATE TABLE … PARTITION BY` catches the STRING aliases by declared text. At
conversion the text is gone (neither the field record nor the `TYP` sidecar
keeps it; `core/rql/type_modifier.rs` holds only precision and scale), so
conversion refuses a STRING key unless a new additive sidecar, which must join
`RENAMED_SUFFIXES`, recorded it as `TEXT` or `VARCHAR`. Older tables cannot
convert on a text key; use `CREATE … PARTITION BY` plus `INSERT … SELECT`. The
PostgreSQL importer decides from the source's `pg_type` (`citext`, `bpchar`,
`name`), never the mapped type.

### 1.5 NULL

Key columns must be `NOT NULL`; `ALTER … DROP NOT NULL` on one is refused. A
NULL is zero bytes plus a `__null_` companion (`core/rql/nullable.rs:48`), so it
would hash like `0` or `''`, and `IS NULL` names the companion, so it could
never route. On a NOT NULL column the rewriter makes `x IS NULL` into `1 = 0`
(`sarg.rs:506`), routing to zero partitions by proof. Key columns cannot be
generated, retyped or dropped; `RENAME COLUMN` is fine, as the hash never sees
names.

## 1b. Composite keys

### Flat or hierarchical

| | Flat: `H(frame(t) ‖ frame(g))` | Hierarchical: `top48(H₀(t)) ‖ top16(H₁(g))` |
|---|---|---|
| A query fixes `(t, g)` | 1 partition | 1 partition |
| A query fixes `t` only | **every partition** | the partitions overlapping t's block, normally 1 |
| Where one tenant's rows sit | sprayed over all partitions | contiguous, so the tenant can be isolated |
| A big tenant | already spread, but every one of its queries fans out | starts in one partition and is split by g on demand |
| Low-cardinality leading column | spreads evenly | concentrates rows in few partitions |

**Only hierarchical in v1**: "supply `tenant_id` and touch only that tenant's
subtable" is prefix routing, which flat cannot do. Flat, PostgreSQL's choice
for `PARTITION BY HASH (a, b)`, suits only a low-cardinality leading column, a
layout the documentation should advise against ("put the tenant column
first"). `WITH (key_layout = 'flat')` can come later without touching v1
tables.

**Widths.** At most three columns: `[48]`, `[48,16]` or `[48,8,8]`, frozen in
the map header. Level 0 is 48 bits at every arity, so tables leading with
`tenant_id` put a tenant in the same block. The 16 low bits allow 65,536
sub-ranges per tenant.

### What routes

| `WHERE` fixes by equality | Routed to |
|---|---|
| Every key column | One point, so one partition |
| A leading prefix: `tenant_id` alone, or the first two of three columns | That prefix's interval: normally one partition, or the tenant's sub-split pieces |
| Only a non-leading column, such as `asset_group_id` | All partitions |
| A range, `!=`, a function or a cast on a key column | Nothing at that level; it is unconstrained |

**Multi-column IN.** `tenant_id IN (1,2) AND asset_group_id IN (7,8,9)` is
lowered by `rewrite_where` (`core/tokenizer/predicate_rewrite.rs:1673`) to an
`AND` of `OR`s: 6 points (§4.2). Each `IN` is one box holding a value set, so no
product is built to be thrown away. Past **256 points** the expansion
**degrades from the right**, dropping the last constrained level from every box,
de-duplicating, repeating; 3 tenants × 500 groups route to the 3 tenant blocks.
The cap applies **during** the fold (§4.2, step 4), so two or three
10,000-value lists cost O(cap × levels), never 10⁸ boxes. Only dropping level
0, or level 0 alone exceeding the cap, reaches all partitions; degrading only
widens. A tuple-`IN` such as `(t, g) IN ((1,7),(2,9))` is not lowered by the
rewriter, so it fans out.

### Sub-splitting a big tenant

`SPLIT KEY (tenant_id = 42) INTO k`, k a power of two up to 1024, places `k − 1`
boundaries in the tenant's block B at `B + j·2¹⁶/k`, dividing its rows by the
hash of `asset_group_id`: `(42, g)` hits one piece, `42` alone all k (still not
the whole table), other tenants nothing. A single-column key cannot split a
tenant, as one hash value is atomic, which is also Citus's and Yugabyte's limit.
Declare `(tenant_id, <natural sub-key>)` from the start; the second level costs
nothing until used.

### Uniqueness

Every `UNIQUE` and `PRIMARY KEY` must contain **every** key column,
PostgreSQL's rule, so equal unique values share a partition and per-table
enforcement (`unique_lock`, `check_unique`, the unique index) is exact. The
rejected alternative, a leading-column unique key with sub-splitting refused
while it exists, makes uniqueness depend on layout history: every
boundary-creating path (split, merge, rebalance, isolate, co-location joins)
must enforce it, and one miss stores a silent duplicate. The static rule is one
DDL check nothing later can break. Its cost: a `(tenant_id, asset_group_id)`
key rules out `PRIMARY KEY (tenant_id, asset_id)`. The error says so and names
the alternative, partitioning by `(tenant_id)` alone (isolatable, not
sub-splittable). An opt-in is §17.6.

**Row-addressing statements use a composite identifying key** (phase 3 P3,
[as built](#phase-3-p3-as-built)). `FOR UPDATE`/`SHARE`, `SKIP LOCKED`,
`NOWAIT`, `UPDATE … FROM` and `DELETE … USING` used to name each row by one
column (`identifying_field`: an inline `UNIQUE` column, a one-column `PRIMARY
KEY` or unique key) and refuse composite keys; `PRIMARY KEY (a, b)` leaves
`schema.primary_key` empty (`core/rql/lexer_ct.rs:229-233`). By the rule above
a composite-keyed table has no one-column unique key, so they were refused on
essentially every partitioned table, and on plain tables with a composite
primary key. `identifying_key` now accepts a composite primary key or a
`UNIQUE` whose columns are all `NOT NULL`; `RowLocks` keys a row by the framed
tuple (`encode_canonical`, length-prefixed) under the logical name, and
`refuse_write_to_locked_rows` and the deadlock detector use the same key. The
change fixes plain tables too.

### Co-location

`PARTITION BY HASH (tenant_id) COLOCATE WITH accounts` shares `accounts`' map
and placement; reshapes apply to the group under one epoch. Members need the
same level-0 family and `route_v` (INT and BIGINT co-locate). A group sub-split
cuts every member at the same points; a one-column member's rows for that
tenant land in the first piece (low bits zero), still correct. It puts a
tenant's rows in every member on one disk and enables partition-wise joins.
Phase 6.

## 2. SQL surface

```sql
CREATE TABLE t (...) PARTITION BY HASH (c1 [, c2 [, c3]]) [PARTITIONS n] [COLOCATE WITH u];

-- The user's framing; the same conversion as the ALTER (phase 5)
CREATE INDEX t_by_tenant ON t USING partition (tenant_id, asset_group_id) [WITH (partitions = n)];
ALTER TABLE t PARTITION BY HASH (tenant_id, asset_group_id) [PARTITIONS n];

-- Reshaping (phase 5); local to this node, not replicated
ALTER TABLE t SPLIT PARTITION FOR (tenant_id = 42);
ALTER TABLE t ISOLATE KEY (tenant_id = 42) [TO '/mnt/nvme2'];
ALTER TABLE t SPLIT KEY (tenant_id = 42) INTO 8;
ALTER TABLE t MERGE PARTITIONS FOR (tenant_id = 42), FOR (tenant_id = 77);   -- adjacent only
STORAGE MOVE PARTITION t FOR (tenant_id = 42) TO '/mnt/nvme2';
STORAGE REBALANCE PARTITIONS [DRY RUN];

SHOW PARTITIONS t;
SET partition_trace = on;   -- a NOTICE per statement: partitions routed, and why
```

**Refused**, with `0A000` or `42P16` and a reason:

| Refused | Reason |
|---|---|
| `PARTITION BY RANGE` or `LIST`; `DEFAULT` partitions | not designed |
| `PARTITION OF … FOR VALUES WITH (MODULUS m, REMAINDER r)`, `ATTACH`, `DETACH` | the child would hold different rows than in PostgreSQL. A later phase may accept `PARTITION OF` as a **write-only alias** (`INSERT` and `COPY` route through the parent, as a `pg_dump` restore needs; `SELECT`, `UPDATE`, `DELETE` stay refused). |
| A key breaking §1.4 or §1.5, or over three columns; a unique key without every key column | §1.4, §1.5, §1b |
| Naming a partition; `CREATE TABLE` or `RENAME` to a `__pt_` name | `add_table` checks only `contains_key` (`core/database/mod.rs:2100`); rename refuses only the JSON shadow prefix (`database/mod.rs:1408`) |
| `STORAGE MOVE TABLE t` | points at `MOVE PARTITION` |
| JSON columns in shredded mode | shadow tables would be unpartitioned; native JSONB is fine |
| Temporary partitioned tables; KV-backed partitions | KV until measured |
| `DROP INDEX` on a `USING partition` index | not an index (above) |
| Until their phase: physical-rewrite `ALTER` on partitioned tables (phase 6, §11; the metadata forms, `RENAME` and `CREATE`/`DROP INDEX` are built in phase 3 P1); point-changing key assignment (phase 6, §9; any key assignment its `WHERE` does not pin to the same value, §4.5, which includes an `ON UPDATE CASCADE`, `SET NULL` or `SET DEFAULT` writing a child's partition key column); conversion and reshapes (phase 5). `FOR UPDATE`/`SHARE`, `SKIP LOCKED`, `NOWAIT` and partitioned targets of `UPDATE … FROM`/`DELETE … USING` are built (phase 3 P3, §1b); FKs either way are built (phase 3 P4, §10) | §15 |
| A `FOREIGN KEY` referencing a partitioned table by anything but its whole partition key (42830, PostgreSQL's "no unique constraint matching given keys": a unique key holds every key column, §1b, and a key is one column) | §10 |

**What phase 2b refuses, as built (S20).** Each with 0A000 and "not
supported" and a reason, before anything is read or written; the twin's
`REFUSED` list (`tests/partition_twin.rs`) and the site catalogue
(`tests/partition_shell_sites.rs`) pin each row. Everything not listed
answers like a plain table (below).

| Refused on a partitioned table | Where it is refused | Lifted by |
|---|---|---|
| ~~`UPDATE … FROM` and `DELETE … USING` with a partitioned **target**~~ | ~~their executor entry, before the join runs~~ | lifted by phase 3 P3 (composite identifying keys, §1b) |
| ~~`SELECT … FOR UPDATE`/`FOR SHARE`, `SKIP LOCKED`, `NOWAIT`~~ | ~~`refuse_partitioned_row_lock`, before routing~~ (removed) | lifted by phase 3 P3 |
| ~~`ALTER TABLE` in every form, `CREATE INDEX` in every form~~: lifted by phase 3 P1 except the rows of [its table](#phase-3-p1-as-built) | the DDL arms, before any sidecar is written | phase 3 P1 (metadata DDL, §11); physical-rewrite forms phase 6 |
| ~~`FOREIGN KEY` from a partitioned table (`CREATE … PARTITION BY`, `ADD CONSTRAINT`), and onto one (`REFERENCES`, `ADD CONSTRAINT`)~~ | ~~`CREATE` and the DDL arms~~ | lifted by phase 3 P4 ([as built](#phase-3-p4-as-built)); onto one by any column but its whole partition key: 42830, not a refusal |
| An `UPDATE` assigning a key column anything its `WHERE` does not pin to the same value (`SET tenant_id = 5 WHERE tenant_id = 1`, `SET g = g + 1`), and a `DO UPDATE` that does so when its conflict happens | `assignments_keep_point` before routing (`DO UPDATE`: before the update writes); the range guard on every merged row behind it | phase 6 (row moves, §9) |
| `ALTER … PARTITION BY`, `CREATE INDEX … USING partition` (on plain tables too), `SPLIT PARTITION`, `ISOLATE KEY`, `SPLIT KEY`, `MERGE PARTITIONS`; `STORAGE MOVE TABLE` on the logical name, which points at `MOVE PARTITION` | the ALTER, CREATE INDEX and STORAGE arms | phase 5 |
| Temporary and unlogged partitioned tables; an in-memory database; a storage pool with backup mirrors; KV-backed partitions; shredded JSON columns | `CREATE … PARTITION BY` | not scheduled; KV until measured |

`ON CONFLICT` with a target that is not exactly one unique key (one that leaves
out a key column, a subset or a superset) is not a refusal but PostgreSQL's
42P10, on plain tables too, and a multi-row `DO UPDATE` proposing one key
twice is 21000 (§9). Two phase-5 spellings are not yet parsed as such:
`STORAGE MOVE PARTITION t FOR (…)` is read as a move of a table named
`PARTITION` and refused for that, and `STORAGE REBALANCE PARTITIONS` as a
whole-database `STORAGE REBALANCE`, which never proposes the shell.
`SELECT` with `ORDER BY` across partitions is **not** refused: it is correct
and not yet cheap (§8, S12).

Everything else in the twin test's statement set (I6) answers like a plain
table: `INSERT` in every form, single- and multi-partition `SELECT`/`COUNT`
(aggregates, `GROUP BY`, `DISTINCT`, windows, `UNION`, CTEs, `IN (SELECT …)`,
empty routes), `UPDATE` and `DELETE` routed or fanned out, a key column
assigned the value its `WHERE` pins (in place, as on a plain table, §4.5), in
autocommit and in blocks; `RETURNING` on `INSERT`, `UPDATE` and `DELETE`;
`ON CONFLICT DO NOTHING` and `DO UPDATE` in every form, targetless,
by columns and `ON CONSTRAINT`, multi-row and `INSERT … SELECT`; joins
(inner, `LEFT`, self-joins, partitioned × plain and × partitioned, with
CTEs, derived tables, `IN (SELECT …)`, aggregates and `ORDER BY … LIMIT`
over them) and `UPDATE … FROM`/`DELETE … USING` reading a
partitioned source into a plain target; prepared `SELECT` (from the learned
parse), joins, `INSERT`, upserts, `UPDATE` and `DELETE`; `VACUUM` and `VACUUM
FULL`, named and bare. `LOCK TABLE` takes the logical name's lock, which a
partitioned `UPDATE` and `DELETE` check before routing, as a plain table's do.

**Today's mis-parse, fixed first.** `parse_create_table` takes the body between
the first `(` and the **last** `)` (`core/rql/lexer_ct.rs:169-171`), so `… k INT
NOT NULL) PARTITION BY HASH (k)` **creates an unpartitioned table and reports
success**. Phase 0 replaces it with a scan tracking nesting and quotes that
finds the column list's matching paren and refuses a top-level `PARTITION`,
`INHERITS` or `PARTITION OF` after it, on word boundaries. Other trailers
(`WITH`, `TABLESPACE`) keep today's behaviour, a separate latent bug.

**PostgreSQL import** already reads partitioned sources through the parent
(`core/migrate/source.rs:185-194`). It emits `PARTITION BY HASH (<same
columns>)` when every key column is NOT NULL and allowed and every unique
constraint contains the key; otherwise a plain table, warning with the reason.
It never reproduces PostgreSQL's remainders.

## 3. Storage layout and metadata

### 3.1 Parent files and the catalogue

All parent files live in the root location:

| File | Contents |
|---|---|
| `__pt_<uid>_schema.db` and its sidecars `__pt_<uid>_schema_{FIELDS,FK,DEF,UK,JSON,TYP,CHK,GEN,PIX,IDX}.db` | the one shared schema. Sidecar names derive from the schema provider's name (`core/row/schema.rs:459`), so the load code needs no change. |
| `__pt_<uid>_map_<gen>.db` | the partition map log, one live generation (§3.2) |
| `__pt_<uid>_ids.db` | the shared row-id reservation (§3.5) |
| `__pt_<uid>_seq.db` | durable high-water marks of the table's `SERIAL` columns (§10, phase 3) |

**The catalogue row** keeps `table_name` and sets `clusters = uid` (written 0
today, `database/mod.rs:2111-2113`, only logged at load,
`database/mod.rs:2892-2895`); nonzero means partitioned, files `__pt_<uid>_*`.

**Downgrade guard.** An older binary ignores `clusters`: `Table::new` creates an
empty `t_schema.db`, `TableSchema::load` refuses a file shorter than its header
(`core/row/schema.rs:436-445`), the table is marked `schema_unreadable`
(`core/row/table.rs:560-574`) and left out of the map
(`database/mod.rs:958-964`): missing, not empty. That chain was read, not run;
it must be **tested against the previous release's binary** before phase 2
ships (I18). There `CREATE TABLE t` then succeeds (`add_table` checks only
`contains_key`) with a second live row `t`, `clusters = 0`, and since startup
inserts once per row (`database/mod.rs:950-967`), after re-upgrading load order
would pick the `t` served. So the new binary **refuses to start when two live
catalogue rows share a name**, naming both.

**`RENAME`** avoids `rename_table`, which copies every `<old>*` file, rewrites
`<new>_schema.db` by name, and commits via `remember_in_catalogue`, hardcoding
`clusters = 0` (`database/mod.rs:2003-2010`, called at `:1469`): the next start
would open the table as plain, and with no row carrying the uid the reaper
would delete every `__pt_<uid>_*` file. A partitioned rename (1) records the
rename intent (`record_rename_intent`, `database/mod.rs:2073`); (2) writes a new
row with the **same** `clusters = uid` via a `remember_in_catalogue` variant
taking the cluster value, **the commit point**; (3) rewrites the name in
`__pt_<uid>_schema.db`'s header; (4) tombstones the old row. No file is copied
or deleted. Recovery reads two live rows with one uid as "rename committed",
keeps the newer name and finishes steps 3–4; the reaper sees a live uid
throughout (I25). *Phase 2a refuses `RENAME`, and its startup refuses two live
rows sharing a uid (`catalogue_duplicates`, `database/partitioned.rs`, marked
`PARTITION-TODO(rename)`); the rename slice replaces that refusal with this
recovery.*

**`db_storage.cfg` never names a partition**, even in memory:
`StorageManager::render` serialises every `placements` entry
(`core/storage/mod.rs:933`) and round-robin `choose` counts them under the
write lock (`storage/mod.rs:1306`), which would bring back O(N²) placement. A
partition opens from its map entry's location via `seed_at(path)`, never
`pool.location_of` (`storage/mod.rs:495`) or `seed_in`'s lookup
(`database/mod.rs:471`), which answer "root" for unknown names and would open
empty files there. Pool guards consult `Database::partition_locations_in_use()`,
counting materialized partitions only: a location holding any cannot be
dropped or made a backup. `STORAGE DISABLE LOCATION` stops new materializations
there at once and drains the rest via `REBALANCE PARTITIONS`.

### 3.2 The map log

`__pt_<uid>_map_<gen>.db`, in the root directory, is an append-only log of
`len:u32 ‖ crc32:u32 ‖ kind:u8 ‖ payload` records (the existing `crc32fast`);
load replays to the last record whose CRC passes. **Every record is appended
with `append_data(buf, true)`**, through `append_data_txn`, issuing its WAL
record at append time (`file_sync.rs:1637-1645`, `:1529-1530`). The default
deferred append stages into `.tmp` and lands later with `sync_all` and no WAL
record (`file_sync.rs:1647-1700`, `:955-1035`), so nothing would order it before
an outcome.

The framing, the durable appends and the generation/`SEALED` replay (below) are
to be factored out of `core/partition/log.rs` into a shared pure module, which
sharding's cluster map reuses with records of its own
([sharding §2.2](sharding.md#22-storage-epochs-and-authority), an ask in its
§16 phase 0). The records below and `MapState`'s rules stay here.

| Record | Payload | Durability |
|---|---|---|
| `HEADER` | `uid`, `route_v`, key `(write_order, family)` list, widths, co-location group | fsync |
| `LOCATIONS` | interned location paths, so removing a location cannot shift indices | fsync |
| `PLACE` | `[(pid, lo)]`: one record per `CREATE` or reshape. No location: an unwritten partition has none (§6.1). | fsync |
| `MATERIALIZE` | `(pid, floor, loc_idx)`: the files exist at that location; the forget floor at that moment (§7.2) | WAL record at append; fsync, once per partition |
| `DIRTY` | `(pid, floor)`: a clean partition is about to take its first stamp (§7.2) | root log durable before the stamp |
| `SWEPT` | `(pid, swept_to, clean)`, appended after the sweep's writes are durable | group commit |
| `STATS` | `(pid, rows, bytes)`, after vacuum and on a close that wrote something | group commit |
| `RESHAPE_BEGIN` / `_COMMIT` / `_ABORT` | sources and new entries, with targets' locations | fsync |
| `RETIRED_GONE`, `DROPPED` | pids / none | group commit / fsync |

**`MATERIALIZE` is fsynced, once per partition.** Order: delete leftover
`__pt_<uid>_<pid>*` files (§12), create the files exclusively, fsync the
directory once; append `MATERIALIZE` with its WAL record and sync the root log
(`append_durable`); write the first row; at commit, append its outcome to the
same log. Replay stops at the first torn record, so a durable outcome implies a
durable `MATERIALIZE` (consequences: §12). `bulk_load` takes no WAL records
(`file_sync.rs:1523-1528`); there a load that dies is discarded whole, that
mode's existing contract.

An earlier draft of this section left `MATERIALIZE` to group commit, reasoning
from the outcome alone. That misses a crash before *anything* synced: the
partition's files are written through the page cache, so writeback can keep the
first rows' pages while the log loses the record. The map's tail is then torn
and a new partition's files hold data no record explains, which the open must
refuse rather than guess at (`refuse_torn_leftovers`, §12), so the whole table
stays closed until an operator removes the evidence. The crash model in
`drop_recreate_replay_tests` (a transaction across new partitions, cut off
before any sync) hit that in 40 of 40 images. The price is one fsync per
partition, ever: a partition materializes once.

**Materializing cost is unmeasured.** `create_new` fsyncs the directory per
file and per `.tmp` (`file_sync.rs:2077-2111`), `LocalStorageProvider::new`
three times per file (`:634-671`): six to nine for three files. The partition
path's `create_partition_files` creates every file, fsyncs the directory once,
and propagates that error, which today's constructors discard (`let _ =`).
Phase 2a measures first-write latency per new tenant on ext4.

**Compaction** triggers past twice the last snapshot plus 1 MiB (a fixed
threshold would undercut one snapshot of 65,536 slots and compact on every
append): (1) clone the map under the map mutex, release; (2) write `HEADER`,
`LOCATIONS` and a `SNAPSHOT` of the clone to a new generation; (3) retake the
mutex, append records taken since the clone and `SEALED`, fsync file and
directory, switch appends; (4) delete the old generation. Load takes the
highest generation ending in `SEALED`, reaping the rest. WAL file ids hash the
file name (FNV, `core/wal.rs:164-173`), so old records never replay onto a new
generation and compaction need not wait for a checkpoint. The mutex, also
taken by `MATERIALIZE` on INSERT, is held only for step 3's tail.

### 3.3 The shell refuses data by construction

`Table::partition_shell(name, schema_io)` builds it. Guarding a method list
would be an allowlist, and anything off it would answer from emptiness; three
rules close that:

- **Data fields become private** (`io_pointers`, `io_rows`, `io_filters`,
  `io_sequence`, `len`, `last_row_id`, `filters`, `indexes`, `free_space`,
  `readers`, `vacuum_stats`; `table.rs:48-150`), behind accessors that return
  `Err` on a shell. The compiler lists every outside read: 29 non-test ones of
  `io_rows` and `io_pointers` alone, in `vacuum.rs`, `introspect.rs`, `wal.rs`,
  `catalog_views.rs`, `storage/sql.rs` and `database/mod.rs`. `pg_index` and
  `pg_indexes` treat `indexes == None` as "no indexes" (`catalog_views.rs:988`,
  `:1053`), so a shell would hide every declared index from the view a
  migration tool reads to decide whether to re-issue `CREATE INDEX` (§14).
- **Every data method checks first**: each `pub`/`pub(crate)` method of `impl
  Table` starts with `self.data()?` or is on a named schema-only allowlist; a
  test parsing the `impl` block fails on any other. This catches, among others,
  `find_row_with_value`, `locate_row_by_id` (`table.rs:2452`), index probes,
  `vacuum_now` (`:1100`), `compact` (`:1166`), `sizes_of`, `relation_size`, and
  the executor's `seed_sequences` (`executor.rs:6865`), `lock_selected_rows`
  (`:4427`) and `returning_rows_for_write` (`:8617`), each erroring until
  converted to dispatch on `Database::relation(name) ->
  Relation::{Plain(Arc<Table>), Partitioned(Arc<PartitionedTable>)}`.
- **Every name-keyed `Database` method starts with `self.relation(name)`**,
  dispatching or erring before removing anything from `tables`; the `impl` scan
  covers `impl Database` too. These reach files via `seed_in`, bypassing the
  shell: `table_bytes` (`database/mod.rs:1584`), whose `create_new` for 15
  suffixes would litter the root with empty `<logical>*` files and report 0;
  `reload_table` (`:2493`), which removes the map entry before `Table::new(name)`
  fails, so `DROP TABLE IF EXISTS` then silently succeeds and `CREATE TABLE t`
  duplicates the catalogue row (`CREATE INDEX` and metadata `ALTER`s reach it);
  `move_table_to_backend` (`:1642`); `rename_table` and helpers
  (`copy_table_files` `:1512`, `rewrite_schema_name` `:1948`);
  `drop_table_to_depth`; `vacuum_all` (`:2337`), which logs a failed table and
  returns the rest, so it fails the statement instead. The `VACUUM` statement,
  named and bare, resolves each name through `relation()` and vacuums a
  partitioned table's materialized partitions (S11). Before S11 a bare
  `VACUUM` reached the shell and failed in any database holding a partitioned
  table (finding F2); a partitioned table that cannot be opened is skipped by
  a bare `VACUUM` with a WARNING and fails a named one.
  Whole-table `STORAGE REBALANCE` (`storage/relocate.rs:214-227`) picks from
  `tables`, smallest first, which a shell would be; it skips shells.

### 3.4 The partition `Table`

`Table::open_partition(physical, seed_at(location), schema clone, ids, guard,
mode)` runs `new_with_constraints`'s body (`table.rs:482`) but loads no schema
file or `_seq.db`, spawns no per-table tasks in `mode = External` (phase 3 on),
installs the range guard (§4.5), and sets **`cache_scope` explicitly** to
`"<db_id>\0<physical name>"`; derived from the shared `schema.name`
(`table.rs:467`), one partition's cached rows would answer for another (I20).
The seam sits in files another session is editing (`table.rs`, `vacuum.rs`, the
processor) and the field change is mechanical but wide, so it lands as one
reviewed change, announced first, after that session's index work.

### 3.5 One row-id sequence for the logical table

`locate_row_by_id`, the unique check's `exclude_id` (`table.rs:1981`, `:2375`)
and "at most one live pointer carries a given id" (`table.rs:2474`) need
table-wide unique ids. One sequence per logical table is reserved durably in
`__pt_<uid>_ids.db` in large steps and handed out in chunks of 1024 (one
contended `fetch_add` each), so no raw copy (§6.2), merges included, gives
`exclude_id` two rows with one id. A missing or unreadable ids file refuses the
table, never falling back to the last record's id, which the code calls exact
only for legacy ascending-id files (`table.rs:603-611`). As `create_new` and
`LocalStorageProvider::new` silently create missing files
(`file_sync.rs:2077-2080`, `:634-641`), the parent's ids and map files and every
file of a materialized partition are checked by a never-creating open
(`File::options().read(true).write(true).open`) before any provider is built;
a missing one fails with a named error (I9).

## 4. Routing and pruning

### 4.1 Where it hooks

Routing runs on **exactly the `WHERE` text the partitions' scans will
evaluate**, the post-rewrite clause tokenized against the shared schema as
`build_candidates` does (`table.rs:2730`), late, at the executor arms that
resolve the table name and dispatch on `relation()`:

| Statement | Hook |
|---|---|
| SELECT | the `QueryRows` arm (`executor.rs:10911`) and `execute_query_rows` (`:10954`), taking a `RowSource::{Single, Partitioned}` |
| Prepared SELECT | **As built (S15): learned parse, routed per execution.** `execute_partitioned_select` notes its parse (`note_plain`, after the row-lock refusal) under the **logical** name, so pgwire learns it as a plain table's. A learned execution reaches `execute_prepared_rows`, which looks the name up again and reads through `read_partitioned`, the text path's own read half: pgwire rendered this execution's literals into the template (`execute_watched`), and that `WHERE` routes fresh, with the materialized flags read after this execution's snapshot. **S16b caches the route's derivation, never a route:** for the full-key point shape (every key column compared by `=` with exactly one parameter, as a top-level conjunct), `learn_route` (pgwire) keeps a `prepared::RoutePlan`, which parameter feeds each key column and the column's write order and declared type, and only after checking that the key each of the two verifying executions binds routes exactly where `route_where` routes its rendered `WHERE`. Each execution binds its own values (`RoutePlan::bind` → `prepared::key_value_bytes`, the scan's own reading of an unsigned integer or plain string; anything else binds nothing and routes the rendered `WHERE`), and `FanOut::plan` names the partition from that key (`PartitionedTable::keyed_range`, which refuses a key bound for another table uid or another key, then `materialized_in`), with the snapshot taken first and the materialized flag read after it, as before. An `IN` list, part of the key, a range, an `OR` or a `NOT` keeps the per-execution `route_where`. So a learned parse still cannot pin the first tenant's partition (I11), and a cached key hashes as `route_where` and the insert path do (I2): a property test over SMALLINT, INT, BIGINT, TEXT and a composite key checks all three agree. A one-partition fan-out awaits its read in place rather than through `join_all`. `run_statement` runs a learned parse with no `row_lock` inside `statement_snapshot::scope`, as the text path runs a read-only statement (I7 on the fast path); a plain table's learned `FOR UPDATE` stays outside it, as `is_read_only` keeps its text outside. A signed numeric literal is never learned (`prepared::literal_class` admits unsigned numbers only): the text path rewrites `k = -1` to `k = - 1`, so the literal would not pass through the rewrites unchanged, which every admitted one must. Such an execution runs from its text, where `k = - 1` is not routed yet and reads every materialized partition (a superset, so correct). Routing `k = - 1` on the text path is a pure performance gain and a separate, later item. |
| Prepared INSERT, UPDATE, DELETE | **As built:** the text path every execution. `execute_partitioned_update` and `execute_partitioned_delete` call `note_rewritten`, so pgwire never learns them, and each execution's rendered `WHERE` routes on its own values (I11); a prepared `INSERT` routes each row as the text `INSERT` does. |
| INSERT | after defaults, generated values and CHECKs (`executor.rs:9917`), from the final `RowWrite`; `RETURNING` is built from the payload the partition stores (§9) |
| INSERT … ON CONFLICT | **As built (S18):** `check_conflict_target_infers_a_key` and `check_partitioned_conflict_target` at the statement's first dispatch (42P10 before anything runs); the insert routes as above; `DO UPDATE` re-dispatches its generated `UPDATE … WHERE <target> = <literal>` through the UPDATE hook, which routes it to the inserted row's partition (§9) |
| UPDATE / DELETE | `executor.rs:10318` / `:10737`; **as built:** `execute_partitioned_update` / `execute_partitioned_delete`, which resolve the `RETURNING` list (`check_partitioned_write`) and, for an `UPDATE`, prove its key assignments keep the point (`assignments_keep_point`, §4.5) before routing, then `write_partitions` (§9) |
| Joins | `execute_join`'s relation loop (`executor.rs:24`): each relation's pushed-down predicate routes it. **As built (S19):** the loop resolves each relation through `Database::relation`; a partitioned one is read by `RowSource::partitioned_with`, which plans its fan-out (`FanOut::plan_with`) under the `Visibility` the join already holds, so every relation and partition, a self-join's two sides included, reads under one snapshot, scoped or not. Each routes on its own pushed-down predicate, the exact text its scans evaluate, through `route_where` (`extract_gated`, never the path-aware variant). No transitive inference: `t JOIN d ON t.k = d.k WHERE d.k = 5` fans out on `t`, correct and slower (future work). |
| UNION, CTEs, `IN (SELECT …)` | re-dispatched as SQL text (`substitute_in_subqueries`, `executor.rs:645`) |
| `$n` | already inlined as literal text (`pg_params.rs:97`) |

### 4.2 The algorithm

1. If `sarg::clause_is_constant_false` (`sarg.rs:665`) holds, route to **zero**
   partitions: the only zero exit, a proof the scan already trusts.
2. Run `sarg::extract_gated(tokens, |wo| key_wos.contains(wo), &scales)`
   (`sarg.rs:473`), shaped like `point_update_stripe` (`table.rs:2162`); `None`
   means all partitions. Use this entry point, not `extract_gated_with_paths`.
   The path-aware variant gives `doc->>'k'` predicates virtual write_orders
   counting down from `u32::MAX - 1` (`index::FIRST_PATH_COLUMN`). The gate
   above already rejects them, but any code that walks the predicates must
   treat a write_order that is not a real field as unconstrained.
3. Fold the `MaskExpr` (`sarg.rs:66`) into **boxes**, one optional value set
   per key level (unset is unconstrained). The match is an allowlist; any
   variant it does not name widens.

   | `MaskExpr` | Boxes |
   |---|---|
   | `Pred { op: Eq, wo: level i, value }` | one box, level i = `{value}` |
   | any other `Pred`, or a predicate on a non-key column | one box, all levels unconstrained |
   | `All`, or any unlisted variant | one box, all levels unconstrained |
   | `Or` whose every branch is `Pred(Eq)` on one key level (a lowered `IN`) | **one** box, that level = the set of values |
   | `And(children)` | the product of the children's boxes. Two sets for one level are **unioned, not intersected**: an empty intersection would route to nothing without proof, and would hide an evaluation error the scan would raise. |
   | `Or(children)`, otherwise | the union of the children's boxes |

4. Check the 256-point cap (§1b) **as the fold goes**: when a partial `And`
   product, a union, or one box's level-set product would exceed it, degrade
   right-to-left at once, in the limit dropping level 0 (the whole space).
   Memory stays O(cap × levels).
5. Expand each box's leading constrained levels into point intervals (level 0
   unconstrained covers everything).
6. Binary-search the pinned map for overlapping partitions; union them.

`!=` (no `SargOp`), `NOT`, `LIKE`, functions, casts (`tenant_id::text = '5'`),
`OR` across columns, and `id = 1.5` on an integer key (the evaluator compares
floats; `coerce_literal` refuses it) all widen. Fan-out is always correct, only
slower.

### 4.3 Skipping without opening

A partition without `MATERIALIZE` is **known empty** and skipped unopened, by
proof, never by row-count estimate: per §4.4 the flag is set before the first
row is written, the writer commits after, a reader seeing the row snapshotted
after the commit, and it reads the flag after its snapshot. A resident bitmap
of materialized partitions makes the skip O(1). A bit is set at `MATERIALIZE`
and cleared only when the pid is retired, never
because vacuum found no live rows, since nothing proves that survives a
concurrent insert.

### 4.4 Snapshot first, then the map

Every statement, each in an explicit transaction included, (1) takes its
`Visibility` (`Table::visibility_for`, `table.rs:1065`); (2) loads the map epoch
with `ArcSwap::load_full`, holding a counted `Arc<MapEpoch>` (a plain `load`
guard is invisible to the strong count reclamation waits on, §6.2), and reads
the materialized flags; (3) holds both throughout, the `Visibility` registered
until the last partition call returns. Reversed, rows are lost: a statement
pins epoch E, a reshape flips to E+1, a writer inserts into an E+1 target and
commits, and a snapshot taken after that commit sees the row while the
statement reads the E source, which never had it. With the snapshot first this
cannot happen: any write to an E+1 target happened after the flip, so it
committed after any snapshot taken before the flip. Swapping the steps must fail
I7.

**Single-partition writes keep today's behaviour**: an `UPDATE` or `DELETE`
without foreign keys passes no `Visibility` (`executor.rs:10416-10422`), so the
table snapshots under `write_lock` (`table.rs:3343`, `:3649`) and a queued
writer sees the previous one's committed result instead of raising 40001. A
one-partition write does the same under that partition's `write_lock`, with the
slot's `write_gate` held shared, which still precedes any write to an E+1
target of that range because a reshape's fence waits for the gate.
Multi-partition statements use the one `Visibility`, so fan-out writes raise
more 40001s than on a plain table.

### 4.5 The range guard

Every partition carries `(KeySpec, lo, hi)`; the guard is on the row, not a
branch:

- **`insert_row_in_txn`** (`table.rs:2829`) errors if the recomputed point lies
  outside `[lo, hi)`.
- **`update_rows_locked`** recomputes the point from the merged `row_write`
  once assignments and expressions apply (`table.rs:3750-3856`), before
  `check_unique` or any branch, and fails with 0A000 if it changed. Two
  branches never relocate: the fixed-width fast path (`write_columns_in_place`,
  `table.rs:1548`, taken at `:3925-3931` whenever no other transaction runs, as
  in a quiet-server batch job or migration) and the in-place overwrite
  (`relocated == None`, `:3991`). Guarding only the relocating branch would let
  `SET tenant_id = 43` overwrite a row in tenant 42's partition, report
  success, and hide it from routed queries. Every `UPDATE`, the point-update
  fast path and `ON CONFLICT DO UPDATE` (`apply_conflict_update`) included,
  reaches this function.
- **Reshape raw-copy and delta writes** check the target's `[lo, hi)`.

**Phase 2a also refuses before writing.** The guard checks rows as a
statement writes them, so on its own it refuses a fan-out `UPDATE` only after
earlier partitions have written new versions (invisible once the statement's
transaction aborts, but written). So an `UPDATE` on a partitioned table that
assigns a key column is refused up front, with the same 0A000, unless every
key column it assigns gets a constant that a top-level conjunct `key = v` of
its `WHERE` already pins, compared by level hash
(`PartitionedTable::assignments_keep_point`): `SET tenant_id = 42 … WHERE
tenant_id = 42 AND id = 7`, an ORM's every-column `UPDATE`, passes; an
expression on a key column, or a constant nothing pins, does not, even where
no matched row would change. Stricter than the guard, which stays.

**In-place eligibility, as built (97e187f).** An in-place overwrite cannot be
undone by the statement's abort, so a partition's `update_rows_locked` keeps a
statement off it whenever the guard might refuse a later row after an earlier
one was overwritten. It decides that from the same proof, a free function over
the guard's `KeySpec` and the schema
(`partition::table::assignments_keep_point`, which the executor's
`PartitionedTable::assignments_keep_point` calls too): **a same-value key
assignment pinned by the `WHERE` (`SET tenant_id = 1, n = 42 WHERE tenant_id
= 1 AND …`, `ON CONFLICT`'s `SET tenant_id = EXCLUDED.tenant_id`) is eligible
for the in-place update, like a plain table**, and writes no more versions
than its twin. A key assignment the `WHERE` does not pin, or one an expression
computes, writes new versions only; the guard still checks every merged row;
a multi-partition fan-out passes a `Visibility` and so never overwrites in
place (§9). Before the fix every key-assigning statement wrote a version the
plain table overwrote, one dead slot per row per statement, which the twin
saw in `reltuples` and `n_dead_tup`. Tests: `tests/partition_write.rs`'s
`a_same_value_key_assignment_writes_no_more_versions_than_the_twin` (the same
`reltuples`, `n_live_tup` and `n_dead_tup` as the twin after routed and
`ON CONFLICT` same-value assignments, the twin's dead count 0) and
`a_genuine_key_change_is_refused_before_any_partition_write` (a statement
keeping one row's point and moving another's is refused with no in-place
overwrite and nothing changed; the pinned form overwrites in place once).

The rule is "the point is unchanged", which depends only on the frozen hash so
every node agrees, not "the row stays in this partition", which depends on this
node's count and sub-splits and could pass on the primary but fail on a
replica (§12). It runs in release builds, not as a `debug_assert`, at one
XXH64 per key level per written row, so a routing or reshape bug fails a
statement rather than storing a row no query finds. With agreement (I2), the
superset rule (I3) and snapshot-first (I7), the guard (I4) is why routing
cannot silently miss a row; the twin test (I6) checks all four.

## 5. Concurrency and contention

**Per partition**, so unshared across partitions: files, mmaps, `.tmp`
staging; `write_lock`, taken exclusively by `DELETE`, general `UPDATE` and the
aborted-write sweep ([MVCC](mvcc.md), "Which writers take turns"); the 64
`unique_lock` stripes and `compaction_gate`; free space, reader epochs,
`dead_stamps`, cache scope and generation; filters, indexes, vacuum. `INSERT`
and point `SELECT` never took `write_lock`: expect parity, not a gain.

**Still shared, under any partition design:**

| Shared point | Per statement | Held across I/O | Fix, cheapest first |
|---|---|---|---|
| `TransactionManager` (`core/mvcc/transaction.rs:296`) | ~3 write-lock takes (begin, `mark_wrote`, commit), a running-set copy per snapshot (`:1058`), 2 registry-mutex trips; SELECTs included | no | a lock-free running set or epoch snapshots: deep, a separate design |
| `StatusLog` mutex around the **root** WAL `writev` (`core/mvcc/status.rs:662-703`) | one per writing commit | `writev` | stage bytes under the mutex, append outside (small). Per-disk outcome logs need cross-log commit ordering; not proposed. |
| One WAL per directory: `writer` across `writev`, `gate` across `fdatasync` | every partition on a disk | yes, deliberately | WAL shards keyed `(dir, shard)`, each registered by recovery; never a log per partition (an fsync each) |
| `Database::sync_durable` syncs **every** pool path (`database/mod.rs:1027-1075`) | a commit pays another disk's fsync debt | yes | sync only logs this transaction touched, root last; required before phase 4 |
| `RowLocks::release_all` | two global mutexes per commit | no | atomic fast path at `row_count == 0`; cheap, independent of this work |
| The query-stats ring | one mutex | no | shard it |
| `Arc<PartitionedTable>` refcount, epoch `load_full` | one `fetch_add` each on one cache line per logical table | no | borrow from the map guard |
| Per-logical-table DDL lock (§11), new | one shared acquire, an atomic RMW | across the statement, never blocking one | none; reader-preferring, so DDL waits, never statements. **Superseded** by persistent-indexes §7's one table-lock manager: partitioned DDL takes ACCESS EXCLUSIVE on the logical name (§11) |
| WAL checkpoint sync pass and keep-tail (`core/wal.rs:1328-1347`, `:1418-1455`) | sequential `sync_data` per dirty file, about 2.3 ms each on .7's disk; then the tail is copied, fsynced and renamed **under the writer lock** | yes | parallelise the pass (bounded threads) or `syncfs` past a threshold; rotate segments instead of copying the tail |

Only the checkpoint row worsens with partitioning: about 3 dirty files per
active partition instead of per table, so 1000 active partitions mean a
multi-second pass and a large tail copied under the lock, stalling every writer
in the location. Phase 2a measures it on ext4 (tmpfs hides it). Each cheap fix
is done when a phase 2 profile names it; the deep ones are not proposed, and
nothing here depends on or rules them out.

## 6. Disk balancing and moving partitions

### 6.1 Placement

**An unwritten partition has no location.** Most of 1024 may stay empty for
months; placing at `CREATE` would balance map entries rather than data, give a
later disk nothing until a rebalance, and aim a new tenant's first write at a
since-filled disk. The location is chosen **at materialization**, recorded in
`MATERIALIZE` (already the durability point), among enabled primaries then
above `min_free_bytes`, by weight (and available bytes under `free-space`).
**The dealer is cross-table**: it picks the location furthest below its share
in materialized count and `STATS` bytes **summed over every partitioned
table**, ties to the lowest index, so small tables cannot pile onto one disk;
never `choose`'s argmax (`storage/mod.rs:1314`), which sends every new
partition to the emptiest disk. Reshape targets use the same dealer, recorded
in `RESHAPE_COMMIT`. A new location gets its share of later materializations
at once; recorded placement is never recomputed ([storage
pools](storage-pools.md)). Until phase 4, all is in root.

### 6.2 One primitive: reshape

Move, split, isolate (a three-way cut at the tenant's block edges), sub-split,
merge and conversion are all `reshape(sources → [(range, new_pid,
location)])`. **Every target is a new pid and every source is retired**, so no
live partition holds versions outside its range.

1. **Begin.** Append `RESHAPE_BEGIN`, fsync. Sources go append-only (no
   free-space reuse or in-place overwrite; vacuum, compaction and forget sweeps
   paused), so a source slot can change only its stamps, never point at
   another row.
2. **Copy, writers running.** Copy every record below `L0`, each source's
   pointer-file length, with its row, to the target holding `point(row)`:
   **raw** (`xmin`, `xmax`, `deleted` kept, as VACUUM FULL moves rows, no new
   MVCC transaction) and range-guarded. Uncommitted versions are copied, so a
   transaction idle between statements keeps its rows; waiting on
   `oldest_snapshot` to filter them would lose those (`settle_bound` tracks
   running transactions and snapshots separately, `transaction.rs:930`) and
   could starve behind a long transaction.
3. **Fence.** Take the source's `write_gate` exclusively; it belongs to the
   slot, not the `Table`, and every write path holds it shared. Writers drain;
   readers are unaffected.
4. **Delta.** Re-read every source record: rewrite changed ones below `L0`,
   copy those at or above it, each **routed by its own recomputed point**,
   never a remembered slot mapping; disagreement aborts. The fence lasts one
   pointer-file pass plus new rows, at about 40 B per record roughly a second
   for 10M rows (inferred; measured in phase 5). Very large partitions need a
   repeated delta: copy until it is small, then fence.
5. **Verify** per-range record counts against the source and a sampled
   checksum of row bytes; build the targets' filters and indexes.
6. **Durable.** `sync_durable` the targets' locations, append `RESHAPE_COMMIT`
   with the new entries and each target's forget pin (§7.2), fsync: **the commit
   point**.
7. **Flip.** Store epoch + 1, mark sources `Retired`, release the fence. A
   writer queued on a source gate wakes, sees `Retired`, and **re-routes its
   share against the new epoch**; it held nothing of the source, so the
   statement does not restart.

   **A re-routed visit is bounded to its share's points.** Each `UPDATE` or
   `DELETE` partition call carries the point ranges it owns (normally `[lo,
   hi)`); a re-routed call to T carries the retired source's ranges ∩ T's and
   skips, uncounted, matched rows outside them. Else, in a merge `S1 + S2 → T`,
   a statement that finished S1 before the fence would rescan in T the S1 rows
   the delta copied with its own new versions, which it sees
   (`core/mvcc/transaction.rs:246-258`; there are no command ids): `SET n = n +
   1` applies twice and count and `RETURNING` double, reporting success. Key
   assignment never moves a point (§4.5), so ranges stay disjoint across
   visits, two reshapes, or two sources into one target. Phase 6's row moves
   break that premise and need their own exclusion (§9).
8. **Reclaim** waits on epochs, not the `Table`: a statement clones a
   partition's `Arc<Table>` only on visiting it, 8 at a time, so it may yet
   visit retired source S while S's strong count is 1. The map keeps each
   superseded `Arc<MapEpoch>` where S was live, plus S's slot, and reclaims S
   (deletes its files, appends `RETIRED_GONE`) once all have strong count 1,
   held by that list alone. Until then E readers read S correctly; their
   snapshots predate the flip (§4.4).

Row locks key on logical name plus key bytes (`core/mvcc/row_locks.rs`), or
the framed tuple with composite identifying keys (§1b), so no reshape drops
them.

**No MVCC-mover fallback.** Delete-and-insert movers lose rows under
`REPEATABLE READ`: their inserts commit before the flip, a transaction whose
snapshot predates them and whose next statement loads E+1 cannot see them, and
the retired source holding its visible version is never read again. The raw
copy keeps `xmin` and `xmax`, so every snapshot sees in a target exactly what it
saw in the source.

### 6.3 Rebalancing

`REBALANCE PARTITIONS [DRY RUN]` plans from `STATS`, never `table_bytes` on an
unopened partition (§3.3). Moves run **in bounded parallel**, by default one
per location and at most 4, on disjoint sources with their own fences and
serialised flips, so adding a disk under 10⁴ partitions is not hours of
sequential copies. The first failure stops new moves; a re-plan resumes.
Partitions never use `move_table_to_backend`, which has no write fence: a writer
holding the old `Arc` can write past the copy into files about to be deleted.

## 7. Scale to many partitions

### 7.1 Resident cost

| State | fds | VMAs | Heap | Tasks |
|---|---|---|---|---|
| Unmaterialized | 0 | 0 | one map entry, ~16 B | 0 |
| Closed | 0 | 0 | one slot, ~64 B: state, location, pin, stats, last use | 0 |
| Open | 15 (3 providers × 5); 10 with persistent-index v1, which drops `_flt.db` | 3 | ~8 KB, plus filters and indexes in proportion to rows | 0 from phase 3 |
| A `Table` today | ~25 | 5 | 15–30 KB, plus filters and indexes | 3–4, ~5.6 wakeups/s idle |

**Startup** from phase 3 opens the shell and replays the map, nothing more; WAL
recovery still replays closed partitions, since `register_directory`
registers every file it finds (`core/wal.rs:1938`).

**The open budget**, at most `min(4096, (RLIMIT_NOFILE − 2048 − 25 × open
plain tables) / 15)` partitions per process across tables, is a **sharded
CLOCK** (no global lock on the hot path), scan-resistant (fan-out opens with
the reference bit clear). Provider open and map return `Result` on the partition path,
never panic; on EMFILE the budget evicts and retries once, then fails the
statement with `53000` (insufficient resources), and replica apply treats
`53xxx` as transient and retries, never skips (§12). **A write is never refused for
the budget**, since success must not depend on this node's `RLIMIT_NOFILE` or
open count (§12); past it, an open evicts and waits. Persistent-index v1
removes the `_flt.db` provider (its filter becomes a snapshot section) and
holds no fd for its own files while a partition is open, so from its phase 1
the divisor drops to 10 (two providers × 5), plus 5 fds per location directory
for `rdb.ixe` (persistent-indexes §1); its v2 would add one per open
partition (divisor 11). Sharding sizes `LimitNOFILE` for that (sharding §11.1)
and asks for a resident-bytes term beside the fd term
(`partition_resident_budget`, sharding §11.1, §16 phase 0), not yet designed
here beyond its measure: Σ of persistent-indexes' O(1) `resident_bytes()`
(hook C11) plus filter bytes plus in-flight load staging, evicted against
`machine budget ÷ processes_per_machine` (persistent-indexes §9).

**Phase 2 has no idle close** and opens every materialized partition at
startup, so it caps declared partitions at 2,048 per database, across its
tables (about 31k fds when all are written), checked by `CREATE`: a constant decides alike on
every node, unlike an `RLIMIT`-derived budget, and no write hits it. Plain
tables open first; a table whose partitions cannot all open stays a shell,
erroring loudly (§3.3), and the rest of the database starts.

**Close writes nothing if `write_generation` did not move**, unless
persistent-index v1 has a snapshot to replace (missing, rejected by this open,
or invalidated by an audit) and every index is ready (persistent-indexes §3,
rule 2), so a version bump rebuilds a read-only partition once, not on every
reopen. **A fan-out open is scan-only** (filter read-only: under
persistent-index v1, the snapshot's FILTER section alone when the cheap proof
passes, else an unpruned scan; index rebuild deferred to the first routed
access or write, §7.2, point 4), so an unrouted `SELECT` wider than the budget
rewrites no O(P) checkpoints, feeds no map-log compaction, and starts no
rebuilds it then discards.

### 7.2 Lazy open and idle close

**1. The forget horizon.** Getting this wrong silently resurrects rows. Today
dropping a `ForgetHandle` leaves the minimum (`transaction.rs:412`), a reopen
registers at the current floor (`:938`), and ids below the forgotten horizon
read as **Committed** (`status.rs:1086`): a closed partition with an unswept
aborted stamp would stop pinning, and its aborted rows would return on reopen.
So the `PartitionedTable` holds **one** `ForgetHandle` at the minimum of its
partitions' **pins**:

- **A pin** means unswept aborted stamps may exist at or above it. It lives in
  the slot, so **a closed partition keeps pinning**: dirty partitions can close,
  and a long transaction cannot grow the open set without bound.
- **Clean** partitions pin nothing. A forget sweep makes one clean if it
  reached `swept_to = h`, saw only stamps below h, and `write_generation` did
  not move meanwhile.
- **After any sweep**, clean or not: once its writes are durable
  (`sync_durable` over the location), append `SWEPT(pid, h, clean)` and raise
  the pin to h.
- **First write after clean**: under the registry lock, before the first stamp,
  pin at `forgotten_below()`, sound since the writer's id ≥ `oldest_active` ≥
  the forgotten horizon. This needs `ForgetHandle::lower_to`, as `forget_below`
  never moves back (`status.rs:1588-1589`). The edge is also **durable**:
  append `DIRTY(pid, floor)` and wait for the root log. Log order alone fails,
  because an aborted writer has no outcome and kernel writeback can land its
  stamp with no record ordering it. Cost: one group-commit wait on a clean,
  hence cold, partition's first write.
- **After a restart**, a partition whose last of the three records is
  `MATERIALIZE`, `DIRTY` or an unclean `SWEPT` is pinned at its last `SWEPT` or
  `MATERIALIZE` floor until swept again; a clean `SWEPT` pins nothing. Sound:
  stamps below the pin were neutralized, later ones are newer, and nothing
  stamps after a clean `SWEPT` without a durable `DIRTY`. A lost `SWEPT` only
  pins lower, so it needs no fsync.
- **Restart cost**: forgetting waits until every partition dirty at the crash
  (the recently written set, not all materialized) is swept once, by
  pointer-file-only opens. At 2 bits per transaction the status bitmap grows
  about 18 MB per 72M transactions meanwhile (inferred).
- **Reshape targets** inherit the source's pin in `RESHAPE_COMMIT`, under
  `forget_pause`, before it is retired.

**2. Exclusive close.** `Open` → `Closing` only at the slot's `Arc<Table>`
strong count 1 (statements and scheduler jobs hold clones; newcomers wait on
the slot's `Notify`). With no per-partition tasks, one live instance per file
set is structural, unlike today's `reload_table`.

**3. Close** lands `.tmp` and, if written since open, writes the filter
checkpoint and `STATS`; one rejected as dirty is backfilled (correct, unpruned
meanwhile). From persistent-indexes phase 1 the filter checkpoint is that
design's clean-point snapshot (FILTER section only until its phase 4), trusted
only under its `DIRTY`/`CLEAN` marker log (persistent-indexes §3, §5); if this
phase is live first, persistent phase 1 ships this close hook with it, since
removing `_flt.db` without it would backfill every reopened partition's filter
from zero (persistent-indexes §12).

**4. Index rebuild on open** is O(partition rows) on the scheduler, affordable
at 1/N of the table; until ready, queries scan and unique checks use filter
plus scan, which is correct. Phase 3 ships with this rebuild.
[Persistent indexes](persistent-indexes.md#4-opening) v1 (its phase 4) skips it
for a partition closed clean: close (point 3) writes a snapshot of the indexes
and filter if the partition was written since open or its snapshot must be
replaced, and the next routed open starts loading it as soon as the proof
passes (marker log ends in `CLEAN`, identity matches the data), its readers
waiting up to `index_snapshot.load_wait` rather than scanning. Anything else
rebuilds exactly as here. A fan-out open stays scan-only and reads only the
snapshot's FILTER section, for pruning. Persistent-index v2, online
persistence for a hot set larger than RAM, is requirements only
(persistent-indexes §16).

**The WAL file map.** `Wal::register_file` silently overwrites a mapped id
(`wal.rs:488-490`) and checkpoints silently skip unresolvable ids
(`wal.rs:1326-1334`). So **close never unregisters** (about 100 B per file ever
opened), and **registering an id mapped to another path is a hard error** from
phase 0: at 10⁶ files a 64-bit FNV collision is small but not negligible, and
today it silently replays into the wrong file.

### 7.3 Shared maintenance

One scheduler per database (`MaintenanceMode::External`), driven by a dirty
queue that writes push rather than per-table timers, replaces each partition's
services, flush, filter and vacuum tasks. Jobs: landing `.tmp`; filter
checkpoints; vacuum (`vacuum::run_once`, `core/vacuum.rs:1314`, under the
`SweepScheduler` permits, `vacuum.rs:1857`); forget sweeps of pinned
partitions, cold ones round-robin at low priority; close preparation. Idle
partitions cost no wakeups: clean ones are no longer swept every 2²⁰ ids
(`vacuum.rs:1523-1532`).

### 7.4 Nothing O(P) per statement

- **Routing** is O(log P + k).
- **Database-wide walks stay O(logical tables)** (`referencing_keys`, catalog
  walks, `index_name_taken`), as partitions are not in `Database.tables`. A
  bare `VACUUM` is O(materialized partitions), the work it exists to do. `pg_class`, `SHOW TABLES` and `pg_stat_user_tables`,
  rebuilt per statement, read `PartitionedTable` running totals kept from
  `STATS` and live deltas, O(1) each. `referencing_keys` walked every
  table per `DELETE`; since phase 3 P4 it reads `Database.fk_reverse`, an
  index maintained with the slots, for all tables ([as built](#phase-3-p4-as-built)).
- **`schema_generation`** moves once per logical DDL, never on materialize,
  close or reshape; `add_table` bumps it for non-internal names
  (`database/mod.rs:2094`), and partitions bypass `add_table`.

**Literal one-subtable-per-tenant scale.** 10⁵ partitions × 3 files × 2 is 600k
directory entries in one location (a lazy `.tmp` halves it). There
`register_directory`'s startup `read_dir`, backup snapshot directory walks, and
post-burst checkpoints (§5) grow from milliseconds to seconds; the phase that
raises N measures them.

## 8. Multi-partition queries

**One snapshot** (§4.4) needs new entry points: only
`query_row_with_visibility` (`table.rs:3010`) and the write paths accept a
`Visibility`, so `count_rows_in_txn` (`:2944`) and `query_row_bytes_in_txn`
(`:3133`) gain `_with_visibility` variants delegating as `query_row_in_txn`
(`:2981`) does. Otherwise a READ COMMITTED fan-out could see a cross-tenant
transfer half applied (I7).

**As built in 2a (S10):** `FANOUT_IN_FLIGHT = 8` partitions are read at once
(`core/rql/row_source.rs`), each a whole scan, in pid order, before the next
group of 8 starts, so no partition future is dropped mid-scan; the
`_with_visibility` variants take **no** worker cap yet, so each partition's
scan still starts its own `min(threads, batches)` workers. The statement-wide
budget below is S12. `ORDER BY` across partitions asks each partition for its
sorted top `k` and sorts the union with the scan's own comparator
(`ConcurrentProcessor::sort_rows_for_merge`), decoding every row: correct, not
cheap, until S12's merge. Writes (S11) visit partitions one at a time (§9).
Phase 2b added no worker budget either: S12 waits on the scan-concurrency
owner, and the fan-out width is still `FANOUT_IN_FLIGHT`.

**Phase 2 fan-out** keeps at most 8 partitions in flight, with workers from
**one statement budget** of `max_permits_threads()`, so the variants take a
worker cap: each call now starts `min(threads, batches)` workers itself
(`core/processor/concurrent_processor.rs:904`), up to all of them per large
partition. A single-partition statement calls straight through: a tenant-scoped point
read must cost no more than an unpartitioned one plus one route. Phase 6's `process_many` runs
`spawn_batch_tasks` per partition on one channel with a shared
`limit_counter`, `stop`, `eval_error`, `cancel`, `count_only` and `TopN`.

| Operator | Across partitions |
|---|---|
| `COUNT(*)` | sum the counts, then `row_count_result` (`core/rql/aggregate.rs:584`) |
| Aggregates, GROUP BY, HAVING, FILTER | phase 2: concatenate and fold once. Later: a `Folder` per partition and `Folder::merge` (`aggregate.rs:1634`), keeping AVG as `(total, n)` |
| LIMIT without ORDER BY | stop launching partitions once enough rows exist, then truncate |
| ORDER BY … LIMIT k OFFSET o | each partition returns its sorted top `k + o` (`QueryRows.limit` already includes OFFSET); concatenate and re-cut with `sort_rows_if_needed` (`concurrent_processor.rs:1800`, made `pub(crate)`) |
| ORDER BY without LIMIT, DISTINCT, windows, expression sorts | gather, then the existing post-scan pipeline; valid because every partition's schema is a clone of one, so `write_order`s agree |
| Prepared SELECT and COUNT | **As built (S15, S16b):** the learned parse, read through `read_partitioned` like the text path, routed per execution (§4.1, I11). `run_statement` runs it inside `statement_snapshot::scope`, so a fan-out takes one snapshot on the fast path too (I7); a full-key point read names its partition from the bound key (`RoutePlan`, `keyed_range`), snapshot first; a one-partition read is awaited in place |
| Joins | concatenated bytes in one `RowSet::Serialized` (`core/rql/join.rs:331`); the 4 GiB cap (`join.rs:343`) applies to the sum, and still errors. **As built (S19):** every relation and partition, a self-join's two sides included, under the join's one `Visibility` (`FanOut::plan_with`), each relation routed on its own pushed-down predicate (§4.1), with no transitive inference across `ON` (a key pinned only on the other side fans out: correct, slower). A prepared join takes the scoped text path. `UPDATE … FROM`/`DELETE … USING` read a partitioned source through the same join, and only a plain target is written back; a partitioned target is refused (phase 3) |
| FOR UPDATE, SKIP LOCKED | refused until composite identifying keys (§1b); then gathered |

**An empty route still runs the pipeline.** A constant-false clause, or only
unmaterialized partitions (a new tenant's first `SELECT COUNT(*) … WHERE
tenant_id = 42`), runs the post-scan pipeline over an empty source:
`row_count_result(0)` and a `Folder` finished with no input, so `COUNT(*)` gives
one row of 0 and `MAX(x)` one row of NULL, as PostgreSQL and the twin do. "No
partitions, so no rows" is a silent wrong answer for every aggregate without
`GROUP BY`.

**Query cache.** Each partition caches its share under its own scope and
generation; pids are never reused, so a scope never returns meaning other rows.
No cached result spans partitions, and only statements routing to 4 or fewer
are admitted, so a wide fan-out cannot flush the cache.

**Unrouted fan-out cost is unmeasured.** The code's one figure, `COUNT(*)` on a
one-row table at about 0.46 ms against 0.15 ms for `SELECT 1`
(`concurrent_processor.rs:875`), predates the `min(threads, batches)` cap and no
longer holds. Per partition remain the visibility, `build_candidates`, one
worker, and an open if closed. Phase 2a measures fan-out over 1024 one-row
partitions and phase 3 the open cost, before default N is fixed. Such queries
show in `partitions_touched` (`pg_stat_statements` and the statement ring), a
`partition_fanout_warn` counter (default: over 256 partitions), and
`partition_trace`.

## 9. Writes

- **INSERT** routes from the final `RowWrite`, materializes if needed (placing,
  §6.1), takes the `write_gate` shared, re-checks the epoch, and runs
  `insert_row_in_txn`, whose in-partition unique check under its stripes is
  exact by §1b. Multi-row `INSERT` already re-dispatches each tuple
  (`insert_values_batch`, `executor.rs:5661`); `INSERT … SELECT` routes each
  row.
- **UPDATE and DELETE** visit routed partitions in ascending pid order, with one
  `Visibility` if several (§4.4), each call bounded to its point ranges (§6.2,
  step 7) and taking only its own `write_lock`, none held across partitions, so
  no lock-order deadlock. `point_update_stripe` works unchanged in the one
  partition a fully keyed `WHERE` reaches. Counts sum; RETURNING runs
  `returning_rows_for_write` over the same route and `Visibility` and
  concatenates.
- **Conflicts.** A partition's conflict is retryable only if it applied
  nothing, the statement's only if no partition did; otherwise 40001,
  non-retryable. Both retry loops honour this: autocommit's and
  `execute_in_txn`'s (`executor.rs:9445`), the one ORM traffic uses. A failed
  statement in `BEGIN` poisons the block (25P02), so no partial apply commits.
- **As built in 2a (S11)** (`rql::row_source::write_partitions`): the
  statement's `Visibility` is taken first, then the epoch, the route and the
  materialized flags (§4.4). One partition: its `update_rows_as_of` or
  `delete_rows_as_of` with no `Visibility`, so a plain table's behaviour
  exactly (snapshot under `write_lock`, the point-update path, the in-place
  fast path). Several: one at a time in ascending pid order, every one under
  the statement's one `Visibility` and transaction id; passing a `Visibility`
  also keeps them off the in-place overwrite, so a later partition's failure
  leaves only versions the transaction's abort retires. A statement therefore
  commits or aborts across its partitions as one transaction, and a crash
  before its outcome is durable leaves none of it. The retry marker is
  stripped from a partition's error once an earlier partition applied rows
  (I17). A prepared `UPDATE` or `DELETE` is
  marked rewritten (`note_rewritten`), as a prepared `SELECT` is, so each
  execution takes the text path and routes its own values (I11).
- **`RETURNING`, as built in 2b (S17).** `execute_partitioned_update` and
  `execute_partitioned_delete` resolve the list against the shell's schema
  before anything is written (`check_partitioned_write`; a bad list fails as
  on a plain table), pass `collect` to `write_partitions`, and build the rows
  only after every partition succeeded (`partitioned_write_reply`):
  `returning_rows_for_write` over the shell, the collected pre-images, and,
  for an `UPDATE`, the assignments cloned before the write consumed them and
  the per-row expressions, exactly as the plain arms do. The pre-images were
  read by the write itself, under the statement's one `Visibility` (several
  partitions) or the partition's `write_lock` (one), and are never read
  again, so no row is reported that the statement did not change (memory
  `one-snapshot-per-statement`). Rows concatenate in pid order; PostgreSQL
  gives none without `ORDER BY`, and the twin compares them as multisets. An
  empty route returns zero rows with the row description. A failed statement
  returns its error and no rows: the partial `collect` is dropped with it.
  `INSERT … RETURNING` needed only its refusal lifted
  (`refuse_partitioned_insert_clauses`, removed by S18): the
  returned row is built from the payload the partition stores, and the
  multi-row and `INSERT … SELECT` paths concatenate their per-row answers.
  pgwire's Describe and scale lookups already read the shell's schema.
  Tests: `row_source::returning_tests` (one snapshot under a writer
  committing between partition visits, mutation a snapshot per partition;
  unset columns reported as stored, the overlay trap, mutation pre-images
  without their text columns; a third-partition failure returns nothing and
  changes nothing, mutation the partial rows returned on error; I17 with
  `RETURNING`), `tests/partition_returning.rs` (every statement shape against
  the twin with NULLs, JSONB, `NUMERIC(10,2)`, a default, a generated column
  and a `BIGSERIAL`, blocks, prepared; failures change nothing; Describe of
  `RETURNING id AS v, n * 2`, mutation the overlay skipped; a prepared
  `UPDATE … RETURNING` across writes and a restart, mutation the assignment
  expressions dropped), and the twin's `InsertReturning`, `UpdateReturning`,
  `UpdateReturningExpr`, `DeleteReturning`, `DeleteReturningFanOut`,
  `PreparedUpdateReturning` and the empty-route kinds.
- **Assigning a key column** with the point unchanged is ordinary, as it must
  be: ORMs assign every column, usually its current value. Pinned by the
  `WHERE`, it is eligible for the in-place overwrite exactly as on a plain
  table (§4.5, as built in 97e187f). Any other key change
  fails the whole statement at the guard (§4.5, which also says why the rule is
  not "stays in its partition") with 0A000 ("changing a partition key value is
  not supported yet"). Phase 6 adds the move, allowing every key change:
  retire the row in the source under its locks, release them, insert into the
  target under its stripes, in one transaction; first-updater-wins guards
  concurrent movers, RETURNING shows the new row. A moved version has a new
  point and the mover's `xmin`, so a later visit of the same statement would
  match it again; phase 6 must design a per-statement record of versions
  written.
- **ON CONFLICT, as built in 2b (S18).** The target is a unique key, so
  contains the partition key: probe, insert and follow-up update all hit the
  inserted row's partition, whose own uniqueness check, the in-flight-writer
  rule of `find_row_with_value` / `find_row_with_tuple` included, decides the
  conflict exactly. `apply_conflict_update` runs unchanged: its generated
  `UPDATE … WHERE <every target column> = <literal>` routes through
  `execute_partitioned_update` to that one partition. Targetless `DO NOTHING`
  is covered too, as every unique key contains the partition key.
  - A target (`DO NOTHING`'s or `DO UPDATE`'s) that leaves out a key column
    matches no unique key, so it is refused at the statement's first dispatch,
    before anything runs, with PostgreSQL's 42P10 ("there is no unique or
    exclusion constraint matching the ON CONFLICT specification";
    `check_partitioned_conflict_target`, which replaced
    `refuse_partitioned_insert_clauses`). The parser now keeps `DO NOTHING`'s
    target (`InsertRowData::on_conflict_target`) for it. Since 1c421c4 a
    target must name exactly one unique key's columns, in any order, as
    PostgreSQL infers its arbiter, on plain tables too
    (`check_conflict_target_infers_a_key`): a subset or a superset of a key is
    42P10. Before it, `ON CONFLICT (tenant_id) DO UPDATE` over a key on
    `(tenant_id, grp, id)` updated every row of the tenant, on both kinds of
    table, so the twin could not see it.
  - A multi-row `DO UPDATE` proposing one key twice is refused with 21000
    before any row is written (1c421c4, plain tables too). Keys are compared
    as written, so `1` and `'1'` are a known miss, never a false refusal.
  - `ON CONFLICT ON CONSTRAINT name DO UPDATE` resolves the name to the
    constraint's columns (`constraint_columns`: a table-level key such as
    `<t>_pkey`, a single-column key, or an inline `UNIQUE` by its default
    name), on plain tables too; an unknown name is 42704. Before S18 the name
    was ignored, so a composite key could not be found at all.
  - A `DO UPDATE` assigning a key column a value that moves the point
    (`SET tenant_id = EXCLUDED.tenant_id + 1`, `SET tenant_id = 77`) is a key
    change: `assignments_keep_point` refuses it with 0A000 (phase 6) when the
    conflict happens, before the update writes. PostgreSQL refuses the
    cross-partition case with 0A000 too ("invalid ON UPDATE specification").
    `SET tenant_id = EXCLUDED.tenant_id` substitutes the literal the `WHERE`
    pins, so it keeps the point and, like any same-value key assignment
    the `WHERE` pins, overwrites in place as a plain table does (§4.5); an
    expression is not proven to, and is refused, as in a plain `UPDATE`.
    A multi-row statement whose earlier
    tuples landed fails whole, as any failed statement does.
  - Multi-row upserts across partitions re-dispatch per tuple
    (`insert_values_batch`), with the clause restored on each and the tag
    counting rows. `DO NOTHING … RETURNING` that skips a row answers an empty
    row set rather than a bare count, so the RowDescription is sent (on plain
    tables too), and a tuple's empty `RETURNING` reply no longer strips the
    retry marker, which only a tuple that wrote a row may do (I17).
  - Tests: `tests/partition_upsert.rs` (every shape against the twin; the
    multi-row clause and count, mutation the clause not restored per tuple;
    the key-changing `DO UPDATE` refused with nothing stored, in a block and
    prepared; a rolled-back block leaves nothing; 42P10, 42704 and prepared
    upserts), `tests/partition_insert.rs` (42P10 before any write in every
    shape), `rasterizeddb_compat/tests/partition_upsert_matrix.rs` (sixteen
    clients over the primary key and a composite UNIQUE in every transaction
    shape, and 300 keys over every partition from four connections, mutation
    the in-flight rule off in `find_row_with_tuple`: 848 rows for 300 keys),
    and the twin's ON CONFLICT kinds, `InsertOnConflictSubsetTarget` (42P10)
    and `InsertOnConflictUpdateTwice` (21000) among them.
- **Replication capture** (`note_write`, `executor.rs:9383`) records the
  logical name.

## 10. Constraints

- **UNIQUE and PRIMARY KEY**: per partition, global by §1b's rule, which
  `ALTER TABLE ADD UNIQUE` and `CREATE UNIQUE INDEX` check before validating
  each partition.
- **Foreign keys** (built in phase 3 P4, [as built](#phase-3-p4-as-built)).
  A partitioned child's keys live in the shell's FK sidecar like any other
  declaration, so every partition shares them, and `fk_reverse` files them
  under their parent. Referencing a partitioned table needs a unique key on
  the referenced column, which holds every partition key column (§1b); a
  key is one column, so it is the whole partition key, and every existence
  check and referenced-key lock is by that key: each check routes to the
  one partition its value hashes to (a check that would not route is
  refused, never fanned out or skipped), and the lock is taken under the
  parent's logical name on the stored key bytes, which a parent `DELETE`
  locks too. The child side of a parent's change (does any child reference
  these keys; the action) reads or writes the child through its fan-out,
  routed when the key column is the child's partition key; actions run as
  SQL through `execute_with_context`, which routes (I24).
- **SERIAL**: one sequence per logical table (`sequence_name(table, col)`,
  `core/rql/sequence.rs:33`). Phase 2, with every materialized partition open,
  seeds it once per process from a `MAX` over them (`seed_sequences`); that
  `MAX` is unroutable, so under lazy open the first post-restart insert would
  open and scan every partition. From phase 3 the high-water is reserved
  durably in `__pt_<uid>_seq.db` in steps, like row ids (§3.5), never from
  `MAX`; a crash leaves a gap, as in PostgreSQL.
- **CHECK, DEFAULT and generated columns** live on the shared sidecars and run
  before routing, so a `DEFAULT` on a key column works.

## 11. DDL fan-out

**One schema.** Every partition's `TableSchema` clones the shell's, so write
orders, dropped-column history and `__null_` companions match by construction,
and the "schema_id is a write_order" corruption class cannot occur between
partitions.

**Metadata-only DDL** (rename column, defaults, CHECKs, `CREATE`/`DROP INDEX`,
marking `DROP COLUMN`, table `RENAME`) reopens partitions, safe only once
`MaintenanceMode::External` and exclusive close exist, so it ships in **phase
3**: in phase 2 each partition's own tasks hold provider clones and
write until they notice `alive = false` (`table.rs:239-244`), so a reopen would
put two live instances on one file set, `reload_table`'s hazard times the
partition count. Steps: (1) write the shell's sidecars once; (2) rebuild the
shell's schema in place, never via `reload_table` (§3.3); (3) bump
`schema_epoch`; (4) exclusively close and reopen each **open** partition with
the new clone, O(open partitions) and never O(P), with an index rebuild each,
which a new index needs anyway; closed ones take it at open. Statements hold the table's DDL lock shared from routing
to their last partition call and step 3 holds it exclusively, so none mixes
epochs. **It is reader-preferring, and DDL try-acquires it** against
`lock_timeout`, failing with `55P03`: statements never queue behind DDL, so one
long fan-out cannot stall every tenant, and no wait cycle forms through a
statement holding it while waiting on a row lock (the row-lock deadlock detector
sees only transaction edges, `core/mvcc/row_locks.rs:607-635`). The cost: DDL
can starve under load and must be retried.

**Superseded (phase 2b plan, 2026-10-24).** Phase 3 does not build this
lock: there is one table-lock manager, persistent-indexes §7's (PostgreSQL's
modes and grant rule, one wait-for graph), and metadata DDL on a partitioned
table takes ACCESS EXCLUSIVE on the **logical** name, never on a partition's
physical name; reopening the partitions is the reload's work under that lock.
Two managers would need a lock order and two graphs, with cycles across them
invisible. The cost is PostgreSQL's queueing: a pending ACCESS EXCLUSIVE
queues new statements on the table, so a long fan-out can stall its tenants
behind an `ALTER`. Whether phase 3 accepts that or try-acquires with `55P03`
is open; nothing in 2b depends on it. I26 is re-specified on that manager when
phase 3 is planned.

**As built (phase 3 P1).** The superseded paragraph is what was built,
with PostgreSQL's queueing (no try-acquire): metadata DDL takes ACCESS
EXCLUSIVE on the logical name, statements lock that name before any
partition, and the reload (`reload_partitioned`) retires every open
partition, writes the shell's sidecars once, and reopens every open
partition under the one schema with its indexes rebuilt, all under the
lock; no `MaintenanceMode::External` or exclusive close was needed, since
retiring a partition stops and joins its maintenance and no statement holds
it. Steps (1)-(4) above hold, with `schema_epoch` read as the shape epoch
(moved only when the columns are) and `schema_generation` (moved once).
Details, the refusals left and the crash table: [Phase 3 P1 as
built](#phase-3-p1-as-built).

**Physical-rewrite DDL** (populated `ADD COLUMN`, `ALTER … TYPE`, rewriting
`DROP COLUMN`; `rewrite_table_with_columns`, `executor.rs:2125`;
`retype_column`, `:3068`) is refused until phase 6. It reads the whole table
into memory and recreates it; per partition it is not transactional, and a
crash midway leaves two layouts under one schema, silently corrupting data.
Phase 6 makes it a **generational reshape**: copy every partition to new pids
under the new schema, flip map and schema generation in one `RESHAPE_COMMIT`,
keep the old generation until then, at twice the disk meanwhile.

**Conversion** (`ALTER TABLE … PARTITION BY`, `CREATE INDEX … USING partition`)
is a reshape from the plain table, fencing writers only for the delta; it
commits at `RESHAPE_COMMIT`, then rewrites the catalogue row to `clusters =
uid`. At startup a map with `RESHAPE_COMMIT` and a row still at 0 rolls forward;
one with only `RESHAPE_BEGIN` rolls back, deleting the new files, in the shape
of `recover_interrupted_rename` (`database/mod.rs:2597`). DDL is not
transactional here, so every window resumes or refuses to open; none is half
applied silently.

## 12. Durability, recovery, backup, replication

| Operation | Order | A crash leaves |
|---|---|---|
| CREATE | (1) delete any orphan `__pt_<uid>_*`; (2) shell schema and sidecars, `HEADER`, `LOCATIONS` and `PLACE`, fsynced; (3) the catalogue row with `clusters = uid`, **the commit point**; (4) insert into both maps | before (3): orphans with no catalogue row, which are reaped. After (3): a complete, empty table. |
| Materialize | §3.2 | files without `MATERIALIZE`: **deleted at startup** (below), never reused. `MATERIALIZE` with files missing: **the table refuses to open** (I9). |
| Reshape | §6.2 | before `RESHAPE_COMMIT`: sources untouched (pausing only sets in-memory flags); replay deletes targets, appends `RESHAPE_ABORT`. After: replay deletes retired sources. |
| Rename, conversion | §3.1, §11 | resumes or rolls back |
| DROP | (1) tombstone the catalogue row, **the commit point**; (2) remove from both maps; (3) `DROPPED`; (4) partition files; (5) parent files last | files of a uid with no catalogue row, which are reaped |

`CREATE` avoids `add_table` (`database/mod.rs:2093`), which writes the row first
(`:2105-2188`) and never rolls back, leaving a row with no map. Map recovery
runs after WAL recovery, before any partition opens.

**The reaper** touches only names matching
`^__pt_[0-9a-f]{16}_([0-9a-f]{8}(_pointers|_flt)?|schema(_[A-Z]+)?|map_[0-9]+|ids|seq)\.db(\.tmp)?$`,
and deletes one only if its uid has no catalogue row, its pid is retired or
lacks `MATERIALIZE`, it sits where its map does not place it, or it is a
non-live map generation (§3.2). Persistent indexes extend the pattern with
their per-partition files by a second pattern,
`^__pt_[0-9a-f]{16}_[0-9a-f]{8}\.(ixs|ixm)(\.tmp)?$` (persistent-indexes §1,
§10: no table file can end that way). It runs **only if every map parsed cleanly**;
one unreadable map disables reaping for that start, with a warning, so a
corrupt log never gets live files deleted.

**Files without `MATERIALIZE` are never reused.** Data files are written
through the page cache before their log records are durable, so writeback can
persist rows of a transaction whose outcome and `MATERIALIZE` were lost. The
partition is then unmaterialized (skipped, unpinned, never swept), the
forgotten horizon passes the aborted id X, and reusing the files with a floor
above X would read X's rows as committed (`core/mvcc/status.rs:1084-1087`). So
the reaper deletes them **before forgetting resumes**, and materializing deletes
leftovers again. Deleting is safe: a durable outcome implies a durable
`MATERIALIZE` (§3.2), so such files hold only versions no snapshot can see.

**S3 backup.** Snapshots walk only the database directory, and other locations'
logs are deliberately not streams (`core/backup/mod.rs:428-435`, `:1016`): root-located
partitions and their parent files are covered, but off-root partitions would be
**silently left out**, a gap plain tables on pool
disks already have. Refusing placement alone misses a backup enabled later, so
both: under `s3_backup`, off-root placement is refused with the reason; and the
backup service refuses to start, and each snapshot fails loudly, when
`Database::partition_locations_in_use()` reports a materialized partition
outside the database directory. Its only check today reads `db_storage.cfg`
(`warn_about_excluded_tables`, `core/backup/mod.rs:987-1010`), which never names
a partition, and only warns. Both lift in phase 4b.

**The cross-session ordering gap** (inferred, untested): session C syncs disk
D; A writes to D and stages its outcome in the root log; C's root sync makes A's
outcome durable before A's rows on D; a crash breaks A's all-or-nothing.
Spread partitions make multi-directory transactions normal, so phase 4 is
gated on a page-cache-drop crash test (SIGKILL cannot reproduce it) and the
fix: `stage()` writes only outcomes whose transactions finished their non-root
syncs.

**Replication.** Statements ship with logical names
(`replication/manager.rs:563-577`), and each replica routes with its own map.
`CREATE … PARTITION BY` replicates, with per-node partition counts. Conversion
replicates as logical DDL, each node using its own count, so the refusals it
adds (unique key without the key columns, `DROP NOT NULL`) match everywhere.
Reshapes are physical and local, like `STORAGE MOVE TABLE`. LWW `table_stamps`
stay per logical table. **Every refusal of a logical statement depends only on
replicated state**: replica apply skips a failed statement under `Skip` and
`LastWriterWins`, commits the rest and advances
(`replication/server.rs:413-428`), silently diverging the nodes, and under
`Abort` stalls; hence §4.5's rule and §7.1's never-refused writes. `53xxx`
resource failures are the one local failure left; replica apply must retry
them, which `server.rs` does not yet distinguish. The reseed path is unread; a
test must show a reseeded node returns identical rows.

## 13. Hot tenants

`ISOLATE KEY (tenant_id = 42) TO '/mnt/nvme3'` reshapes the tenant's block into
its own partition, as Citus's `isolate_tenant_to_new_shard` does (own files,
write lock, disk): the ceiling for a single-column key. `SPLIT KEY` spreads a
tenant over k partitions and up to k disks (§1b). Not provided: splitting one
atomic `(tenant, group)` value, and automatic splitting; `SHOW PARTITIONS`
(§14) and `partition_trace`, which samples the hottest level-0 hashes, find
candidates. An automatic isolate past a size
threshold could come later, one concurrent reshape at a time. **Dropping a
tenant** is a routed, MVCC, replicated `DELETE … WHERE tenant_id = 42`; a
physical drop of an isolated tenant would need a fence, a wait until no
snapshot or transaction predates it, and replication as the logical `DELETE`,
and is not designed here.

## 14. Observability

- **`pg_class`** lists the logical table as relkind `'r'`; its size and tuple
  counts are the running totals (§7.4). `'p'` would send psql and pg_dump to `pg_partitioned_table`,
  which `catalog_views.rs` lacks, and to children they cannot query.
- **`pg_inherits`** stays empty (`core/rql/catalog_views.rs:1122`).
- **`pg_index`, `pg_indexes`** list declared indexes from the shell's
  `schema.secondary_indexes`, not live `indexes` (§3.3), and omit the partition
  key.
- **Partition and parent files** start with `__`, so `is_internal`
  (`catalog_views.rs:1265`) hides them everywhere.
- **`SHOW TABLES`, `pg_stat_user_tables`**: running totals.
- **`SHOW PARTITIONS t`**, per partition: pid, `[lo, hi)`, location, state
  (unmaterialized, closed, open, fenced, retired), rows, bytes, dead stamps,
  writes per second, last use, pin, blocks shared by colliding tenants; plus map
  epoch, `route_v` and open-budget use.
- **Metrics:** partitions touched per statement, fan-out warnings, budget
  evictions, reopens within 10 s (thrash), p99 open latency, materializations
  per second, reaper deletions.

## 15. Phased plan

Each phase is safe alone and refuses what it does not support.

| Phase | Ships | Still refuses | Measures |
|---|---|---|---|
| **0** | Nesting-aware `CREATE TABLE` trailer scan, making `PARTITION BY`, `PARTITION OF`, `INHERITS`, `ATTACH`, `DETACH` explicit refusals; refusal of `__pt_` names; hard error on a `Wal::register_file` id collision | all partitioning | the three mis-parse shapes, each asserting no catalogue row is written |
| **1** | Pure, unwired modules: the hash (`partition::xxh64`, `partition::key`), `RangeMap`, the map-log codec, `route(MaskExpr)` | — | route ns/op; I1, I2-pure, I3, I5 |
| **2a** | **Built** (S1–S11; S12, the worker budget and cheap merge, not built, §8). **The `table.rs` seam (§3.4), one reviewed change:** private data fields, the shell, `open_partition`, the range guard on the merged row, `_with_visibility` variants (their worker cap is S12; 2a bounds partitions in flight with `FANOUT_IN_FLIGHT = 8`, §8), the shared id source.<br>**The table:** `PartitionedTable`, the map, `CREATE … PARTITION BY HASH` with default N = 64 (§1.2), ≤ 2,048 declared partitions per database (§7.1), root only, lazy materialization with exclusive creation, eager `Result` opens, existence checks (I9).<br>**Statements:** routing, single-snapshot fan-out for SELECT and COUNT, routed INSERT, UPDATE, DELETE, VACUUM over the partitions. **Prepared SELECT, UPDATE and DELETE** skip the learned-parse cache (`note_rewritten`), using the text path (2b gives `SELECT` the cache).<br>**Also:** the uniqueness rule, catalogs with running totals, `SHOW PARTITIONS`, DROP, the reaper, the duplicate-name startup refusal, the name-keyed `Database` guards (§3.3). **Not advertised.** | everything else, incl. joins, ON CONFLICT and RETURNING on partitioned tables, and §2's phased refusals (the as-built list is in §2) | on .7, ext4 not tmpfs: 100 concurrent tenants' DELETE/UPDATE mix vs one plain table (expect a large gain); INSERT and point SELECT (expect parity); concurrent same-row increments in one tenant, 40001s vs plain; fan-out latency over 1024 one-row partitions; first-write latency per new tenant; writer stall p99 during a checkpoint with 1024 active partitions; point-read routing cost (< 5 µs); idle wakeups at 1k open partitions. **2b proceeds only if the first shows the gain.** **As measured:** on tmpfs only, partbench and jsonbench (the 2b row's figures); the ext4 items wait for a venue (.7); 2b went ahead without them. |
| **2b** | **Built** (S15–S20, 2026-10-24; [as built](#phase-2b-as-built)). Joins (per-relation routing, one `Visibility`, no transitive inference; partitioned sources of `UPDATE … FROM`/`DELETE … USING`), the prepared-SELECT cache with late routing (I11) and a cached route derivation for full-key point reads (S16b), `ON CONFLICT` and `RETURNING` on partitioned tables; a same-value key assignment overwrites in place (§4.5). **Not advertised** until the wall-clock A/B below. | as 2a, less these: partitioned targets of `UPDATE … FROM`/`DELETE … USING`, `FOR UPDATE`/`SHARE`, `SKIP LOCKED`, `ALTER`/`RENAME`/`CREATE INDEX`, FKs (phase 3); key changes, including by `DO UPDATE` (phase 6); conversion and reshapes (phase 5) | prepared point-read cost vs plain. **Exit criterion, the end of "not advertised":** in prepared mode, point SELECT by the full key and `COUNT(*)` for one key value run within ~10% of a plain table's ops/s at 1 and 16 clients (partbench, same binary, back to back). As measured on 2a (90e781b, 64 partitions, 1000 × 100 rows) they ran at 0.42–0.53× plain, because 2a's prepared statements skip the learned-parse cache; simple mode was level, so the gap is the parse, not the routing. Writes were already at parity (0.87–1.15×) and unindexed scans within 0–15% (jsonbench, 8 partitions). S15's learned parse brought them to 0.81–0.88× (point) and 0.86–0.93× (`COUNT`), wall-clock on a loaded box; S16b's profile put the rest in re-deriving the route, `join_all` and one async layer. **On a shared box the merge gate** for the prepared-read work is instructions per op plus cycles per op at c=16, part/plain, from `perf` on a profiling build of `examples/partition_prepared_bench.rs`. **As measured (S16b, user-mode instructions per op, part/plain): point 1.02 at c=1 and c=16 (1.19 and 1.18 before), `COUNT` 1.005 (1.04 before): the merge gate is met.** Wall-clock on that box (load 7–30) ranged 0.49–1.96 for one binary, so it is not evidence either way. **"Advertised" additionally needs**, and is still pending, an interleaved wall-clock A/B on a truly idle box: plain and part alternating, ~10 short rounds, the median ratio within ~10%, recorded with its load averages. The owner (rasterizeddb-47) decided this. |
| **3** | Lazy open, scan-only fan-out opens, idle close, sharded CLOCK, pins, `SWEPT` and `DIRTY`, `MaintenanceMode::External`, lazy `.tmp`, persisted `SERIAL` high-water, metadata DDL and RENAME (§11) under ACCESS EXCLUSIVE on the logical name (the table-lock manager, §11), map-log generations, `fk_reverse`, FKs referencing partitioned tables, composite identifying keys (§1b) with FOR UPDATE, SKIP LOCKED, UPDATE FROM, DELETE USING; up to 65,536 partitions per node, default N = 1024 once measured (§1.2). **Advertised from here.** | as 2b, less these | fds, heap, wakeups and open cost per partition; startup at 10⁴ partitions with 100 active; time to first forget and to first `SERIAL` insert after restart at 10⁴; first-touch latency |
| **4** | Placement at materialize time across locations, weighted across tables; pool guards counting materialized partitions; touched-only `sync_durable`. **Gated on** the cross-session crash test and fix (§12). Databases without `s3_backup` only. | off-root placement under `s3_backup`; backup refuses to run beside off-root partitions (§12) | per-disk group-commit size and throughput at 1, 2, 4 disks |
| **4b** | S3 snapshots and log archive stream every pool location, with a multi-location restore test; phase-4 refusals lift | — | snapshot time vs location count |
| **5** | Reshape: MOVE, SPLIT, ISOLATE KEY, SPLIT KEY, MERGE, bounded-parallel REBALANCE, conversion; each with crash recovery, range-bounded re-routes, epoch-gated reclamation | — | fence duration vs partition size; throughput during a reshape |
| **6** | Row moves on key UPDATE, generational physical ALTER, co-location and partition-wise joins, `process_many` and `Folder` merge, `PARTITION OF` write-only alias | — | conversion time per GB |
| **7** | The §5 shared-point fixes that phase 2a's measurements single out: StatusLog staging, RowLocks fast path, WAL shards | — | commits/s vs cores and disks |

**Asks from the sibling designs**, scheduled by their owners here, listed in
[sharding §16](sharding.md#16-phased-plan) phase 0: the shared map-log framing
module (§3.2); the `FOREIGN` per-pid state, `RangeMap::within(owned, n)` and
`ACQUIRE_SPAN`/`RELEASE_SPAN` map-log edits (sharding §2.1, §5.2); the
resident-bytes open-budget term (§7.1). Persistent-index v1 hooks phase 3's
open and close (§7.2, point 4) for index sections in its phase 4; its phase 1
already moves every table's filter checkpoint to clean-point snapshots and, if
phase 3 is live by then, makes phase 3's close a FILTER clean point (point 3,
persistent-indexes §12).

## 16. Invariants and the tests that pin them

Each listed mutation must fail its test; afterwards, grep the tree for
`MUTATED`. Crash tests are bounded, remove their temp directories, and are
judged on exit code, not a grep of the summary line. "Enforced by" names the
section specifying the mechanism.

| # | Invariant | Enforced by | Test (and the mutation it must catch) |
|---|---|---|---|
| I1 | The hash never changes within a `route_v` | §1.3 | reference and framed golden vectors per family and level |
| I2 | Insert and query agree on a value's point | §1.3 | property test through the real tokenizer, `insert_row_with_defaults` and `extract_gated`, for every spelling the evaluator calls equal: `42`, `'42'`, `42::bigint`, `$1` text and binary, the prepared render; INT = BIGINT. *(Mutation: hash the literal text.)* **As built:** `partition::table_tests::insert_and_query_agree_on_points_for_every_spelling`, `tests/partition_insert.rs`'s `insert_and_query_agree_on_every_spelling`; **the cached route (S16b):** `pgwire::prepared_partition_tests::the_cached_route_hashes_like_route_where_and_insert`, over SMALLINT, INT, BIGINT, TEXT and a composite (BIGINT, TEXT) key, 900+ values: the bound key's partition equals `route_where`'s and the insert point's, and exactly the certain values bind. *(Mutations: hash a string's quoted text; hash an integer's text.)* |
| I3 | Routing is a superset, in bounded memory | §4.2, §4.3 | random `WHERE` trees (AND, OR, IN, ranges, casts, functions, contradictions) over random rows: every brute-force-matching partition is routed; two and three 10k-value `IN` lists route in bounded allocation and time, and still as a superset. *(Mutations: intersect inside `And`; prune an `Or` with an unconstrained branch; accept casts; keep only the first box; truncate at the cap instead of degrading; cap after the product; clear the bitmap bit when vacuum finds no live rows.)* **Joins (S19):** `row_source::join_tests::a_join_reads_only_the_partitions_its_own_predicate_proves`: a key pinned in the `WHERE` or an `ON` term reads its partition, a constant-false clause none, a key pinned only on the other side every materialized one. *(Mutation: route on an empty clause instead of the relation's predicate.)* |
| I4 | No row outside its range; no update changes a point | §4.5 | an injected wrong route errors; a key-changing `UPDATE` with no other transaction running (in-place fast path eligible) returns 0A000, the in-place counter stays, I6 finds no stray row. *(Mutations: remove the guard; move it back onto the relocating branch only, which I6 must catch.)* **As built:** `tests/partition_write.rs`'s `refused_writes_change_nothing` and `a_genuine_key_change_is_refused_before_any_partition_write` (a statement keeping one row's point and moving another's, in one partition: refused, no in-place overwrite, nothing changed); `tests/partition_upsert.rs`'s `a_do_update_that_moves_the_point_is_refused_before_any_write`; the twin's `UpdateKeyChange` and `InsertOnConflictKeyChange` (refused) and `UpdateKeySameValue` and `InsertOnConflictKeyAssign` (compared). **In-place eligibility (97e187f, §4.5):** `a_same_value_key_assignment_writes_no_more_versions_than_the_twin`, the twin's `reltuples`, `n_live_tup` and `n_dead_tup` after each same-value assignment. |
| I5 | The map is total and disjoint | §1.1 | fuzz the map log, truncating or corrupting at every offset: the last valid epoch is recovered, or the table refused loudly |
| I6 | A partitioned table answers like a plain twin | everything | **twin-table model test**, extending the index model test: N = 7 and 1024, open budget 1, random reshapes between; random inserts, updates (in place, relocating, same-value key assignment), deletes, rollbacks, conflicts, ON CONFLICT, RETURNING, VACUUM, VACUUM FULL, IN, OR, `$n`, prepared runs; every read compared (counts, aggregates, ORDER BY LIMIT OFFSET, DISTINCT, windows, joins). Empty routes: `COUNT(*)`, `MAX`, other aggregates without `GROUP BY`, `RETURNING`, `UPDATE … RETURNING`, on a never-written tenant and on `WHERE tenant_id = 1 AND tenant_id = 2`. **As built (2b):** `tests/partition_twin.rs`, four configurations (`twin_hash_tenant_7`, `twin_hash_tenant_64`, `twin_hash_tenant_grp_64`, `twin_hash_full_key_7`), seeds 0x5eed0001 and 0x5eed7a11 at 300 statements by default (`PART_TWIN_SEED`, `PART_TWIN_OPS`), a restart midway, what both tables store compared after writes, every kind required to run and every answering kind to agree on rows; each pgwire session's answers (rows as multisets where PostgreSQL gives no order, column names, type OIDs, tags, SQLSTATEs) compared, inside blocks too. The 2b kinds: the RETURNING kinds (S17), the ON CONFLICT kinds with 42P10 and 21000 (S18), `Join`, `LeftJoin`, `SelfJoin`, `JoinRouted`, `JoinFanOut`, `JoinPartitionedBoth`, `JoinAggregates`, `PreparedJoin`, `UpdateDimFromT`, `DeleteDimUsingT` (S19), and prepared reads that must run from the learned parse (S15). `REFUSED` holds only `UpdateKeyChange`, `InsertOnConflictKeyChange` (phase 6), `UpdateFrom`, `DeleteUsing`, `ForUpdate`, `SkipLocked` (phase 3); `KNOWN_DIVERGENCES` is empty. N = 1024, the open budget and reshapes wait for phases 3 and 5. `tests/partition_shell_sites.rs` runs each statement front door against a twin too (I8). **Phase 3 P1:** twenty-one DDL kinds (indexes, unique keys, CHECKs, defaults, NOT NULL, RENAME round trips of a column, the key and the table run as sequences with reads and writes under the change, a marking DROP COLUMN, a prepared SELECT across a RENAME COLUMN) compared, and the refused ones (`UniqueWithoutKey`, `DropNotNullKey`, `DropKeyColumn`, `AddColumnPopulated`, `AlterType`, `DropNotNull` of a NOT NULL column, `AddForeignKey`) in `REFUSED`; `SUCCEEDING_DDL` requires each changing kind to succeed on both at least once. **Phase 3 P4:** `AddForeignKey` leaves `REFUSED`; three foreign-key round trips (`ForeignKeyFromT`, `ForeignKeyOntoPartitioned`, `ForeignKeyFromSink`) are compared and in `SUCCEEDING_DDL`, with a per-side `{k}` parent partitioned by its key on the partitioned side. *(Mutations: a reload keeping one partition's old instance; the shell's sidecars written after the partitions reopen; the shape epoch left after a renamed column: `partitioned_ddl_tests::a_schema_change_reopens_every_open_partition_under_one_schema`.)* |
| I7 | One snapshot per multi-partition statement, taken before the map | §4.4, §8 | cross-tenant transfers beside a fan-out `SUM(balance)` that stays constant, across reshapes and in `REPEATABLE READ`; a fan-out `SELECT` at budget 1 with a slow lane, a reshape flipped and the reclaimer run mid-statement, returns the twin's rows. *(Mutations: a snapshot per partition; map read before snapshot; reclaim on the `Table`'s strong count.)* **Prepared fast path (S15):** `pgwire::prepared_partition_tests::the_fast_path_reads_one_snapshot_across_partitions`, 20 runs of a prepared unrouted `SUM`, a transfer committed between the first and second partition reads of each, under 5 s. *(Mutations: no scope in `run_statement`, failed at the first cached run by the scope check; a snapshot per partition, failed by the sum.)* **Joins (S19):** `row_source::join_tests::a_join_never_sees_a_cross_partition_transfer_half_applied` (partitioned × plain, a self-join, two partitioned tables, through `execute()` with no scope, a transfer across partitions and tables committed between the first and second partition reads of each fan-out) and `pgwire::prepared_partition_tests::a_prepared_join_reads_one_snapshot_across_both_sides` (the same over prepared executions, which take the scoped text path). *(Mutations: `plan_with` taking its own snapshot, failed by the self-join; a snapshot per partition, failed by the sums; no scope on the text arm, failed by the scope check.)* |
| I8 | The shell never serves data | §3.3 | a site catalogue, each statement correct or an error, never empty or a subset, the shell still in `Database.tables` after: DROP and ALTER `IF EXISTS`, `CREATE IF NOT EXISTS`, CREATE INDEX (reaching `reload_table`), VACUUM t, bare VACUUM (table in the report), LOCK, FOR UPDATE and SKIP LOCKED (refused until phase 3, then succeeding on a `PRIMARY KEY (tenant_id, asset_group_id, asset_id)` table), COPY, every ALTER verb, `STORAGE MOVE TABLE`, `STORAGE REBALANCE` and `DRY RUN`, `SHOW STORAGE`, every `pg_*` view (declared indexes in `pg_indexes`), FK checks, `seed_sequences`; no `<logical>*` file in root. *(Mutation: delete one routing call; that statement must error.)* **Phase 3 P1:** the DDL sites run in pairs that leave the table as it was, like the twin or refused; after every site the shell's schema file reads back as the shell holds it and every partition holds the shell's schema, a failed site changes neither, and the schema after the last site is the first one. |
| I9 | A partition opens only from its map location; missing files of a materialized partition, or the parent's ids or map file, are an error | §3.1, §3.5 | delete the files: a loud error; make the pool answer root: it still opens from its map entry. *(Mutation: open through `create_new` without the check.)* |
| I10 | Uniqueness holds | §1b | concurrent duplicate inserts and upserts across tenants and through reshapes; DDL refusals. **As built (S18):** `rasterizeddb_compat/tests/partition_upsert_matrix.rs`: `concurrent_upserts_on_a_partitioned_table_keep_every_key_unique_and_count_every_update` (sixteen clients over the primary key and a composite UNIQUE, `DO NOTHING` single- and multi-row and `INSERT … SELECT`, `DO UPDATE SET n = n + 1`, plain `INSERT`, in autocommit and committing and rolled-back blocks: no duplicate, every reported update applied once) and `three_hundred_keys_across_partitions_are_stored_once_under_concurrent_upserts` *(mutation: the in-flight rule off in `find_row_with_tuple`, 848 rows for 300 keys)*; `tests/partition_insert.rs`'s `duplicates_are_refused_and_racing_duplicates_store_one_row`; `tests/partition_ddl.rs`'s `a_unique_key_must_contain_every_key_column`. Reshapes wait for phase 5. |
| I11 | A prepared statement never pins a partition | §4.1 | prepared SELECT, UPDATE, DELETE, 10 runs alternating tenants, past the learned third. *(Mutation: route before `note_plain`.)* **As built (S15):** `pgwire::prepared_partition_tests::each_execution_routes_its_own_values` (point SELECT and COUNT, two tenants in two partitions, 10 runs each: own rows, own partition's reads only, 2 parses per statement; mutations: the first execution's route kept, failed at run 3; `note_rewritten` restored, 20 parses), `a_tenant_first_written_after_learning_is_counted` (mutation: each `WHERE`'s first materialized list kept), `text_and_binary_keys_route_alike_from_the_cached_parse`, `a_learned_parse_is_not_parsed_again`; `tests/partition_prepared.rs` (writes, DROP and re-CREATE under the name, columns reordered, a type change, the name changing kind, a restart; mutation: no schema-generation reset, failed on the reordered columns); the twin's `PreparedSelect`, `PreparedCount`, `PreparedDescribeAlias` and `PreparedSelectAcrossDdl`, which fail a run past the second that the cache did not answer. |
| I12 | Row ids are unique across the logical table | §3.5 | merge two partitions: no repeated id; ids file missing: table refused |
| I13 | Close, reshape, restart and crash never resurrect an aborted row | §7.2, §12 | abort a write in p, close p, forget past it for several `FORGET_STEP`s, reopen: invisible; again across a restart and a reshape. Write in a new partition, SIGKILL before group commit with data pages forced out by a data-file-fsync failpoint; restart, force forgetting past the id, write the same tenant: the old row stays invisible. *(Mutations: unpin on close; all partitions clean on restart; `SWEPT` before durability; `DIRTY` group-commit only; reuse leftover files at materialize.)* |
| I14 | At most one live instance per file set | §7.2 | open/close storm at budget 1 under unique-key writers |
| I15 | The reaper never deletes live data | §12 | corrupt one map, restart: every other table's files intact |
| I16 | WAL registration is never silently wrong | §7.2 | a colliding registration errors; checkpoint after close and a crash, compare with the twin |
| I17 | No retry after a partial apply | §9 | a conflict in the second partition of a fan-out UPDATE, in autocommit and in BEGIN. **As built:** `row_source::write_tests::a_conflict_after_a_partition_applied_rows_is_not_replayed` and, with `RETURNING` (S17), `row_source::returning_tests::a_returning_conflict_after_a_partition_applied_rows_is_not_replayed`; a multi-row upsert keeps it by `insert_values_batch`, where a tuple's empty `RETURNING` reply no longer strips the marker (S18, §9). |
| I18 | Downgrade fails loudly, leaving no shadowing duplicate | §3.1 | the previous release opens a phase-2 database: table absent, not empty; `CREATE TABLE t` there, insert, re-upgrade: the new binary refuses to start, naming both rows |
| I19 | Every crash window leaks rather than loses | §12, §3.2 | failpoints at each step of create, materialize, rename, reshape, conversion and drop, under SIGKILL and the page-cache-drop harness, including a page-cache drop after the outcome is durable: rows equal before or after, never neither. *(Mutation: append `MATERIALIZE` with `append_data(_, false)`.)* |
| I20 | Cache scopes are distinct | §3.4 | one `WHERE` on two partitions with different rows. *(Mutation: scope from `schema.name`.)* |
| I21 | Placement is balanced and never reaches `db_storage.cfg` | §6.1, §3.1 | 1024 materializations over 3 locations: within ±1 (round-robin) or proportional (free-space); a location added mid-run gets its share of later ones, one below `min_free_bytes` none; `db_storage.cfg` unchanged |
| I22 | Reshape writes are neither lost nor applied twice | §6.2 | writers through `MOVE PARTITION`; a `MERGE` with a fan-out `UPDATE … SET n = n + 1` parked between its sources; a statement crossing a split then a merge; all vs the twin, row count and `RETURNING` included. *(Mutations: skip the re-route; re-route without the range bound.)* |
| I23 | Replicas route on their own and refuse alike | §12 | primary 1024, replica 64: identical results; primary 64, replica 1024 plus a `SPLIT KEY`, key-assigning `UPDATE`s (same value, same point, different point) under each conflict policy: identical rows, nothing skipped on one side only |
| I24 | FK actions reach partitioned children | §10 | a parent delete cascades into every partition holding children. **As built (P4):** `tests/partition_fk.rs` (four configurations, each answer the plain twin's, every action, through a restart); the twin's `ForeignKeyFromT`, `ForeignKeyOntoPartitioned`, `ForeignKeyFromSink` |
| I25 | A rename never loses a partitioned table | §3.1 | rename, restart: rows intact, nothing reaped; crash at each step, restart. *(Mutation: `clusters = 0` on rename.)* |
| I26 | DDL never hangs the table | §11 | a `REPEATABLE READ` transaction holding `FOR UPDATE`, a second waiting on it, then an `ALTER`: it completes or fails with `55P03`, both transactions finish; a DDL storm under unique-key writers. **Phase 3 P1 (as built, PostgreSQL's queueing, persistent-indexes §7):** `tests/partition_alter.rs`'s `a_ddl_queues_behind_the_tables_users_and_a_timeout_writes_nothing` (a reading and a writing block: seven DDL kinds fail with 55P03 under `lock_timeout` and one with 57014 under `statement_timeout`, nothing written; unbounded, the DDL waits for the block and a reader arriving after it waits for the DDL) and `writers_through_a_stream_of_ddl_lose_and_mix_nothing` (four writers through thirty DDLs; every acknowledged row stored once, in its partition, indexes answering as scans, after a restart too); `pgwire::ddl_lock_tests::partitioned_ddl_waits_for_readers_and_its_refusals_take_no_lock`. *(Mutation: statements lock a partition's name and DDL the logical one; the DDL runs beside the reader.)* |
| I27 | A backup never silently omits a partition | §12 | off-root partitions, then `s3_backup`: the service refuses, no snapshot succeeds with a subset |

## 17. Risks and open questions

1. **The seam collides with work in progress** (§3.4).
2. **The downgrade guard rests on today's code**, read at `61e9200` (I18).
3. **The fence grows with the source's pointer file**: the repeated delta
   (§6.2, step 4), never the MVCC mover.
4. **Forgetting pauses after a restart** until partitions dirty at the crash
   are swept (§7.2).
5. **Shared CPU points may cap the gain** at high client counts (§5); phase 2a
   measures before anything promises more.
6. **The static uniqueness rule** will surprise `PRIMARY KEY (tenant_id, id)`
   under a two-column key. A possible opt-in, `WITH (split_floor = 1)`, would
   accept a unique key on a leading prefix, refuse a boundary inside such a
   prefix on every boundary-creating path, take the minimum across a
   co-location group, and need its own tests.
7. **Unrouted fan-out at 10⁴–10⁵ partitions** stays costly even with skipping.
   Open: does a `max_fanout` refusal beat a slow success? Proposed: a warning
   plus a setting.
8. **Index memory** is rebuilt on every reopen; if that churns, use a longer
   idle window or [persistent indexes](persistent-indexes.md), whose v1 loads
   a snapshot instead after a clean close (§7.2, point 4).
9. **`pg_dump`** emits a plain table, relkind being `'r'` (§14); a re-import
   loses partitioning unless the importer is told.
10. **Out of scope:** a physical tenant drop (§13), and global uniqueness on a
    non-key column, which would need a hidden table partitioned by it and a
    two-partition write protocol.
11. **One map per co-location group** couples members' reshape costs; separate
    maps with enforced equal boundaries would not, for more code.
12. **Checkpoints grow with active partitions** and can stall a location's
    writers for seconds (§5).
13. ~~**Row-addressing statements wait on composite identifying keys**~~:
    built in phase 3 P3 ([as built](#phase-3-p3-as-built)). Writers fail at
    once on a held row rather than wait as PostgreSQL's do.
14. **Replica apply skips a transient `53xxx`** under `Skip` and
    `LastWriterWins` until `replication/server.rs` retries it (§12).
