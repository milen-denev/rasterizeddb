# PostgreSQL compatibility

RasterizedDB speaks the PostgreSQL wire protocol, so the practical question is
not "does it implement SQL" but "does it accept the SQL a real application
sends". The `rasterizeddb_compat` crate answers that empirically: it models a
slice of a production schema (`pms`) as sea-orm entities, and probes one
language feature per raw-SQL statement.

```
cargo run -p rasterizeddb_compat --bin capabilities   # the matrix
cargo test -p rasterizeddb_compat                     # regression guard + ORM tests
cargo run -p rasterizeddb_compat --example probe_one -- "<sql>"
```

Out of scope by decision: `CREATE INDEX` — the engine has its own
leaf-structure and bit-sliced filter indexing.

## Status

**100 / 100 probes pass.** sea-orm performs find-by-id, filtered and ordered
queries, paging, inserts and updates including `NULL`, and related-entity loads
through joins.

## Upgrading across the `write_order` fix

`TableSchema::add_field` used to derive `write_order` and `offset` by re-reading
the field file through a handle obtained from `create_new`. Whether that read
saw the fields already written depended on the storage provider:

- `MemoryStorageProvider::create_new` returns a **fresh, unshared buffer**, so
  the read always came back empty and every column was written with
  `write_order: 0` and `offset: 0`.
- `LocalStorageProvider` reopens the real file, so file-backed tables got the
  correct `0, 1, 2, …`.

Since `schema_id` *is* `write_order`, identical write orders made every column
compare equal, and a `SELECT` could be served the `WHERE` clause's columns
instead of the ones it asked for. That made every in-memory database — which is
what `--in-memory` and the compat harness run — return wrong columns, while
file-backed databases were unaffected. Deriving both values from `self.fields`
removes the provider dependency entirely.

Field records written before the fix still carry whatever they were given, so an
in-memory-derived data directory created by an older binary will load with the
broken ordering. The
write-ahead log stores *physical* redo — bytes at an offset in a file — and
knows nothing about schema, so replaying an old log under a new binary would
faithfully restore old-layout bytes that the new schema then reads at new
offsets. If a directory does have to cross this change, shut down cleanly first
so the log is empty, then upgrade.

## Type system

Types are stored fixed-width and ordered by `memcmp` wherever an ordering is
meaningful, because the comparison evaluator, the fingerprint index and the
zone-map bounds all operate on raw column bytes. A type needing a bespoke
comparator would have to be special-cased in three places and would fall out of
the filter index entirely.

| SQL | storage | notes |
|---|---|---|
| `UUID` | 16 bytes, network order | `memcmp` reproduces canonical text order |
| `INET` / `CIDR` | 18 bytes: `[family][addr(16, BE)][prefix]` | sorts by family, then address, then netmask |
| `NUMERIC` / `DECIMAL` | `i128` at a fixed scale of 9 | one scale keeps it comparable as an integer |
| `TIMESTAMPTZ` | `i64` microseconds from the Unix epoch | little-endian, so it compares as a signed integer |
| `BYTEA` | variable width, same mechanism as `TEXT` | distinguished only so the wire reports `bytea` |
| `TEXT[]` | Postgres' binary array form, variable width | the element OID lives in the value, so one type serves arrays of anything |

`BYTEA` and `ARRAY` report `STRING` as their *physical* type, so the row writer,
the fetch path and the in-place-update path need no knowledge of them; only the
wire layer, which reports the declared type, tells them apart. Neither is
indexed: they share `STRING`'s storage but have no comparator, so indexing them
would store values no query could match.

Storing UUIDs natively rather than as `U128` matters for pruning. Canonical UUID
octets are big-endian while `U128` is read little-endian, so copying the bytes
straight in destroys the ordering. Measured on 1M v7 keys: byte-reversed storage
prunes a range query to 10% of the table, while a straight `U128` copy prunes
nothing at all, and point lookups improve ~18x.

## Supported

- **Projection** — `*`, column lists, `AS` aliases, qualified (`u.name`,
  `"users"."name"`) and quoted names, case-insensitive resolution, arithmetic.
- **Filtering** — `=`, `!=`, `<`, `<=`, `>`, `>=` on every scalar type,
  including lexicographic ordering of strings;
  `AND`/`OR` with parentheses; `IN`, `NOT IN`, `BETWEEN`, `NOT BETWEEN`, `LIKE`,
  `NOT` before a boolean column; boolean literals. `ILIKE` matches
  case-insensitively over ASCII.
- **Ordering and paging** — `ORDER BY` with `ASC`/`DESC`, including a sort key
  that is not selected; `LIMIT`; `OFFSET`.
- **Joins** — `INNER JOIN` and `LEFT JOIN`, any number of relations, with
  aliases. The key may be **composite** — `ON a.x = b.x AND a.y = b.y`, which
  every junction table needs. Matching on one column and filtering on the other
  is not the same join: it fans out on the first and then discards, which agrees
  with SQL only where the first column is already unique. An `ON` term that is
  *not* an equality between the two relations filters the relation being joined,
  which is exact on the null-supplying side where a `WHERE` term is not. Executed as a hash join over materialised rows:
  each relation is scanned once with its own share of the `WHERE` clause pushed
  down, so filter pruning and predicate evaluation still apply per relation.
  Ordering and paging run over the combined rows.
- **`SELECT DISTINCT`** — deduplicated after the scan, with `LIMIT`/`OFFSET`
  applied to what survives.
- **Uncorrelated `IN (SELECT …)`** — the inner query runs once and its results
  are folded into the outer statement as a literal list, which then follows the
  ordinary `IN` path, filter pruning included. A subquery returning nothing
  makes the predicate unsatisfiable rather than raising a syntax error.
- **Aggregates** — `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, with `GROUP BY`,
  `HAVING`, and ordering or paging over the grouped output, over a single table
  or over a join. `SUM` and `AVG` over an exact type stay exact rather than
  falling back to floating point, so a money column keeps its cents.

  `LIMIT` on an aggregate caps the *output*. It is deliberately not pushed into
  the scan: `enforce_limit_early` keys off the absence of an `ORDER BY`, so a
  limit reaching the scan would stop it after N input rows and return a
  plausible wrong count.
- **Writes** — `INSERT` (single and multi-row), `UPDATE`, `DELETE`, and
  `INSERT ... RETURNING`.
- **Protocol** — the extended query protocol with bound parameters in text and
  binary format, which is how every driver and ORM actually talks.
- **`pg_stat_statements`** — one row per statement shape, with `calls`,
  `total_exec_time`, `mean_exec_time`, `stddev_exec_time` and `rows`, built from
  the engine's own statement log rather than from an extension. It carries one
  column PostgreSQL has no counterpart for — `rows_scanned`, the candidate rows
  the scan read — and `SELECT pg_stat_statements_reset()` empties it. The block
  counters are reported as zero: there is no shared buffer pool here to count.
  See [introspection.md](introspection.md#pg_stat_statements).

Predicate forms are desugared into the engine's basic `column <op> value`
grammar before evaluation (`core/tokenizer/predicate_rewrite.rs`), so `IN`
becomes several indexable equality probes rather than one predicate the filter
would have to skip.

### `LIKE` and `ILIKE`

`LIKE` and `ILIKE` support the full pattern language. Patterns that reduce to a
prefix, suffix, substring or equality still become `STARTSWITH`/`ENDSWITH`/
`CONTAINS`/`=`, because those are cheaper and `STARTSWITH` is the one string
operator the filter index can lower to a range. Anything else — an interior `%`,
or any `_` — carries its wildcards into a `LIKE` operator backed by a pattern
matcher.

That split is not an optimisation detail. `a%b` is **not**
`STARTSWITH 'a' AND CONTAINS 'b'`: that does not require `b` to follow `a`, and
with three segments it loses the ordering entirely, so it would return rows
PostgreSQL does not. The reduction is only applied where it is exact.

`LIKE` is deliberately **not** sargable, and safe by construction rather than by
a rule: `sarg::op_of` accepts only `Token::Op` and five comparisons, while every
string operator arrives as a `Token::Ident`, so a pattern predicate yields an
unfiltered scan. A fingerprint hashes the whole value and zone bounds bracket a
prefix; neither can answer a pattern, and a mask that tried would prune rows that
match — returning *fewer* rows with no error.

## DDL

`CREATE TABLE`, `DROP TABLE`, and `ALTER TABLE` add/rename/drop column all work,
including quoted and schema-qualified names, `IF EXISTS`/`IF NOT EXISTS`, and
types carrying a precision such as `NUMERIC(10,2)`. Table-level constraints in a
`CREATE TABLE` body are skipped rather than parsed as columns.

**`ADD COLUMN` on a populated table rewrites the rows.** A row is a fixed-width
record, so a new column would otherwise be read at an offset the existing rows
never wrote. The rewrite reads each row through the old schema and writes it
back through the new one, filling the new column from its `DEFAULT` or with
`NULL`. `ADD COLUMN … NOT NULL` with no `DEFAULT` is refused **on a populated
table**, for the reason PostgreSQL refuses it: there is nothing to put in the
existing rows. On an empty table it succeeds, as it does in PostgreSQL — which
matters, because early migrations add such columns before any data exists.

**`DROP COLUMN` keeps the column's slot** and marks it deleted, which is cheaper
than the rewrite and invisible to queries.

`INSERT … ON CONFLICT DO NOTHING` works, with or without an arbiter, which is
what makes a seed migration re-runnable against a database that already holds
the rows. The arbiter is not consulted — any uniqueness violation is swallowed,
where PostgreSQL would error if the named columns matched no constraint. That is
more permissive, and the difference only shows for a statement PostgreSQL would
have rejected. `DO UPDATE` finds the conflicting row by its arbiter key as the
column stores it (`1.254` into a `NUMERIC(10,2)` key is `1.25`).

Two rows of one `INSERT … ON CONFLICT DO UPDATE` with the same arbiter key
refuse the statement with 21000, as in PostgreSQL, the keys compared as each
column stores them: `5.0` and `5.00`, `'A0EE…'` and `'a0ee…'`, `'2020-01-01'`
and `'2020-01-01 00:00:00'`, `true` and `'t'` are one key each. A NULL key
equals nothing, and an omitted or `DEFAULT` key is its column's default when
that is a constant (a sequence's is a new value per row).

Statements accepted without acting, each for a stated reason rather than
silently: index DDL (the engine maintains its own indexes), extensions, schemas,
`COMMENT ON`, `ANALYZE`/`VACUUM`, and table storage tuning —
`ALTER TABLE … SET COMPRESSION`, `SET`/`RESET (reloptions)`,
`VALIDATE CONSTRAINT`, `CLUSTER`, `OWNER TO`. `CHECK` is enforced; see
[CHECK constraints](#check-constraints).

**`UNIQUE` and `PRIMARY KEY` are enforced, including composite keys.** Declared
inline, as a table constraint, or through `ALTER TABLE … ADD CONSTRAINT`;
`PRIMARY KEY` counts because it is a `UNIQUE NOT NULL`. An `INSERT` or `UPDATE`
that would duplicate a value fails and names the constraint, its columns and the
row that already holds it, rather than reporting a generic failure an application
cannot branch on. Absent values are exempt, since SQL lets a unique column hold
any number of `NULL`s.

Single-column constraints declared inline live on `SchemaField.is_unique`;
everything table-level lives in a per-table sidecar, because a schema flag
carries no constraint name and `DROP CONSTRAINT` could never have found one.

An inline `UNIQUE` therefore answers to the name the table derives for it now,
`<table>_<column>_key` (cut where long), and to nothing else. **After
`ALTER TABLE old RENAME TO new` it is dropped as `new_<column>_key`;**
PostgreSQL keeps `old_<column>_key`, and asking for that name here is 42704
(a no-op under `IF EXISTS`). Earlier builds matched any name ending in
`_<column>_key`, which on a table with both `id` and `item_id` dropped the
wrong column's constraint, and dropped one for a made-up name.

Two limits remain. Uniqueness is checked against **committed** data, so two
concurrent transactions can insert the same value and both commit — the probe was
never an index and never locked. And `CREATE UNIQUE INDEX` is currently swallowed
with the rest of index DDL, so it establishes **no** constraint: express
uniqueness as a `UNIQUE` constraint instead. The access path genuinely is
unnecessary here; the uniqueness is not, and conflating the two is a bug.

## Foreign keys

Enforced, not merely accepted. Declared inline on a column, as a table
constraint, or through `ALTER TABLE ADD CONSTRAINT`; persisted in a per-table
sidecar so they survive a restart; and droppable by name.

- **Insert and update** check that the referenced row exists. `NULL` is exempt,
  since a foreign key constrains what a value may be, not whether there is one.
- **Delete** applies the declared `ON DELETE` action: `CASCADE` removes the
  referencing rows, `SET NULL` clears the reference, and `RESTRICT`/`NO ACTION`
  refuse while a reference remains.

Every check is phrased as a query the engine already runs — a `SELECT` to test
existence, a `DELETE` or `UPDATE` to propagate — so constraint enforcement is
filtered and evaluated by the same paths as anything else, and a cascade that
triggers another cascade works by recursion rather than a second mechanism. A
cycle is caught by a depth limit rather than recursing forever.

Composite keys are refused outright: enforcing one column of a two-column key
would enforce something weaker than what was declared.

Since the partitioning work's phase 3 P4 (`docs/partitioning.md`, "Phase 3 P4
as built") foreign keys also run to and from hash-partitioned tables, and:

- `ALTER TABLE … ADD … FOREIGN KEY` checks the rows already in the table, as
  PostgreSQL does (23503 naming the first value with no parent; it used to add
  the key unchecked). `… NOT VALID` skips that check and enforces new writes
  only; `VALIDATE CONSTRAINT` checks the rows later. The PostgreSQL import adds
  a key its source never validated `NOT VALID`, and a key whose source rows
  break it (loaded with `--disable-triggers`) `NOT VALID` too, with a note in
  the report, rather than stopping at its last step.
- `DROP TABLE` of a table another table's key references is refused (2BP01)
  unless `CASCADE`, which drops those keys and keeps their tables.
- A key between columns PostgreSQL cannot compare is refused with 42804:
  the integer widths, `REAL`/`DOUBLE`, `DATE`/`TIMESTAMP` and the text types
  pair any way round; an integer child may reference a `NUMERIC`, `REAL` or
  `DOUBLE` key and a `NUMERIC` child a `REAL` or `DOUBLE` one, not the
  reverse; any other type only itself. Values are compared in the other
  side's type, so an `INT` 7 finds a `NUMERIC` 7.00, a `DATE` only a
  `TIMESTAMP` midnight; an `INT8` beyond a `DOUBLE`'s precision finds none
  (PostgreSQL rounds it).
- Names are kept as written (UTF-8, spaces, quotes), and an unnamed key is
  PostgreSQL's `<table>_<column>_fkey`, numbered on a collision; a column's own
  `CONSTRAINT n REFERENCES …` is named `n`.

Still unlike PostgreSQL: a plain table may reference a column no unique key
covers, and `CREATE TABLE` may reference a table that does not exist yet (each
insert's check names it); identifiers are not case-folded.

### Release note: foreign keys from older binaries

What a database written by an older binary holds, read by this one:

- **An unnamed key keeps the name it was stored under**, `fk_<column>_<parent>`
  (the old default). New unnamed keys are `<table>_<column>_fkey`. Drop an old
  one by its stored name: `ALTER TABLE c DROP CONSTRAINT fk_pid_p`; its
  PostgreSQL name is 42704.
- **Names were stored ASCII-filtered**, the referenced table and column too
  (`"my parent"` as `myparent`). Such a key reads back as written. One whose
  parent table or column lost characters names a relation or column that does
  not exist: its checks fail naming it, and it is invisible to `DROP TABLE`'s
  2BP01 check, which matches the stored parent name. Drop it by its stored name
  and add it again.
- **Keys an older `DROP TABLE` left behind** (it dropped a referenced table and
  kept its children's keys) still name that table. Creating a table under the
  name makes it their parent again, and a later `DROP TABLE` of it is refused
  with 2BP01 naming them; drop them (`DROP CONSTRAINT`) or use `DROP TABLE …
  CASCADE`.
- **A key added `NOT VALID` looks like any other key after a restart**:
  validity is not recorded and `pg_constraint` is not emulated, so nothing
  shows which keys still have unchecked rows; run `VALIDATE CONSTRAINT` on
  each to find their orphans.

### CHECK constraints

A check refuses a row only when it is FALSE, as in PostgreSQL: a NULL result
passes, so `CHECK (x > 0)` and `CHECK (x IS NULL OR x > 0)` both take a NULL
`x`. It is judged on `INSERT`, on `UPDATE` (against the row as it will be
stored) and over the stored rows by `ALTER TABLE … ADD CONSTRAINT … CHECK`,
all three the same way: a row violates the check when `NOT (check)` holds,
evaluated as a `WHERE` clause would be.

Accepted forms include what `pg_dump` writes — `((status = ANY
(ARRAY['a'::text, 'b'::text])))`, a varchar column's `((status)::text = ANY
((ARRAY[…])::text[]))`, `'…'::timestamp without time zone` — and what ORMs
generate: comparisons, `BETWEEN`, `IN`/`NOT IN`, `IS [NOT] NULL`,
`IS [NOT] DISTINCT FROM`, `IS [NOT] TRUE/FALSE`, `LIKE`/`ILIKE`/`NOT LIKE`,
`SIMILAR TO` (and `pg_dump`'s `~~`, `~~*`, `!~~`), a `CASE` value compared,
`COALESCE`, `length`/`char_length`/`upper`/`lower`/`btrim`, `jsonb_typeof`,
`doc ? 'k'`, `doc @> …`, `doc ->> 'k' IS NOT NULL`, POSIX regular
expressions (`~`, `~*`, `!~`, `!~*`, `regexp_like`, `pg_dump`'s `x ~
similar_to_escape('…')`), and `AND`/`OR`/`NOT` of them. A check no evaluator
here can read is refused where it is declared, always with 0A000, rather than
stored unenforced. Not yet supported: a `CASE` whose branches are themselves
conditions (`CASE WHEN a THEN b > 0 ELSE true END`).

#### Regular expressions in a CHECK

A pattern is translated from PostgreSQL's dialect (its ARE) into one this
engine matches exactly as PostgreSQL does, checked against 16674 answers
recorded from PostgreSQL 17 (`core/rql/pg_regex_pg17.tsv`): `.` takes a
newline unless the `n` flag is given, `\d` and `[0-9]` are ASCII,
`[[:<:]]`, `[[:>:]]`, `\m`, `\M`, `\y`, `\Y` are word boundaries, `\b` is a
backspace and `\B` a backslash, and the flags `i c n m s t q` apply. The
shapes regex checks are written in are read: `NOT (s ~ 'x')`, `(s ~ 'a') =
false`, a literal on the left, `COLLATE "C"`, `s ~ ANY (ARRAY[…])`.

A pattern outside that subset neither refuses the table nor its rows: the
table is created (or the constraint added), the constraint is **not
enforced**, and a WARNING (01000) naming it is sent to the client and logged.
Outside the subset: back-references, look-around, `\x`, `\c` and octal
escapes, collating elements other than one ASCII character, equivalence
classes, non-ASCII range endpoints, non-ASCII characters under
case-insensitivity, the flags `x p w b e`, braces that are not a repetition
count, a quantifier on a quantifier or an anchor, and unknown classes. A few
constructs depend on the locale's character classes: `\w`, `\W`, `\s`, `\S`,
`\d`, every bracket class (`[:digit:]` included: under an ICU locale it takes
'١'), word boundaries, and case-insensitive matching. They are answered for
ASCII text, where every locale agrees; for other text the answer is UNKNOWN.
A check whose regular expressions stand under AND, OR and NOT alone is then
still judged, exactly: `s ~ '^\w+$' AND n > 0` refuses `n = -1` whatever `\w`
says. A check that reads that answer as a value (`(s ~ '…') IS TRUE`,
`COALESCE`, `CASE`) is not judged for the row; the statement says so once, as
a WARNING (01000) naming the table and the constraint with how many rows, and
the log says it once per statement.

A pattern or flags read from the row (`CHECK (s ~ t)`) outside the subset
refuse that row with 0A000, and an invalid one with 2201B, as PostgreSQL's.

A check that reads a `character(n)` column (`char`, `char(n)`, `bpchar`) is
kept unenforced, with a WARNING, on every DDL path (CREATE TABLE, ALTER TABLE
ADD CONSTRAINT, ADD COLUMN … CHECK): PostgreSQL compares `character(n)`
blank-padded (`'AB '` is `'AB'`), and this engine stores it as text, so
enforcing it would refuse rows PostgreSQL accepts. The column's declared type
is recorded with the table for a later ALTER, and follows the column: `RENAME
COLUMN` carries it, `DROP COLUMN` retires it, and `ALTER COLUMN … TYPE` between
`character(n)` and text sets or retires it, declining the column's stored
checks when it becomes `character(n)` (a check stored while it was
`character(n)` stays declined after `TYPE text`). The stored check reads the
column `::bpchar`, as `pg_dump` writes it.

`SHOW CHECKS` lists every check, with `enforced`, `enforced (ASCII-exact)` (a
locale-dependent pattern, answered exactly for ASCII text), `not enforced` or
`cannot be compiled` (one stored by an older binary) and why; each that is
not enforced is also logged once when its table is opened. `ALTER TABLE …
ADD CONSTRAINT` says, as a WARNING, how many stored rows went unjudged.

A pattern PostgreSQL itself rejects (`'('`, `'[a'`, `'*a'`) is accepted at
CREATE TABLE, as there, and every row it meets is refused with 2201B; an
unknown flag, or `g`, is 22023.

#### Release note: checks stored by older binaries

Older binaries stored some checks without enforcing them, logging "could not
be compiled" at each write: checks using `COALESCE`, `IS NOT NULL`,
`char_length`, `upper`, `IS DISTINCT FROM` or `->> … IS NOT NULL`, among
others. This binary enforces them. Rows written while they were not enforced
may violate them, and a later `UPDATE` of such a row fails with 23514 even if
it does not touch the checked columns. Find them before upgrading traffic:

```sql
SELECT * FROM <table> WHERE NOT (<check expression>);
```

and fix or delete each row, or drop the check (`ALTER TABLE <table> DROP
CONSTRAINT <name>`) and add it again once the data holds.

### Correlated EXISTS in UPDATE and DELETE

A correlated `[NOT] EXISTS` in an `UPDATE` or a `DELETE` (`UPDATE t … WHERE
EXISTS (SELECT 1 FROM u WHERE u.tid = t.id)`, Django's forms included) runs
only in a `REPEATABLE READ` or `SERIALIZABLE` transaction, and is refused with
0A000 under `READ COMMITTED` (autocommit or an explicit block). Its subquery is
read once, under a snapshot taken before the rows it writes are scanned, so a
concurrent writer between the two would make it touch rows PostgreSQL's does
not; a pinned snapshot makes them one. Write it as `UPDATE … FROM` /
`DELETE … USING` to run it under `READ COMMITTED`. `IN (SELECT …)` and `NOT IN
(SELECT …)` in a write share the snapshot gap and are not yet refused.

A correlation may name the target schema-qualified (`"public"."t"."id"`), cast
either side (`u.tid = t.id::bigint`, `::numeric(10,1)`), or, in `UPDATE …
FROM`, name the target beside its source. A cast this engine does not compute
as PostgreSQL does — a type modifier other than NUMERIC's (`::varchar(1)`,
`::timestamp(0)`), or `character` — is refused with 0A000 there. One that compares an outer
column under arithmetic (`u.x = t.x + 1`), or an `EXISTS` used as a value
(`x - EXISTS (…)::int`), is refused with 0A000.

### NUMERIC past nine places

Expressions compute `NUMERIC` at the engine's nine places. A column declared
with more (`NUMERIC(20,12)`) is still exact where it is stored, compared with
a literal of any length or a column of another scale (`WHERE ja.a > jb.a`),
compared in an expression (`CASE WHEN a > 1`), cast (`a::numeric(10,2)`,
`a::float8`, `a::text`), assigned (`UPDATE … SET a = a + 0.000000000001`,
computed exactly and rounded once to the column's scale), copied by `INSERT …
SELECT` from any source (a join, a derived table, a CTE, `min`), and in a
select list, `ORDER BY` key or `RETURNING` item built from `+`, `-`, `*`,
unary minus, `COALESCE`, `CASE`, `LEAST` and `GREATEST` (`SELECT a + 1`,
`-a`, `a * 2`). A `UNION` of NUMERIC columns at different scales, or of a
NUMERIC and an integer, an expression, an aggregate or a numeric constant,
gives each value at its own scale; one whose branch types cannot be resolved
to NUMERIC (a float, a join or `SELECT` with no `FROM`) is refused with
0A000 rather than mixed.

A constant with more than nine places, written either way (`0.000000000001`,
`43e-17`), is `numeric` as in PostgreSQL, carried with its digits (up to 38
places) and computed exactly in the same places: `SELECT c + 43e-17` is
44812.65000000000000043, and a `CASE` with such a branch stays `numeric`.

Refused with 0A000 rather than answered without the value's last digits:
a value with more than nine places stored into an unconstrained `NUMERIC`
column, which keeps nine here (declare it `NUMERIC(p, s)`),
`WHERE`-clause arithmetic on such a value or constant (`WHERE a + 0 > 1`), a
quotient that does not end (`a / 3`), such a constant inside an aggregate's
argument (`sum(c + 43e-17)`), and a `UNION` whose first branch is an integer
and a later one NUMERIC (cast the first branch's column to `numeric`). The
refusal for a column's value depends on the data: it fires only for rows whose
value has nonzero digits past the ninth place, so the same statement may run
against some rows and be refused against others.

## JSON

`JSON` and `JSONB` are accepted, but nothing stores JSON. Each JSON column is
mapped onto a table of its own, one column per key, and the column on the parent
table holds a `UUID` pointing at the row that holds the document's fields.

The reason is that an opaque blob is invisible to everything that makes this
engine fast. A binary JSON encoding would have to be parsed per row, would sort
by nothing meaningful, and could not be fingerprinted or bounded by a zone map —
so every predicate over a document field would degrade to a full scan. Mapped
onto columns, a field is an ordinary column: the comparison evaluator, the
bit-sliced filter index and the zone maps all see it, and
`WHERE hits > 10` on a document field is planned exactly like
`WHERE hits > 10` on a declared one.

**Type inference.** A key's column type comes from the first value seen for it:

| JSON value | column type |
|---|---|
| `true` / `false` | `BOOL` |
| whole number | `U64` / `I64` |
| fractional number | `F64` |
| string parsing as a timestamp | `DATETIME` |
| string parsing as an address | `INET` |
| any other string | `STRING` |
| array | `ARRAY` |
| object | a table of its own, referenced by `UUID` |
| `null` | no column; the key is simply absent |

A string is only read as a timestamp or an address if it also *looks* like one.
Without that check `"12345"` would become a timestamp, because the epoch parser
accepts a bare integer.

**Auto-migration.** A document carrying a key the table has no column for gets
one, so a schema that grows in the application does not have to be migrated by
hand. A key whose value contradicts its column's type **widens** the column
rather than refusing the document: integer meeting a fraction becomes `F64`, and
anything genuinely incompatible becomes `STRING`. Widening rewrites the existing
values through the new type rather than discarding them — `10` becomes `10.0`,
then `"10.0"` — so no already-stored document loses data to a later one.

**Naming.** The shadow table is `__json_<parent>_<column>_<hash>`, where the
hash is over the qualified name. The prefix keeps it out of the way of user
tables, and the hash keeps two columns from colliding when their names truncate
to the same thing.

**Where documents may be assigned.** `INSERT`, `UPDATE` and `ALTER TABLE …
ADD COLUMN … JSONB` all map documents the same way. The `UPDATE` path was added
after it was found writing the document into the `UUID` column as raw text: the
statement reported `UPDATE 1`, the previous contents were gone, and the column
read back as `{}`. When a feature lands on one arm of the executor, the sibling
arms need it too.

**The top level may be an object or an array.** A document's fields become
columns; an array's elements become rows. Both round-trip, including the case
that matters most in practice — an array of uniform records:

```
[]                                  ->  []
[1,2,3]                             ->  [1,2,3]
["alpha","beta"]                    ->  ["alpha","beta"]
[{"a":1,"b":"x"},{"a":2,"b":"y"}]   ->  exact
[{"a":1},{"b":"differs"}]           ->  exact      (ragged keys)
[{"tags":["a","b"]}]                ->  exact      (objects containing arrays)
```

A **bare scalar** is refused in every position — `INSERT`, `UPDATE` and
`DEFAULT` alike — because it has neither fields to become columns nor elements
to become rows. `DEFAULT '[]'` was for a time accepted and silently stored as
`{}`, which is the same value refused in one position and quietly changed in
another; that is fixed, and `[]` is now stored as itself.

An array whose elements are themselves arrays shreds correctly **at the top
level** — `[[1,2],[3,4]]`, `[["a"],["b"]]`, `[[]]` and `[[[1]]]` all round-trip,
each inner array getting its own table and a reference, the same way a nested
object does.

**Known gap: an array of arrays *under a key*.**

```
{"nested":[[1,2],[3,4]]}    ->  {"nested":["[1,2]","[3,4]"]}
```

The inner arrays come back as JSON *strings*. The statement reports success, so
an application storing this loses the value without being told. The top-level
and under-a-key forms went through different rendering paths, and only the first
has been fixed.

**Scope it by the value, not by the column.** The exposure is *any JSON value
containing an array of arrays, at any depth* — not "columns that store an array
of arrays". A survey of the reference workload missed its one affected site by
categorising columns by what they hold at top level; there the array-of-arrays
sits two levels down, under a key, in a column whose top level is an object.

An object holding an array is fine (`[{"tags":["a","b"]}]` round-trips), so this
is specifically an array whose *direct elements* are arrays.

Registration is persisted in a per-table sidecar, so a restart reconnects each
JSON column to its table. See `core/rql/json_map.rs` and
`rasterizeddb_compat/tests/json_columns.rs`.

## Databases

The database named in a connection string selects one. Each is a whole engine
instance — its own tables, write-ahead log and transaction manager — so a
transaction cannot span two, which is PostgreSQL's rule as well.

```
--pg-default-db <name>                         (default: postgres)
```

```
postgres://superuser:secret@host:5432/analytics?sslmode=disable
                                     ^^^^^^^^^ resolved against the catalogue
```

A name that does not exist is refused with `3D000 database "x" does not exist`,
before authentication. That ordering is forced: **roles are per database**, so
which `rdb_roles` a password is checked against depends on which database was
asked for, and the name has to be resolved to have a catalogue to check. The
cost is that an unauthenticated client can tell which database names exist.

`CREATE DATABASE <name>` and `DROP DATABASE [IF EXISTS] <name>` work over the
wire. `CREATE DATABASE` takes no options — `WITH OWNER`, `TEMPLATE`, `ENCODING`
are refused rather than accepted and ignored, since nothing here would honour
them. A dropped database is refused while any session is still attached to it,
as in PostgreSQL, and the default database cannot be dropped at all.

Names are stricter than PostgreSQL's: letters, digits and underscores, not
starting with a digit, at most 63 characters. A name is also a directory name,
so it is restricted to what means the same thing on every filesystem and cannot
escape the root. An unquoted name in `CREATE DATABASE` folds to lower case and a
quoted one does not, as with any identifier.

### On disk

**The location directory is the default database**, and every other database is
a subdirectory of it:

```
/var/lib/rdb/            <- the default database (`postgres`)
  db_data_rows.db
  db_data_schema.db
  analytics/             <- database "analytics"
  staging/               <- database "staging"
```

The asymmetry is what makes this a non-migration: a data directory written by a
single-database build *is* a catalogue holding exactly the default database, and
nothing moves. Existing deployments should pass `--pg-default-db` naming
whatever their clients already connect as, since the default is now `postgres`.

Which databases exist is read from the directory rather than from a metadata
file — a subdirectory is a database if it holds the schema file every database
has. A catalogue file could disagree with the directories it lists; this cannot.

### Limits

- **Replication, S3 backups and the rastcp protocol act on the default database
  only.** They are configured per process, and nothing yet gives a named
  database its own peers or bucket.
- **A dropped database's background tasks are not stopped.** Vacuum and filter
  checkpointing run until the process exits, writing to files that have already
  been unlinked. Their work is discarded, but the space is not reclaimed until
  then.
- **No `current_database()`, `pg_database` or `\l`.** Connecting works; tooling
  that introspects the database list does not.
- **No schemas.** `search_path` is not honoured, and a schema-qualified name has
  its qualifier stripped.

### Why the query cache had to change

The query cache is one map for the whole process, keyed by table name, clause
and limit, while the generation counter that retires an entry is per table. Two
databases each holding a `users` table would have shared cache slots and never
invalidated each other's, so a scan in one could have been served the other's
row pointers. Each table's cache key is now qualified by the id of the database
that owns it — an id rather than the name, so a database recreated under a name
does not inherit what its namesake left behind.

## Connection limits and timeouts

```
--max-connections <n>                          (default: 500; must be >= 1)
--authentication-timeout <time>                (default: 60s; bare number = seconds)
--idle-in-transaction-session-timeout <time>   (default: 0 = off; bare number = ms)
--idle-session-timeout <time>                  (default: 0 = off; bare number = ms)
```

These follow PostgreSQL's settings of the same names:

- **Over `max_connections`** a connection is answered, after its startup
  packet and any TLS negotiation, with `FATAL 53300` "sorry, too many clients
  already". Connections still being refused are capped at the same number
  again; past that a socket is closed unanswered. Each connection, admitted
  or being refused, holds two descriptors, so a storm costs at most four per
  unit of `max_connections`. There is no unlimited setting.
- **`max_connections` must fit the open-file limit.** At start-up the server
  needs `4 × max_connections + 256` descriptors (256 kept for table files, the
  WAL and checkpoints). If the soft `RLIMIT_NOFILE` is short of that, it is
  raised as far as needed, up to the hard limit. If the hard limit is short
  too, `max_connections` is lowered to fit, a warning is logged, and
  `SHOW max_connections` reports the lowered value. Without this, a cap the
  limit could not hold ran the process out of descriptors before anyone was
  refused: `accept()` failed with `EMFILE` and the engine's own file opens
  failed inside sessions already admitted.
- **`authentication_timeout`** bounds everything before the session exists —
  the startup packet, TLS and the password exchange. A silent socket is
  closed; one that stalls mid-authentication is told `FATAL 57014`
  "canceling authentication due to timeout".
- **A startup packet** longer than 10,000 bytes (PostgreSQL's
  `MAX_STARTUP_PACKET_LENGTH`), or shorter than 8, is refused before anything
  is allocated, with `FATAL 08P01` "invalid length of startup packet", and the
  connection is closed.
- **`idle_in_transaction_session_timeout`** ends a session that sits inside an
  open block (failed or not) for longer, with `FATAL 25P03`; the block is
  rolled back before the error is sent. **`idle_session_timeout`** does the
  same outside a block, with `FATAL 57P05`. Both run only while the server
  waits for a client that has been sent `ReadyForQuery` — never during a
  statement, and not between `Parse` and `Sync`. Sessions `SET`, `SET LOCAL`,
  `SHOW` and `RESET` them, with PostgreSQL's units (`us`, `ms`, `s`, `min`,
  `h`, `d`); `RESET` returns to the server's flag. `SET LOCAL` is undone when
  the block ends, but not by `ROLLBACK TO SAVEPOINT`. It undoes only the
  setting it named, and a plain `SET` of that setting later in the block
  supersedes it, so the plain `SET` outlives the block as in PostgreSQL.
- A session that is ending gives the client 2 s to take its last bytes — the
  `FATAL`, any output still buffered, a TLS `close_notify` — and closes
  without them after that, so a client that has stopped reading cannot keep a
  terminated session's slot.
- `max_connections` and `authentication_timeout` are shown by `SHOW` and
  refused by `SET` (`55P02`).

A failed `accept()` — `EMFILE` when the process is out of descriptors, for
one — is logged and retried with a backoff of 50 ms growing to 1 s; it never
stops the listener, and a listener that cannot start at all (port taken,
certificate unreadable) exits the process with status 2 rather than 0.
Accepted sockets get TCP keepalive (on Linux: first probe after 60 s idle,
then every 15 s, dead after 4 unanswered), so a peer that vanished without
closing its socket stops holding a slot.

## Authentication and encrypted transport

```
--pg-auth scram-sha-256|md5|password|trust     (default: scram-sha-256)
--pg-user <name> --pg-password <password>      bootstrap role
--pg-tls require|prefer|disable                (default: require)
--pg-cert <path> --pg-key <path>               optional; self-signed if omitted
```

**SCRAM-SHA-256** (RFC 5802/7677) is the default and what every modern driver
negotiates. The password never crosses the wire and the server stores only
values derived from it, so a stolen catalogue yields nothing replayable. The
implementation is pinned against the RFC 7677 worked example.

`md5` and cleartext exist for clients that speak nothing else. Both put a
replayable secret on the wire, and cleartext is only safe under TLS.

Three properties are deliberate:

- **An unknown user is indistinguishable from a wrong password.** The SCRAM
  exchange runs to completion for a user that does not exist, using a salt
  derived from the stored secret rather than a random one — a random salt would
  differ between attempts and reveal that the account is missing.
- **Channel binding is refused rather than ignored.** Accepting `p=` and not
  binding would defeat the protection the client asked for.
- **The verifier format follows the configured method**, as `password_encryption`
  does in PostgreSQL. A SCRAM verifier stores nothing an `md5` challenge can be
  answered from, so the two do not silently mix.

Roles live in an ordinary table, `rdb_roles`, so they are queried, written and
persisted by the same machinery as anything else. **The table is per database**:
an account created in one database is not an account in another, and a database
created by `CREATE DATABASE` is seeded with the bootstrap role so that it is
reachable at all. There is no cluster-wide role catalogue and no `GRANT`; see
*Databases* above. `CREATE USER`/`CREATE ROLE`,
`ALTER ROLE … PASSWORD` and `DROP ROLE` are supported; `CREATE USER` implies
`LOGIN` and `CREATE ROLE` does not, as in PostgreSQL. A bootstrap role is
created only when the catalogue is empty, so a restart cannot resurrect a
deleted account or reset a password — which also means `--pg-password` does
nothing on an existing data directory.

**`is_superuser` is recorded but never checked.** It is parsed from `CREATE ROLE
… SUPERUSER`, stored and read back, and no code path consults it: any role that
can log in can create roles, drop tables and read `rdb_roles`. There is no
`GRANT`/`REVOKE` and no per-table privilege. Nor is there a `pg_hba.conf`
equivalent — one authentication method covers the whole listener.

**TLS** uses rustls. With no certificate configured a self-signed one is
generated at start-up, so an encrypted listener needs no preparation.
`--pg-tls require` refuses a cleartext connection rather than downgrading it.

## Transactions

`BEGIN`, `COMMIT` and `ROLLBACK` do what they say. They used to be accepted and
ignored — each statement committed as it ran, so an application that rolled a
transaction back had already committed every statement in it, with no way to find
out.

The engine is MVCC: an update writes a new row version rather than overwriting
one a reader might still be looking at, and each statement reads through a
snapshot. Uncommitted work is invisible to other sessions, a rollback undoes
inserts, updates and deletes alike, and two transactions updating the same row
get a serialization failure rather than duplicating it.

Isolation levels: `READ COMMITTED` (default) takes a fresh snapshot per
statement; `REPEATABLE READ` fixes one for the transaction. `SERIALIZABLE` is
accepted and behaves as `REPEATABLE READ` — it does **not** detect write skew,
which needs predicate locking. `READ UNCOMMITTED` behaves as `READ COMMITTED`.

A failure inside a block poisons it: later statements are refused with `25P02`
until the block is closed, and committing a poisoned block rolls it back. A
connection dropped mid-block rolls it back too.

Savepoints work — `SAVEPOINT`, `ROLLBACK TO SAVEPOINT`, `RELEASE`. That is what
an ORM's nested transactions are built from, so `sqlx`'s `outer.begin()` and
sea-orm's nested transactions both work. Rolling back to a savepoint also
un-poisons a failed block, which is the reason the feature exists: recovering
from one failed statement without losing the whole transaction.

Not built: blocking-on-conflict (the second updater is told to retry rather than
made to wait), and write-skew detection under `SERIALIZABLE`. See `docs/mvcc.md`.

### Row locks (`FOR UPDATE`, `FOR SHARE`, `SKIP LOCKED`, `NOWAIT`)

A locking read takes each row's lock and waits (bounded) for a holder. A
writer honours them, but how it meets one depends on timing:

- When some row is locked anywhere at the statement's start, an `UPDATE` or
  `DELETE` asks for each row's lock first and is refused at once with
  `55P03` on a row another transaction holds. PostgreSQL would wait.
- When no row is locked at the start, the writer skips that pass and marks
  each row just before writing it. A `FOR UPDATE` that locks the row after
  the mark waits for the writer's transaction to end and then reads its
  committed row. If the lock came first, the writer meets it at the mark:
  having written nothing yet, the statement waits for the holder and is
  replayed, which is PostgreSQL's wait; having already written other rows,
  it fails with `55P03`.

A writer's marks are per row until one transaction has written more than
1,024 rows of one table. Past that they become one mark on the whole table,
to bound memory: until that transaction ends, **every** `FOR UPDATE` on the
table waits for it, `NOWAIT` fails with `55P03`, and `SKIP LOCKED` skips
every row. A batch job that updates thousands of rows of a queue table in
one block therefore holds up every worker's `SKIP LOCKED` claim on that
table until it commits; run such a job in smaller transactions.

## Not yet supported

Ordered by how much application traffic each blocks.

### Nullable columns — implemented

See `core/rql/nullable.rs`. Each nullable column `x` carries a companion `BOOL`
column `__null_x`, so nullability needs no change to the row layout, no new
schema field, and nothing from the storage engine — a companion is an ordinary
fixed-width column. A column is nullable exactly when its companion exists.

`IS DISTINCT FROM` and `IS NOT DISTINCT FROM` work, rewritten through the
companion the same way: `a IS DISTINCT FROM v` becomes
`(__null_a = 1 OR a != v)`. They are the null-safe comparison — true or false
where `=` would answer NULL — which is what an ORM emits against a nullable
column. On a `NOT NULL` column there is no third outcome and they degenerate to
`!=` / `=`.

`WHERE` gets SQL's three-valued semantics without a three-valued evaluator: the
rewrite guards each comparison against a nullable column with
`__null_x = 0 AND …`, which reproduces SQL's answers under `AND`, `OR` and
negation, and stops `WHERE country = ''` matching a `NULL` row.

`IS NULL` and `IS NOT NULL` become `__null_x = 1` and `__null_x = 0` before the
tokenizer sees them, so **both are ordinary equalities on a real column and both
are safe to prune on.** Worth stating plainly, because the rejected design below
makes the opposite claim about `IS NOT NULL` and it is easy to carry that warning
across: the `!=` trap is about *complementing* an over-approximating probe, and
an equality against `0` complements nothing. Its mask is a superset of the
matching rows exactly as an equality against `1` is.

**The rest of this section describes an alternative that was rejected and never
built.** It is kept for the reasoning, not as guidance — every bullet below
describes the inline-flag design, and several of its constraints do not apply to
the companion-column design that ships.

The alternative considered was an inline flag byte before each value, at the
same one-byte cost. It was rejected because it changes the row's physical
layout, which means the three width computations, the fetch loop and the
in-place-update path all have to agree about it:

- **Storage** — a flag byte immediately before each **nullable** column's value,
  with `SchemaField.offset` still pointing at the value. Non-nullable columns
  stay byte-identical to today, so tables that use no `NULL` keep exactly the
  current layout and hot paths. Three write-width computations must agree:
  `RowPointer::new`, `RowPointer::calculate_total_bytes` and
  `RowPointer::build_row_data`.
- **Filter API** — `ColumnWritePayload` and `Column` gain an explicit
  `is_null: bool`, and `backfill_row` takes `Option<Vec<u8>>`. NULL must stay a
  third state, distinct from both "absent" and "empty": encoding it as
  zero-length bytes would collide with the empty string, so `WHERE name = ''`
  would match `NULL` rows.
- **Fingerprints** — `NULL` maps to the reserved `CODE_NULL`, not `CODE_EMPTY`,
  which would make it indistinguishable from a deleted row. `NULL` should also
  count as a distinct value in the cardinality estimate, or the selectivity gate
  will decline to index exactly the mostly-`NULL` columns where `IS NULL`
  filtering is worth having.
- **Zone maps** — `NULL` never enters min/max bounds. Same reasoning as `NaN`: a
  value that cannot satisfy a range predicate must not widen the range.
- **Predicate shape** — `rewrite_where` emits a unary
  `ComparerOperation::IsNull` / `IsNotNull` with the column as the sole operand,
  so the tokenizer never learns the `IS`/`NULL` keywords. On the wire that is a
  two-token span, `[Ident, Op]`. `IsNull` is a clean fingerprint probe;
  `IsNotNull` could not be, because with a unary operator the only way to answer
  it is to complement the `IsNull` probe — and the complement of an
  over-approximation *under*-approximates, dropping real results. The same trap
  as `!=`.

  **This constraint died with the design.** The companion-column rewrite turns
  both forms into equalities against a real `BOOL` column, so neither is a
  complement and both prune safely. See the note above.
- **Transactions** — writes on this path must go through
  `append_data_txn`/`write_data_txn` and let the statement's owner call
  `commit_durable` once, or one logical row write splits into several and
  reopens the torn-update window.

### 2. Smaller gaps

- Correlated subqueries: `EXISTS (… WHERE inner.x = outer.y)`, and scalar
  subqueries in the select list. Only the uncorrelated `IN` form works, because
  it can be executed once; a correlated one would have to run per outer row.
- `ON CONFLICT … DO UPDATE`, `DISTINCT ON`, `UPDATE … FROM other_table`,
  window functions, `CREATE SEQUENCE`, table partitioning, and `DO $$ … $$`
  plpgsql blocks. All confirmed present in a real application's migrations, so
  these are gaps rather than hypotheticals.
- `NOT` over a parenthesised group — needs the evaluator to negate a subtree
- `CASE WHEN`, `::type` casts, and `WITH RECURSIVE`. The last is refused **by
  keyword**: treating a recursive CTE as non-recursive computes the base case,
  stops, and returns a shorter answer with no error.
- `RIGHT`/`FULL`/`CROSS JOIN`, and `WHERE` terms spanning two relations. Refused
  by name rather than mis-evaluated. An `ON` term spanning two relations is only
  accepted when it is a further **equality**, which makes it part of a composite
  join key; anything else there would have to be evaluated after the join.
- On the null-supplying side of a `LEFT JOIN`, only `<column> IS NULL` is
  supported in a `WHERE`, and only when that column is `NOT NULL`. Anything else
  is refused, because a `WHERE` term pushed down there changes *which left rows
  appear* — it would return rows PostgreSQL excludes. An `ON` term is different
  and **is** pushed down: it changes which right rows are candidates, leaving
  unmatched left rows to survive with `NULL`s exactly as they should.

  The `NOT NULL` requirement is subtle and is a property of the *column*, not of
  the query: on a nullable column `IS NULL` cannot distinguish "no matching row"
  from "matched, with a NULL value there", so it is refused rather than answered
  as an anti-join.
