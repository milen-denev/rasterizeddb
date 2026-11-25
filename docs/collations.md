# Collations

Text compares by its bytes here, which is PostgreSQL's `C` collation. A
deterministic collation named on a column (`COLLATE "C"`, `"POSIX"`,
`"en_US"`, `"de-DE-x-icu"`) is accepted and changes nothing, as before.

A **nondeterministic** collation makes text that differs only in case (or in
case and accents) compare equal:

| Collation | Ignores | PostgreSQL spelling it stands for |
|---|---|---|
| `rdb_ci` (also `case_insensitive`, `"und-u-ks-level2"`) | case | ICU `und-u-ks-level2`, `deterministic = false` |
| `rdb_ci_ai` (also `ignore_accents`, `"und-u-ks-level1"`) | case and accents | ICU `und-u-ks-level1`, `deterministic = false` |
| type `citext` | case | the `citext` extension |

```sql
CREATE TABLE people (
  id    int PRIMARY KEY,
  name  text COLLATE rdb_ci NOT NULL UNIQUE,
  email citext,
  place text COLLATE rdb_ci_ai
);
SELECT id FROM people WHERE name = 'BOB';      -- finds 'Bob'
SELECT id FROM people WHERE place = 'cafe';    -- finds 'Café'
```

A value is returned as it was written; only comparisons change.

## What follows the collation

- `=`, `<>`, `<`, `<=`, `>`, `>=`, `IN (…)`, `IN (SELECT …)`, `BETWEEN`,
  `LIKE`/`ILIKE`, `IS [NOT] DISTINCT FROM`, `EXISTS` comparisons, in `WHERE`
  of `SELECT`, `UPDATE` and `DELETE` and in a join's `ON`, against a literal,
  a parameter, an expression or another such column;
- `UNIQUE`, `PRIMARY KEY` (`'abc'` and `'ABC'` are one key), `ON CONFLICT
  (column)`, `CREATE [UNIQUE] INDEX`, and a key added by `ALTER TABLE`;
- a `FOREIGN KEY` between two such columns: a child `'abc'` finds its
  parent `'ABC'` (`ON DELETE CASCADE`, `NO ACTION`, `RESTRICT`);
- `GROUP BY` (each group shows its least spelling under the collation),
  `SELECT DISTINCT` of plain columns, `min`/`max`, `count(DISTINCT …)` and
  the other DISTINCT aggregates, over one table or a join;
- `UNION`, `INTERSECT` and `EXCEPT`, whose rows are told apart and
  sorted under each column's collation (a branch may be a join);
- `ORDER BY`, and a window's `PARTITION BY` and `ORDER BY`: by the
  collation, then by the value, so values that compare equal still come
  back in one order;
- `ALTER TABLE … ADD COLUMN c text COLLATE …` (the rows already there get
  their keys) and `ALTER COLUMN c TYPE` to another text type.

The order is by code point of the folded text (lowercased, canonically
decomposed, accents removed for `rdb_ci_ai`), the engine's `C` order of it,
not the Unicode Collation Algorithm's: `'Zoe'` sorts before `'Ännchen'`.
`ß` is not `ss`.

## Refused for now (`0A000`), rather than compared by the bytes

A `UNION`/`INTERSECT`/`EXCEPT` of `*` over such a column (name the
columns), a `HAVING` that
compares `min`/`max` of such a column (it compares their bytes),
`min`/`max` as a window function, a `SELECT DISTINCT` of
such a column beside an expression, a `CHECK` reading one, a partitioned
table with one, a foreign key whose action would write the child's key
(`SET NULL`, `SET DEFAULT`, `ON UPDATE CASCADE`), `ALTER COLUMN TYPE` to a
type that is not text, and giving an existing column one by `ALTER TABLE`.
A regular expression or `SIMILAR TO` over one is refused as PostgreSQL
refuses it. Comparing columns of two different nondeterministic
collations, or one with a column compared by its bytes in a join key, is
`42P22`.

An expression over such a column (`upper(name) = 'BOB'`, `DISTINCT
upper(name)`) compares by its bytes; PostgreSQL carries the column's
collation through it.

## How it works

Each such column has a hidden companion, `__ci_<column>` (`__cia_<column>`
for `rdb_ci_ai`), a stored generated column holding the column's key. A
comparison reads the companion and folds its other side the same way, so the
indexes, unique keys, zone maps and filters work on plain bytes as they
always do. The companion is the record of the collation: the schema's format
is unchanged. It costs the column's bytes once more, for the columns that ask
for it.
