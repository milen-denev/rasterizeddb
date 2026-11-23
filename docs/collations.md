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

- `=`, `<>`, `<`, `<=`, `>`, `>=`, `IN (…)`, `BETWEEN`, `LIKE`/`ILIKE`,
  `IS [NOT] DISTINCT FROM`, in `WHERE` of `SELECT`, `UPDATE` and `DELETE`,
  against a literal, a parameter or an expression;
- `UNIQUE`, `PRIMARY KEY` (`'abc'` and `'ABC'` are one key), `ON CONFLICT
  (column)`;
- `CREATE INDEX` on the column (B-tree or hash), which then finds and orders
  what the collation calls equal and ordered;
- `ORDER BY` the column in a single-table query: by the collation, then by
  the value, so values that compare equal still come back in one order.

The order is by code point of the folded text (lowercased, canonically
decomposed, accents removed for `rdb_ci_ai`), the engine's `C` order of it,
not the Unicode Collation Algorithm's: `'Zoe'` sorts before `'Ännchen'`.
`ß` is not `ss`.

## Refused for now (`0A000`), rather than compared by the bytes

`GROUP BY`, `DISTINCT`, `min`/`max` and `… (DISTINCT …)` aggregates, window
functions, `UNION`/`INTERSECT`/`EXCEPT` (but `UNION ALL` works), a join that
compares such a column (selecting one in a join is fine), a correlated `IN
(SELECT …)` (an uncorrelated one is compared under the collation),
a foreign key or `CHECK` on one, a partitioned table with one, and giving a
column one by `ALTER TABLE` (declare it in `CREATE TABLE`). A regular
expression or `SIMILAR TO` over one is refused as PostgreSQL refuses it.
Comparing columns of two different nondeterministic collations is `42P22`.

An expression over such a column (`upper(name) = 'BOB'`) compares by its
bytes; PostgreSQL carries the column's collation through it.

## How it works

Each such column has a hidden companion, `__ci_<column>` (`__cia_<column>`
for `rdb_ci_ai`), a stored generated column holding the column's key. A
comparison reads the companion and folds its other side the same way, so the
indexes, unique keys, zone maps and filters work on plain bytes as they
always do. The companion is the record of the collation: the schema's format
is unchanged. It costs the column's bytes once more, for the columns that ask
for it.
