# rdb_studio

A database console for RasterizedDB — and, over the same wire protocol, for
PostgreSQL.

```sh
cargo run -p rdb_studio                # pick a connection
cargo run -p rdb_studio -- studio-demo # open a saved profile straight away
```

## What it does

| Screen | What it answers |
|---|---|
| **Dashboard** | What is the database *doing*, as a live line per metric, plus where the space is going |
| **Overview** | Every statistic the server reports, grouped, with the six that matter as tiles |
| **Tables** | Every table with its sizes and what is holding it — memory, files, S3 — and one table's columns, constraints and statistics |
| **Schema** | The foreign keys as a layered map, each line anchored at the column it joins |
| **Data** | A page of rows, editable in place, with insert and delete |
| **Query** | Several statements at a time, each reporting separately, with timings and a history you can prune |
| **Activity** | What the *server* has been running, what each cost, how many rows it had to read to answer, and how much of the time went on relations it had to build first |
| **Bench** | How fast one statement runs: queries per second and end-to-end latency (mean, median, p90/p95/p99/p99.9, min, max) over N connections of its own, with warm-up, simple or prepared protocol, and runs kept side by side to compare |
| **Backup** | What is stored in S3, whether archiving is healthy, and restoring from it |
| **Storage** | Which directories the database writes into, how full each is, and which one holds each table |
| **Replication** | Whether it is on, who it is talking to, and how far behind they are |
| **Migrate** | Importing a live PostgreSQL database into this one |
| **Config** | Every runtime knob, with the ones a running server can change actually editable |

Databases and tables are created and dropped from the sidebar; columns are added
from the table detail pane.

## The two dialects

Both servers speak the PostgreSQL v3 wire protocol, so one transport serves
both. What differs is *how you ask a database about itself*, and that is the
whole of [`src/dialect.rs`](src/dialect.rs):

- **RasterizedDB** answers a statement family added for this:
  `SHOW DATABASE STATS`, `SHOW TABLES`, `SHOW TABLE STATS <t>`,
  `SHOW COLUMNS FROM <t>`, `SHOW SCHEMA MAP`, `SHOW DATABASES`,
  `SHOW CONFIG`, `SHOW CONFIG KEYS`, `SET CONFIG <key> = <value>`,
  `SHOW BACKUP STATUS`, `SHOW REPLICATION STATS`,
  `SHOW REPLICATION PEER STATUS`, `SHOW QUERIES`, `SHOW QUERY STATS` and
  `RESET QUERY STATS`. See
  [docs/introspection.md](../docs/introspection.md).
- **PostgreSQL** answers the equivalent `pg_catalog` / `pg_stat_*` queries.

Both are written to return **the same column names**, so everything above the
dialect layer is written once. The dialect is chosen when you save the
connection; the console asks the server one question at connect time and warns —
in a banner that stays up — when the answer disagrees with what you chose.

Two figures have no PostgreSQL equivalent and are marked rather than faked:
`reclaimable_bytes` is estimated from `n_dead_tup`, and `filters_ready`
describes an index only RasterizedDB has.

The activity screen is the sharpest case of the same rule, because there the
*question* differs and not just a figure. RasterizedDB keeps a ring of finished
statements; PostgreSQL keeps none, and the nearest thing it has —
`pg_stat_activity` — is what its backends are executing at this instant. So that
pane is headed "RUNNING NOW · THIS INSTANT, NOT A HISTORY" there, and the two
columns nobody can answer for a statement that has not finished — rows returned,
rows scanned — show as dashes rather than zeroes. The lower pane is
`pg_stat_statements`, which genuinely is the equivalent of `SHOW QUERY STATS`,
and the page says so plainly when the extension is not loaded instead of
rendering an empty table.

## The query screen

Beyond running SQL:

- **Run selection** for working through a scratch buffer of several statements.
- **Format** — clauses onto their own lines, keywords upper-cased. It only ever
  touches bare words: string literals, quoted identifiers and comments are
  copied through byte for byte, so `WHERE note = 'select from where'` survives.
- **Templates** for the selected table — `SELECT`, `COUNT`, `INSERT`, `UPDATE`,
  `DELETE` — written with its real column list. The write templates always carry
  a `WHERE`, because a skeleton `UPDATE` without one is a whole-table write
  waiting for somebody to press run before finishing the thought.
- **Open and save `.sql` files** through the desktop's own file chooser, and
  **copy or export results** as CSV, JSON, Markdown or `INSERT` statements.
- **Export the session history** as a runnable script.

### The editor

SQL is coloured as you type: keywords, types, functions, strings, numbers and
comments each get their own colour, and a name that is a table in the connected
database — or a column of the selected one — is coloured as one, which is also
the quickest way to notice that a name is *not*.

It is checked as you type, too. What the checker finds is marked in the text and
listed under the editor; clicking a finding puts the cursor on it. **Errors** are
things the server will refuse anyway — an unclosed string, comment or bracket, a
stray comma. **Warnings** are the ones the server will run happily:

- `UPDATE` or `DELETE` with no `WHERE`, which touches every row;
- `= NULL` / `<> NULL`, which is never true and so silently matches nothing;
- a statement starting on a new line with no `;` after the one before it;
- a near-miss keyword — `FORM`, `WHRE`, `SELCT`;
- against RasterizedDB, a table name that is not in the database. (Not against
  PostgreSQL, whose table list leaves out views.)

Every rule is written to stay quiet when unsure, and none of them ever stops a
statement from running — the server is the authority.

| Keys | Does |
|---|---|
| Ctrl+Enter | runs everything |
| Ctrl+Shift+Enter | runs only the statement the cursor is in |
| Enter | a new line at the same indentation, one deeper after `(` |
| Tab / Shift+Tab | indent / unindent — the selected lines, if any |
| Ctrl+/ | comments the lines out, or back in |
| Ctrl+Shift+F | formats |
| Ctrl+S | saves to a `.sql` file |

### Copying and exporting

Every result set can go to the clipboard or to a file, in four shapes. CSV keeps
the `NULL`/empty-string distinction the way the format allows — an unquoted
empty field is `NULL`, a quoted one is the empty string — JSON says it properly,
and Markdown marks it `∅` because it cannot. `INSERT` output is one statement
per row, so a row a target rejects fails on its own instead of taking the batch
with it.

The data browser exports **the page you are looking at**, and names the file for
it (`users-page-3.csv`). Exporting page 3 and getting `users.csv` invites you to
think you have the table; to export a whole one, run a `SELECT` and export that.

Exported history is a runnable script, with each statement's duration and result
as a comment — and **statements that failed are commented out**. They are in the
file because they are part of the story, not because they should happen again.

Dialogs go through the XDG desktop portal, so it is the desktop's own file
chooser. Dismissing one is a decision, not a failure, and is reported as
nothing at all.

### Timings

Every figure is a **round trip**: sent to answered, labelled as one. It includes
the network and the driver, so it is not the server's execution time and is
never presented as one — over loopback the difference is microseconds, over a
link to another continent it is most of the number. Statements sent together are
one exchange and get one figure between them rather than a fabricated split.

### History

Everything the session sent — typed, or issued by a dialog, a backup action or a
config change — with its duration, its outcome and how long ago. Clicking an
entry **loads it into the editor and does not run it**: the list holds the
session's `DROP` and `DELETE` statements as readily as its `SELECT`s. Running is
a separate button. Entries can be copied, deleted one at a time, or cleared.

## Migrating from PostgreSQL

Drives the engine's `IMPORT DATABASE FROM POSTGRES`: it reads a live PostgreSQL
server and builds a new database here from what it finds — tables, columns,
types, nullability, defaults, primary keys, unique constraints, foreign keys,
then every row.

One direction and one shot. It does not write back, it does not stay in sync,
and it refuses a target that already holds tables.

**Preview is on by default.** `DRY RUN` works the whole thing out and writes
nothing, and the report it returns is the point: alongside the table list and
row counts it ends with everything that would not come across unchanged —
sequences that are not imported, `NUMERIC` losing its declared scale, a `jsonb`
column becoming text, `NOT NULL` carried onto the column but not enforced on
later writes. Making somebody opt *out* of reading that first is the wrong way
round. A real import asks once more before it runs.

This is deliberately not a versioned-migrations screen. There is no folder of
numbered `.sql` files and no schema-version table, because the engine has no
such concept and inventing one in a console would mean migration state that only
this console knows about. Versioned migrations belong with the schema they
describe — the query editor opens a `.sql` file and runs it.

The statement carries the source's password, so the copy filed in the history
has it blanked.

## Backups

Everything the S3 backup surface offers, with the destructive parts behind a
confirmation that names what they will do:

- **Backup now** takes a full snapshot; **archive WAL** cuts the point-in-time
  barrier; **prune** applies the retention policy and is the only one that
  deletes anything, so it says how many snapshots would go and that S3 deletions
  are not recoverable.
- **Restore** writes a complete database into a directory you name. Every
  recovery target except *Latest* deliberately leaves out the commits after the
  point it names, which the page warns about before and reports afterwards as
  `records_discarded`.
- **Restoring over the live database is not possible from here** — swapping
  files under open tables leaves every reader looking at storage that no longer
  describes what it thinks it does, so the engine refuses it from SQL. The page
  prints the start-up command that does it instead, filled in from the
  destination and the target already chosen.

Three states are distinguished rather than collapsed: a build without the
feature, a build with no destination configured, and a configured one. A fourth
— a server too old to answer `SHOW BACKUP STATUS` — falls back to the catalogue,
because a catalogue that lists objects proves a destination exists whatever else
the server will say.

## Replication

Node state, the counters, and a row per peer with its lag, queue depth and last
error. The two servers replicate in opposite directions and the page says which
it is looking at: RasterizedDB peers are nodes this one *sends to* and lag is in
transactions; PostgreSQL rows are standbys *receiving* from here and lag is in
bytes of WAL.

A paused peer is shown as paused rather than as a fault. Enable, disable, add,
pause, resync and drop are available against RasterizedDB, each destructive one
explaining what it costs. PostgreSQL is read-only here, because its replication
is configured in `postgresql.conf` and by whoever connects, not from SQL.

## Storage

Which directories the database may write into, a row each with its role, how
many tables it holds and how much room is left; and a row per table saying which
directory holds it and whether its writes are reaching a backup copy.

Against RasterizedDB the page is read-write. The placement policy — `primary`,
`round-robin` or `free-space` — decides where the *next* table is created, and
changing it moves nothing; that is what the rebalance is for, and it has a dry
run whose plan the page shows before anything is copied. A directory can be
weighted, drained (it keeps its tables and takes no new ones), turned into a
backup, or removed once it is empty. One table can be moved on its own.

Two conditions get a banner rather than a colour in a column, because they are
the ones worth interrupting someone about:

- **a primary below the free-space floor** — new tables will not go there, and
  the ones already there have nowhere to grow;
- **a table not reaching a backup the pool has** — whether a file is mirrored is
  settled when it is opened, so a backup location added while the server was
  running does not cover tables that were already open, and the restart that
  fixes it is not obvious from anywhere else.

A draining location and a backup location are both shown as what they are and
not as faults. Free space is taken from the server rather than summed from the
column: two directories on one filesystem report the same bytes, and only the
server can tell that they are the same filesystem.

Against PostgreSQL the same page reports tablespaces, read-only — a table goes
where the statement or `default_tablespace` says, nothing mirrors, and nothing
moves on its own. Free space is shown as unknown rather than as zero, because
PostgreSQL exposes no per-tablespace figure and a made-up one is worse than a
dash.

A server built without the `storage_pools` feature answers `SHOW STORAGE STATS`
as a runtime parameter rather than refusing it — the same trap the backup and
replication pages have — so the page distinguishes "one directory" from "the
server would not say", and names the build flag for the first.

## Editing rows safely

Every in-place edit is addressed by a column that identifies exactly one row,
worked out when the edit begins: the declared primary key, or failing that any
`UNIQUE NOT NULL` column — which is what keeps tables created before the engine
recorded primary keys editable. **A table with neither is read-only**, and the
browser says so and says what would fix it. The alternative — matching an
`UPDATE` on the row's own values — writes to every duplicate of the row that was
clicked, reports success, and is undetectable afterwards.

Columns are dragged to width from the divider in the header, remembered per
table, and double-clicked back to the default.

For the same reason a page is always ordered by that key. Without an `ORDER BY`
neither engine promises the same order twice, so page 2 can repeat a row from
page 1 and skip another.

Clicking a header in the data browser sorts the **table**, not the page: the
statement goes back to the server with an `ORDER BY` in front of the key and
comes back from page one. Sorting the page in hand would rearrange the hundred
rows that happened to arrive under a heading claiming to order the whole thing.

## Columns

Every grid in the console is the same four pieces — resolve the widths, draw the
header and its handles, lay a row out against those widths, wrap it in the
scrolling and drag plumbing. So the table list, the data browser, query results,
the backup catalogue, the replication peers, the column list, the configuration
and every statistics panel all resize the same way: drag the divider in a
header, double-click it to go back to the default.

Widths are remembered per grid and, where the columns belong to a table, per
table — two grids with a `name` column do not share its width, and neither do
two tables.

Clicking a header sorts by it; clicking again reverses it; a third click puts
the grid back into the order the server returned. That third state is not
decoration: a re-sorted grid shows a different order from the one the statement
produced, and there has to be a way back to it without reconnecting. Rows are
ordered by what the figure *is*, not by how it was printed — `1.2 GiB` sorts
above `900 MiB` — and a row with nothing in the sorted column goes to the
bottom in both directions, because "missing" has no place in an order. Rows the
column cannot tell apart keep the order they arrived in.

Columns holding a button or a row of badges have nothing to order by and do not
respond to a click. Sorting the configuration list flattens its sections, since
the sections are an ordering of their own and keeping them would produce a page
of one-row groups; the section name is the first half of every key in the
column beside it, so nothing is hidden.

They are separate pieces rather than one `grid(columns, rows)` call because the
screens genuinely differ: the table list makes each row a button, the catalogue
puts an action in a cell, a peer that is down grows a second line for its error.
What has to be shared is the *alignment* — a header and a body computing widths
separately is a bug waiting for the first column somebody drags.

## Choosing a type

The type field is a list, not a text box, because the two servers do not accept
the same set and a text box asks you to remember which. Each entry carries what
the server will actually store — that RasterizedDB keeps every `NUMERIC` at a
fixed scale of 9 whatever you write, that `VARCHAR(n)`'s limit is enforced by
one server and not the other, that a `JSONB` document is mapped onto a shadow
table one key per column. Types that take parameters ask for them and only then.

Typing a column name suggests a type — `created_at` is a timestamp, `total` is a
numeric, `is_active` is a boolean, `id` is whatever that server keys tables with
— and picking one stops it suggesting.

## Configuration and hot reload

The server says which settings it can change while running — RasterizedDB
through `SHOW CONFIG`'s `hot_reload` column, PostgreSQL through
`pg_settings.context`. Those get an edit box and an `APPLY`. The rest are shown
with their value, greyed, and the flag or file that actually changes them.

A RasterizedDB change is applied by the subsystem that owns it and is **not**
written to disk: it is gone after a restart unless the matching flag is added
too. A PostgreSQL change goes through `ALTER SYSTEM SET` and `pg_reload_conf()`,
so it survives one.

## The live dashboard

One statement per tick — the same statistics query the overview runs — folded
into a ring buffer per series. Sampling stops when the tab is not on screen or
when paused, so a console left open costs nothing.

Counters (`xact_commit`, the transaction id) are plotted as their **slope**: a
counter itself is a straight line with no information in it. The first reading
of a counter establishes a baseline and plots nothing, and a counter that goes
backwards — a restarted server — reads as zero rather than as a negative rate.

## Performance

Two benchmarks live in `examples/`, because the costs that matter here are not
visible in a unit test:

```sh
# What one frame of the widget tree costs, per screen, and whether it leaks.
RDB_STUDIO_TEST_RDB=... cargo run -p rdb_studio --release --example view_cost

# What a query costs end to end, to put beside psql's figure for the same one.
RDB_STUDIO_TEST_RDB=... cargo run -p rdb_studio --release --example query_cost -- "SELECT 1"
```

`view()` runs on every redraw, so anything it allocates is paid sixty times a
second and anything it leaks is paid forever. Current cost, release build,
against a 200-row page of twelve columns: 0.06–0.8 ms per screen, no growth.

Three things came out of measuring rather than guessing:

- **The result grid leaked 0.47 KiB per frame** — about 100 MB an hour with the
  window open. `GridColumn::key` was `&'static str`, which meant `Box::leak` for
  a result set's column names in a function the runtime calls every redraw. It
  is a `Cow` now.
- **A five-thousand-row page cost 20.7 ms a frame**, past the whole frame budget
  before anything was drawn. The data grid only builds the rows in view now,
  with spacers standing in for the rest so the scrollbar stays honest: 0.38 ms,
  a 55× improvement. Below three hundred rows it does not bother.
- **`elide` allocated a `String` for every cell of every grid on every frame**,
  whether or not the value contained a newline. It borrows now.

The schema map keeps its tessellated geometry in a `canvas::Cache` and rebuilds
it only when the map moves or the schema changes.

## Testing

```sh
cargo test -p rdb_studio                       # unit tests

# Everything the dialect layer sends, against real servers. Each is optional.
RDB_STUDIO_TEST_PG=postgresql://user:pw@host:5432/db?sslmode=disable \
RDB_STUDIO_TEST_RDB=postgresql://user@host:5432/postgres?sslmode=disable \
  cargo test -p rdb_studio --test live_dialect
```

The live test is what catches the failures unit tests cannot: a `pg_catalog`
column removed in a newer PostgreSQL, a cast that needs quoting, an engine that
refuses a string literal against an integer key. It writes only to a table it
creates and drops.

## Connections on disk

`~/.config/rdb_studio/connections.json`, owner-readable only. A password is
written only when the profile asks for it.

## What TLS does here

`require` encrypts and **does not authenticate the server**: RasterizedDB
generates a self-signed certificate when none is configured, and verifying it
would refuse the server you just started. This protects against reading the
traffic, not against a server impersonating the real one. The connection screen
says so under the setting.
