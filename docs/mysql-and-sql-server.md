# MySQL and SQL Server compatibility

RasterizedDB serves the MySQL / MariaDB client protocol (`mysql_wire`) and
the SQL Server protocol (`mssql_wire`) beside PostgreSQL's, on the same
databases and roles. This document says how, what each dialect is
translated into, and where the answers would differ from MySQL's or SQL
Server's — which is refused rather than answered differently.

## How it is built

A MySQL or SQL Server connection does not have an engine path of its own.
Each one opens an in-process PostgreSQL session (`pgwire::proxied`) over an
in-memory pipe and talks to it as a PostgreSQL client would. The front end
owns three things:

- **The wire format**: MySQL packets, the handshake, text and binary rows;
  TDS packets, PRELOGIN/LOGIN7, tokens, RPC parameters.
- **The dialect**: a lexer for MySQL or T-SQL and token-level rewrites into
  the engine's SQL (`mysql/translate.rs`, `mssql/translate.rs`). Text is
  never substituted: a keyword inside a string or a quoted name is never
  touched.
- **The session statements** each protocol's drivers send: `SET`, `SHOW`,
  `USE`, `@@variables`; T-SQL's batches, variables and control flow.

Everything else — transaction blocks and their rollback when a connection
drops, savepoints, isolation levels, prepared statements, statement
cancellation, idle timeouts, the privilege gates — is the session pgwire
already runs, so it cannot drift from PostgreSQL's behaviour.

## Common to both

- **Names** are compared case-insensitively (both databases' default), by
  folding them to lower case. A result column is named as it was written in
  the select list (`SELECT UserId` returns `UserId`); `SELECT *` returns the
  names in lower case.
- **Text compares as bytes unless a column says otherwise** (over the
  MySQL protocol; for SQL Server see its section). The servers
  report `utf8mb4_bin` and `Latin1_General_BIN2` as their defaults, and a
  table's default collation (`DEFAULT COLLATE=…`, which `mysqldump` writes on
  every table) is not applied: existing schemas keep the answers they had. A
  *column's* own case-insensitive collation is the engine's
  ([collations](collations.md)): MySQL `…_as_ci` is `rdb_ci`, any other
  `…_ci` (`utf8mb4_0900_ai_ci`, `utf8mb4_general_ci`) is `rdb_ci_ai`; SQL
  Server `…_CI_AI` is `rdb_ci_ai` and any other `…_CI_…` `rdb_ci`. Such a
  column compares, sorts, is unique and is indexed without case, and the
  catalogs report it under its own collation. What the engine refuses for
  one (`GROUP BY`, `DISTINCT`, joins comparing it, …) is listed there.
- **Index names** are per table in both databases and per database here; an
  index made through either protocol is stored as `<table>__<name>` and shown
  under its own name.
- **An error inside a transaction fails only its statement** (each runs
  under a savepoint), as in both databases. A deadlock or serialization
  failure rolls the whole transaction back, as it does there.
- **Errors** carry the database's own numbers (MySQL 1062, 1146, 1054, …;
  SQL Server 2627, 208, 207, …), with the PostgreSQL SQLSTATE at the end of
  the message.

## MySQL / MariaDB

**Connection.** Protocol 4.1 with `CLIENT_DEPRECATE_EOF`, multi-statements
and multi-results; TLS on TCP and on the unix socket. Authentication is
`caching_sha2_password` against the role catalogue: a role's first login
sends its password over a secure channel and is checked against the stored
SCRAM verifier; later logins use the fast scramble against a hash kept in
memory (dropped when the password changes). There is no RSA key exchange,
so a first login over plain TCP is refused with a message saying to use TLS.
A client asking for another plugin (MariaDB's `mysql_native_password`) is
switched to this one.

**Statements.**

| MySQL | Sent as |
|---|---|
| `` `name` ``, `"str"` (no `ANSI_QUOTES`), `'it\'s'`, `x'ff'`, `b'101'` | names, standard strings, `bytea` |
| `LIMIT a, b` | `LIMIT b OFFSET a` |
| `/` | exact division (`a * 1.0 / b`); `DIV` is integer division |
| `<=>`, `&&`, `\|\|` (unless `PIPES_AS_CONCAT`), `!`, `REGEXP` | `IS NOT DISTINCT FROM`, `AND`, `OR`, `NOT`, `~` |
| `TRUE`/`FALSE` | `1`/`0` |
| `AUTO_INCREMENT` | `serial`/`bigserial`; the insert id comes back in the OK packet |
| `INSERT IGNORE`, `ON DUPLICATE KEY UPDATE … VALUES(c)`, `AS new` | `ON CONFLICT [(key)] DO NOTHING / DO UPDATE … excluded.c` |
| `REPLACE` naming every column | `ON CONFLICT DO UPDATE` |
| `UPDATE/DELETE … ORDER BY … LIMIT n` | by primary key in a subquery |
| multi-table `UPDATE a JOIN b`, `DELETE a FROM a JOIN b` | `UPDATE … FROM`, `DELETE … USING` |
| `CREATE TABLE` with MySQL types, `KEY`, `UNIQUE KEY`, `ENUM`, table options | engine types, indexes, a `CHECK` for the `ENUM` |
| `ALTER TABLE … ADD/MODIFY/CHANGE/DROP …, FIRST/AFTER` | one engine `ALTER` per change |
| `IFNULL`, `IF`, `GROUP_CONCAT`, `DATE_FORMAT`, `DATE_ADD`, `JSON_EXTRACT`, `->>`, … | engine functions |
| `SQL_CALC_FOUND_ROWS` + `FOUND_ROWS()` | a count of the query without its `LIMIT` |
| `SHOW DATABASES/TABLES/COLUMNS/INDEX/CREATE TABLE/VARIABLES/…`, `DESCRIBE` | answered from the catalogue |
| `information_schema … TABLE_SCHEMA = 'db'` | the database name, as MySQL tools expect |

`SET autocommit = 0` holds work in a transaction until `COMMIT`; DDL commits
what is open first, as in MySQL. `SET FOREIGN_KEY_CHECKS = 0` holds back
foreign keys a `CREATE TABLE` declares and adds them when it is switched back
on, which is how a `mysqldump` loads; `LOCK TABLES` is accepted and does
nothing. Comment-only queries answer OK.

**Kept by the front end** (the engine has none of these):

- **Stored procedures.** `CREATE [DEFINER=…] PROCEDURE`, `DROP PROCEDURE`,
  `CALL`, `SHOW PROCEDURE STATUS`, `SHOW CREATE PROCEDURE`. The body runs in
  the session: `IN`/`OUT`/`INOUT` parameters (`OUT` into `@variables`),
  `DECLARE`, `SET`, `IF`, `CASE`, `WHILE`, `REPEAT`, `LOOP` with labels,
  `LEAVE`, `ITERATE`, `SELECT … INTO`, cursors, `CONTINUE`/`EXIT` handlers
  (`NOT FOUND`, `SQLEXCEPTION`, `SQLWARNING`, `SQLSTATE`, codes), `SIGNAL`,
  `RESIGNAL`, nested `CALL`. A `CALL` answers with its queries' result sets
  and one OK, as MySQL does. Definitions are kept in the database's
  `__rdb_routines` table.
- **`CREATE TEMPORARY TABLE`**: a table for the connection, which hides a
  table of its name, outlives a rolled-back transaction, and goes when the
  connection closes or is reset. It is a real table under a name of its own
  (`__rdb_tmp_…`), so making one costs what making a table costs.
- **`ON UPDATE CURRENT_TIMESTAMP`**: recorded in `__rdb_mysql_on_update`;
  `UPDATE` and `ON DUPLICATE KEY UPDATE` set the column when an assigned
  value changes and the statement does not set it itself. Only statements
  sent over the MySQL protocol set it.
- **`LOAD DATA LOCAL INFILE`**, with MySQL's field, enclosure, escape and line
  options, a column list and `IGNORE n LINES`; one transaction. A server-side
  file (`LOAD DATA INFILE`) is refused as under `secure_file_priv`.
- **Server-side cursors**: `COM_STMT_EXECUTE` with a cursor type and
  `COM_STMT_FETCH` (Connector/J's `useCursorFetch`).

**Full-text search** is the engine's (`to_tsvector`, `@@`, and a GIN index
over `to_tsvector(…)`). A `FULLTEXT` key (in `CREATE TABLE`, `ALTER TABLE …
ADD FULLTEXT`, `CREATE FULLTEXT INDEX`) is such an index over its columns
under the `simple` configuration, which lowercases and neither stems nor
drops words, as InnoDB's tokenizer does. `MATCH (cols) AGAINST (…)` reads the
same document, so the index answers it:

- natural language mode finds rows with any of the words; boolean mode takes
  `+`, `-`, `*` (prefix), `"phrases"` and `( … )` groups (`>`, `<`, `~` rank
  only, and are read as plain words);
- InnoDB's default stop words and words shorter than 3 or longer than 84
  characters are left out of the search, as InnoDB leaves them out of its
  index;
- where it stands as a condition it is one; read as a number (select list,
  `ORDER BY`, a comparison) it is the row's relevance, 0 when the row does
  not match. The scores are `ts_rank`'s, not InnoDB's, so rows that score
  close can come back in another order;
- a search string bound to a `?` is read at each execute;
- `MATCH` does not need a `FULLTEXT` key over exactly its columns, as InnoDB
  does (error 1191): without one the rows are scanned.

Tables whose names start with `__` are not listed by `SHOW TABLES`; a
PostgreSQL client sees them.

**Refused** (an error, never a different answer): stored functions, triggers
and events (they would run inside the engine's queries), `SPATIAL` indexes,
full-text `WITH QUERY EXPANSION` and `WITH PARSER`, `PARTITION BY`, `CREATE TABLE … LIKE/SELECT`,
`XOR`, `LOAD DATA … SET`, a `REPLACE` that leaves columns out (MySQL resets
them to their defaults), `SELECT @v = @v + …` over more than one row (MySQL
folds it row by row; use `GROUP_CONCAT` or `SUM`), `sql_select_limit`, and
character sets other than `utf8mb4`/`utf8`/`ascii`/`binary`.

`BIGINT UNSIGNED` is a `bigint` (values past 2^63 are refused).

## SQL Server

**Connection.** TDS 7.4 with TLS carried inside PRELOGIN (each TDS packet
its own TLS record, as SQL Server sends them), or TDS 8.0 (`Encrypt=Strict`:
TLS first, ALPN `tds/8.0`; the client validates the certificate, so use a
real one or pin it). A client that asks for encryption gets the whole
connection encrypted; login-only encryption is not offered, so under
`--mssql-tls prefer` a client that declines encryption is served without it
(and sends its password in the clear), and under `require` it is refused.
The unix socket has no TLS. SQL Server authentication only.

**Requests.** SQL batches; RPC calls `sp_executesql`, `sp_prepare`,
`sp_prepexec`, `sp_execute`, `sp_unprepare`, `sp_reset_connection` (typed
parameters are bound through the engine, never written into the SQL);
transaction-manager requests (`SqlConnection.BeginTransaction`); the
reset-connection flag of pooled connections; attention.

**Batches** run as SQL Server runs them: statements need no `;`, an error
ends its statement and the batch goes on (unless `XACT_ABORT`), and
`DECLARE`/`SET @v`/`SELECT @v = …`, `IF … ELSE`, `BEGIN … END`, `WHILE`,
`BEGIN TRY … BEGIN CATCH` (with `ERROR_MESSAGE()` and friends), `PRINT`,
`RAISERROR`, `THROW`, `RETURN`, `EXEC('…')` and `EXEC sp_executesql` work.
`BEGIN TRAN` nests (`@@TRANCOUNT`) and the client is told of each
transaction with ENVCHANGE; `SET IMPLICIT_TRANSACTIONS ON` and
`SET TRANSACTION ISOLATION LEVEL` behave as there.

| T-SQL | Sent as |
|---|---|
| `[name]`, `"name"`, `N'…'`, `dbo.`/`db.dbo.` | names (the engine's one schema), strings; a dot inside a bracketed name (`[DenevCloud.ERP.Colors]`) is the name's own, stored as `․` (U+2024) and shown as `.` |
| `CROSS APPLY (…) t`, `OUTER APPLY (…) t` | `CROSS JOIN LATERAL`, `LEFT JOIN LATERAL … ON true` |
| `TOP (n)`, `OFFSET … FETCH` | `LIMIT … OFFSET …`; `UPDATE/DELETE TOP` by primary key |
| `a + b` with a string side | `\|\|` |
| `IDENTITY(1,1)`, `SCOPE_IDENTITY()`, `@@IDENTITY` | `serial`, the captured value |
| `OUTPUT INSERTED.x` / `DELETED.x` | `RETURNING x` |
| EF Core's `MERGE … ON 1=0 WHEN NOT MATCHED THEN INSERT … OUTPUT` | a multi-row `INSERT … RETURNING` |
| `UPDATE a SET … FROM t a JOIN …`, `DELETE p FROM t AS p` | `UPDATE t AS a … FROM … WHERE`, `DELETE FROM t AS p` |
| `ISNULL`, `LEN`, `GETDATE`, `DATEADD`, `DATEDIFF` (boundaries), `CONVERT(…, style)`, `IIF`, `CHARINDEX`, `COUNT` (an `int`), … | engine functions |
| `WITH (NOLOCK)`, `OPTION (…)`, `ON [PRIMARY]`, `CLUSTERED` | dropped |
| `sys.tables/columns/objects/indexes/types/schemas/databases`, `INFORMATION_SCHEMA.TABLES/COLUMNS`, `OBJECT_ID`, `SERVERPROPERTY`, `@@VERSION` | built from the catalogue |

`master`, `model`, `msdb` and `tempdb` are the default database, so a
client can create and drop its own (`EnsureCreated`, `EnsureDeleted`).
Database names are found without case (`CREATE DATABASE [DenevColors]`
makes `denevcolors`, which a login naming `DenevColors` opens), and a
pooled connection's reset returns to the login's database after a `USE`.

**Text is case-insensitive by default**, as under SQL Server's default
collation: a text column a `CREATE TABLE` or `ADD COLUMN` makes without a
`COLLATE` takes the engine's `rdb_ci` ([collations](collations.md)), so
`=`, `LIKE`, `IN`, keys, foreign keys, `GROUP BY`, `DISTINCT`, joins,
`ORDER BY` and windows ignore case. Each such column carries a hidden key
column beside it. `--mssql-collation bin` makes such columns compare by
their bytes instead. What the engine still refuses over one is listed
there.

**EF Core**: EF Core 10's SqlServer provider runs unchanged: `APPLY`
shapes (`Take`/`First` in projections and filtered `Include`), batched
inserts, `EnsureCreated`/`EnsureDeleted`, ASP.NET Identity's schema,
`Migrate()` with its `sp_getapplock` lock, idempotent scripts,
`RenameIndex`, `DropColumn`/`AlterColumn` (`sys.default_constraints` has
the columns their scripts read), savepoints under `EnableRetryOnFailure`.
A unique index filtered by `key IS NOT NULL [AND …]` over its own key
columns (what EF puts on every unique index over a nullable column) is
the plain unique key, since a unique key here never compares NULLs equal;
a filtered index that is not unique is built over every row.

**Kept by the front end** (the engine has none of these):

- **Stored procedures**: `CREATE [OR ALTER] / ALTER / DROP PROCEDURE`, `EXEC`
  with positional and named arguments, defaults, `OUTPUT` and the return
  status, and calls by RPC (`CommandType.StoredProcedure`, with `OUTPUT` and
  return-value parameters). Each call has its own variables; `#tables` it
  makes go when it returns; nesting stops at 32. Procedures show in
  `OBJECT_ID`, `sys.objects`, `sys.procedures`, `sys.sql_modules` and
  `INFORMATION_SCHEMA.ROUTINES`. Definitions are kept in `__rdb_routines`.
- **`#temp` tables, `##global` ones and table variables** (`DECLARE @t
  TABLE`): real tables named per connection (`__rdb_tmp_…`), dropped when
  their batch, procedure or connection ends, or when a pooled connection is
  reset. Making one costs what making a table costs. A table variable is
  rolled back with a transaction here, where SQL Server keeps its rows.
- **Cursors**: `DECLARE … CURSOR [LOCAL] [SCROLL|STATIC|FAST_FORWARD] FOR
  SELECT`, `OPEN` (which reads the rows), `FETCH NEXT/PRIOR/FIRST/LAST/
  ABSOLUTE/RELATIVE [INTO]`, `@@FETCH_STATUS`, `CLOSE`, `DEALLOCATE`.
  Updatable cursors (`FOR UPDATE`, `WHERE CURRENT OF`) are refused.
- **`MERGE`** with `WHEN MATCHED [AND …] THEN UPDATE/DELETE`, `WHEN NOT
  MATCHED [BY TARGET] THEN INSERT`, `WHEN NOT MATCHED BY SOURCE [AND …]`,
  from a table, a subquery or `VALUES`: run atomically as statements the
  engine has; a target row matched twice is error 8672.
- **`FOR JSON PATH/AUTO`** (`ROOT`, `INCLUDE_NULL_VALUES`,
  `WITHOUT_ARRAY_WRAPPER`, nested `FOR JSON`) and **`FOR XML PATH`** (with
  `TYPE` and `.value('.', …)`, which `STUFF((… FOR XML PATH('')), 1, n, '')`
  string joins use).
- **Bulk load**: `INSERT BULK` and the bulk-load message `SqlBulkCopy` and
  `bcp` send, in one transaction; `SET FMTONLY ON` answers a query's
  columns as the table declares them.

**Full-text search** is the engine's. `CREATE FULLTEXT INDEX ON t (cols) KEY
INDEX …` makes two GIN indexes over the columns' text: `simple` (words as
written, without case) for `CONTAINS`, `english` (stemmed, stop words out) for
`FREETEXT` and `FORMSOF(INFLECTIONAL, …)`. Catalogs, stop lists and
`ALTER FULLTEXT INDEX … ENABLE / START … POPULATION / SET CHANGE_TRACKING` are
accepted and change nothing: the index is current after every write.

- `CONTAINS(col | (cols), condition)`: words, `"phrases"`, `"prefix*"`,
  `AND`/`&`, `OR`/`|`, `AND NOT`/`&!`, parentheses, `FORMSOF(INFLECTIONAL,
  …)`, `FORMSOF(THESAURUS, …)` (the words themselves: there is no
  thesaurus), `NEAR((a, b), n [, TRUE])` with `n` up to 20 or no distance,
  `a NEAR b`, `ISABOUT(…)` (weights rank only);
- `FREETEXT(cols, text)`: any of the words, stemmed;
- `CONTAINSTABLE` / `FREETEXTTABLE (table, cols | *, …, [top_n])`: `KEY`
  (the primary key) and `RANK` (0–1000, from `ts_rank`, not SQL Server's
  statistic);
- the condition may be a literal, a variable or a parameter.

There is no noise-word list for `CONTAINS` (`"the"` finds rows with "the"),
and `CONTAINS(*, …)` is refused (name the columns; the table-valued forms take
`*`). `ALTER FULLTEXT INDEX … ADD/DROP` is refused: drop the index and create
it again.

**Application locks**: `sp_getapplock`, `sp_releaseapplock` and
`APPLOCK_TEST`, per database and in this server only, owned by the
connection or its transaction and released when either ends.

**Refused**: `MERGE … OUTPUT` (other than EF Core's insert form) and a `MERGE`
that updates a column its `ON` reads, `FOR XML RAW/AUTO/EXPLICIT`, stored
functions and triggers, `rowversion`, computed columns, unique indexes
with any other filter, `APPLY` of a table-valued function, `OPENJSON`,
`SET ROWCOUNT`, `TOP … PERCENT/WITH TIES`, `OUTPUT … INTO`, `SELECT @v = @v
+ … FROM` over more than one row, and Windows authentication.
`TRY_CAST`/`TRY_CONVERT` raise the conversion error where SQL Server would
return NULL.

## Verified with

`mysql` 8.4 and `mariadb` 11.4 clients (TCP, TLS, unix socket, `LOAD DATA
LOCAL INFILE`), a `mysqldump` from MySQL 8.4 loaded as is, sqlx-mysql
(prepared statements, binary rows, procedures), MySQL Connector/J 9.1
(server-side cursors), SQLAlchemy 2 + PyMySQL (ORM), Microsoft.Data.SqlClient
7.1 (`SqlBulkCopy`, stored procedures by RPC), Dapper, EF Core 10, tiberius,
and ODBC Driver 13 `sqlcmd`. `rasterizeddb_compat/tests/mysql_protocol.rs`
and `mssql_protocol.rs` run in the compat suite; the client harnesses are in
`.claude/reviews/wire-compat`.

## Not offered

Applying a table's or database's default case-insensitive collation to its
columns (MySQL's and SQL Server's own default behaviour): it would change the
answers of every schema loaded so far, and the engine still refuses grouping
and joins over such columns.
