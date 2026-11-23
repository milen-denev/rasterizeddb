# Role system: plan

Status: **plan, nothing built.** Written 2026-11-23 against local `dev` at
`21a4cc83`. Anchors are relative to `rasterizeddb_core/src/` unless a crate
is named. It lives on branch `wip/roles` in `.claude/worktrees/roles` until
the owner decides to merge it. **No slice starts until the owner says so.**

**Licence and build.** The whole role system is **GPL-3.0-only and
standard**: always in the default build, with no licensed part. Its
security core cannot be compiled out. The optional extensions are Cargo
features in the default list, dormant until used (§8). The licensed
service and enterprise features (sharding plan §4.10, §10) build on its
primitives and never replace them.

## 0. Why: the state today

Roles live in an ordinary table, `rdb_roles(name, password, can_login,
is_superuser)` (`auth.rs:639-656`). Grants, owners, memberships and default
privileges all share one table, `__rdb_acl`, keyed by bare names
(`core/rql/privileges.rs:1-29`). The design is PostgreSQL-shaped, and the
grant grammar is a fair start: table privileges, PUBLIC, WITH GRANT OPTION,
`OWNER TO`, default privileges for tables. The enforcement is not.

**Critical: five ways for any login role to get past every privilege.**
The first was reproduced live on 2026-11-23; the others are read from code.

| # | Hole | Anchor |
|---|---|---|
| C1 | `rdb_roles` is readable and writable by every role. `UPDATE rdb_roles SET is_superuser = true` makes you superuser. `SELECT` leaks every verifier, and an md5 verifier logs in directly (pass-the-hash) | `privileges.rs:392-395` (no owner → allowed); `auth.rs:221` |
| C2 | `__*` tables are skipped by the check, and `DROP TABLE __rdb_acl` is not guarded. Delete the owner rows, and `check` then allows everything. `__rdb_views`, `__rdb_functions` and the trigger catalogue are writable too | `pgwire/privilege_statements.rs:252-254`, `:215` |
| C3 | A view reads any table. `CREATE VIEW` never checks the creator's rights on the tables it reads, and `check` runs before views are expanded | `pgwire/view_statements.rs:236-252`; `privilege_statements.rs:205-208` |
| C4 | Unowned relations can be taken over. `OWNER TO` on any relation without an owner (every legacy table, `rdb_roles`, `__rdb_acl`) is allowed, and so is `may_grant` | `privilege_statements.rs:186-188`; `privileges.rs:411-412` |
| C5 | Checks fail open: an error loading the ACL, a relation with no owner, or a database with no owner rows all mean "allowed" | `privilege_statements.rs:214-215` |

**High.**
- The required privilege is read from the statement's first word, so
  `WITH d AS (DELETE …) SELECT` needs only SELECT, and so does `SELECT …
  FOR UPDATE`.
- Nothing checks ownership of views, sequences, functions, triggers,
  domains or types. Anyone can drop another role's view, or put a trigger
  on another role's table that then runs as whoever writes to it.
- `CREATE ROLE` finds options by substring: `IN ROLE superusers` makes a
  superuser (`auth.rs:1003-1011`).
- Role-name case differs between role statements and GRANT.
- `DROP ROLE` leaves the role's rows in the ACL, so a new role with the
  same name inherits them.
- `trust`, and native authentication with no credential, make every
  session an administrator (`pgwire.rs:2396-2401`,
  `database/mod.rs:4711-4743`).
- Superuser of *any* database administers the whole process (the
  `pgwire.rs:2411-2456` verbs).

**Missing compared with PostgreSQL:**
- role options: SET ROLE, NOINHERIT, ADMIN OPTION, CREATEROLE, CREATEDB,
  CONNECTION LIMIT, VALID UNTIL, ALTER ROLE SET/RENAME, changing your own
  password;
- grants on sequences, functions, types and databases (accepted today, then
  ignored), column privileges, REVOKE CASCADE, default privileges for other
  object kinds;
- REASSIGN OWNED and DROP OWNED, SECURITY DEFINER, row-level security;
- `pg_authid`, `pg_auth_members`, `relacl`, `information_schema` privilege
  views, `has_*_privilege`.
- Docs: `docs/postgres-compatibility.md:446`, `:1241-1257` are out of date.

**Cost.** For a non-superuser, every statement (learned prepared ones
included) spawns a task and runs a full `SELECT` of `__rdb_acl`, decoding
every row into five `String`s (`privileges.rs:279`, `:295-318`). On native,
that statement leaves the fast path. There is no cache.

## 1. Principles

1. **The catalogue belongs to the engine.** Roles, memberships, ACLs and
   policies are system relations that user SQL can never read or write.
   They change only through role and grant statements, and are seen only
   through masked views. Verifiers are never readable through SQL at all.
2. **Fail closed, everywhere.** Missing ownership, a load error or an
   unknown object kind means **deny**, with an error. A database upgraded
   from today gets its unowned objects assigned to an owner by a one-time
   migration (§3), not by an "allowed if unowned" rule.
3. **One decision function, at object resolution.**
   `authorize(principal, action, object)` is called where the engine
   resolves each relation, function and sequence a statement touches. That
   is never the statement's first word or text, so CTEs, subqueries, views,
   triggers, COPY, MERGE, DO blocks and every protocol meet the same check.
4. **Identity by id, not name.** Roles and objects get stable ids. Renames
   keep grants, and a dropped role's rows cannot be inherited.
5. **Fast.** An immutable, generation-numbered snapshot per database,
   shared by `Arc`. A check is hash lookups on bitsets, with no allocation,
   no catalogue query and no task spawn (§6).
6. **PostgreSQL semantics and SQLSTATEs**, checked against PG17 with the
   differential harness (`.claude/reviews/pgdiff`). Where the engine
   deliberately differs, the docs say so.

## 2. The model

| Part | Contents |
|---|---|
| **Roles** (`__rdb_authid`) | `role_id u64` (never reused); `name`, folded like every identifier (folding is resolved, `identifiers-not-case-folded`); attributes `LOGIN`, `SUPERUSER` (**this database only**), `CREATEDB`, `CREATEROLE`, `INHERIT`, `BYPASSRLS`, `REPLICATION`, `CONNECTION LIMIT`, `VALID UNTIL`; `verifier` (SCRAM-SHA-256 by default; md5 only where the server allows md5; never plaintext); per-role session defaults (`ALTER ROLE … SET`) |
| **Process admin** | An rdb attribute, `PROCESS ADMIN`, valid **only in the operator (default) database**. It replaces "superuser anywhere administers the process". Every process-wide verb requires it (`CREATE/DROP DATABASE`, `SET CONFIG`, `STORAGE`, `BACKUP`/`RESTORE`, `REPLICATION`, `IMPORT`, `KV CONFIG/SAVE/FLUSHALL`). The bootstrap user (`--pg-user`) gets it. This is the GPL primitive the licensed tenant mode builds on |
| **Membership** (`__rdb_auth_members`) | `(role_id, member_id, grantor_id, admin_option, inherit_option, set_option)`, as PostgreSQL 16+ |
| **Objects** | A stable id for each table (uid exists), view, sequence, function or procedure, type, domain, and the database itself; owner id on every object |
| **ACL** (`__rdb_acl` v2) | Per object: `(grantee_id \| PUBLIC, grantor_id, privileges: u16 bitset, grant_options: u16 bitset)`, plus a per-column ACL for SELECT, INSERT, UPDATE and REFERENCES. One row per (object, grantee, grantor), not per privilege |
| **Default privileges** | Per (owner role, object kind) |
| **Policies** (R3) | `__rdb_policy`: per table, `(name, command, permissive/restrictive, roles, USING expr, WITH CHECK expr)` |
| **Privilege registry** | Kinds are registered, not hard-coded. The GPL set is PostgreSQL's plus `KV` on a database (KV data commands, granted to PUBLIC by default for compatibility). Licensed crates register theirs through a hook, e.g. the ledger's `LEDGER ADMIN` |

**Each database keeps its own catalogue**, as today. Roles belong to a
database, which is what lets one database's credentials never open another.

## 3. Upgrade from today

- **Opening an old directory migrates once, in one transaction:**
  - `rdb_roles` becomes `__rdb_authid`. A plaintext password is converted to
    SCRAM at migration. An md5 verifier is kept, flagged, and warned about
    at every login until it is reset.
  - Name-keyed `__rdb_acl` rows become id-keyed rows.
  - Every object without an owner gets the database's bootstrap
    superuser, or else its first superuser, as owner.
  - The directory gains the `roles_v2` marker in `incompat_features`, so an
    older binary refuses it instead of reading an empty `rdb_roles` as "no
    roles".
- **`rdb_roles` remains as a read-only compatibility view** `(name,
  can_login, is_superuser)`, without the password, for Studio and scripts.
- **Crash safety.** The migration is covered by the power-loss crash-image
  harness (`core/power_loss`). An interrupted migration leaves either the
  old catalogue or the new one, never a mix.

## 4. Statements (PostgreSQL-compatible)

- **Roles:**
  - `CREATE`/`ALTER`/`DROP ROLE|USER|GROUP` with the full option grammar,
    parsed by tokens (whole words, quoted names, nesting-aware;
    `scanners-must-track-nesting`);
  - `ALTER ROLE … RENAME TO`, `ALTER ROLE … SET`/`RESET`;
  - `ALTER ROLE CURRENT_USER PASSWORD …`, which any role may run on itself;
  - a password given as a SCRAM or md5 verifier is stored as given, not
    hashed again, so `pg_dumpall` restores.
- **Grants:**
  - `GRANT`/`REVOKE` on tables, columns, sequences, functions, procedures,
    types, domains and the database;
  - `ALL TABLES IN SCHEMA public`, read as all tables, since the engine has
    one namespace;
  - `WITH GRANT OPTION`, `GRANTED BY`, `CASCADE`/`RESTRICT` (dependent
    grants are revoked or refused, `2BP01`).
- **Membership:** `GRANT role TO member WITH ADMIN | INHERIT | SET`;
  `SET ROLE`/`RESET ROLE`; `SET SESSION AUTHORIZATION` (superuser only);
  `current_user`, `session_user` and `current_role` with their PostgreSQL
  meaning.
- **Defaults:** `ALTER DEFAULT PRIVILEGES [FOR ROLE r]` for every kind.
- **Cleanup:** `REASSIGN OWNED BY … TO …`, `DROP OWNED BY …`. `DROP ROLE`
  refuses a role that still owns or holds anything (`2BP01`, naming the
  objects) and ends that role's open sessions.
- **Functions:** `SECURITY DEFINER`/`INVOKER`; `SET search_path` on a
  definer function is accepted.
- **Row-level security (R3):** `CREATE/ALTER/DROP POLICY`, `ALTER TABLE …
  ENABLE/DISABLE/FORCE ROW LEVEL SECURITY`, `BYPASSRLS`.
- **Other front ends:**
  - MySQL: `CREATE USER 'u'@'%'`, `GRANT … ON db.*` and `ON db.t` map onto
    the same model, and `SHOW GRANTS` reads the real ACL instead of
    inventing text.
  - T-SQL: `CREATE LOGIN`/`CREATE USER`/`GRANT` map onto it too.

## 5. Semantics the checks must get right

| Case | Rule (PostgreSQL's) |
|---|---|
| Views | Base tables are checked with the **view owner's** rights. `CREATE VIEW` requires the creator to hold SELECT on what it reads. `security_invoker` views use the caller's rights |
| Data-modifying CTEs, `MERGE`, `INSERT … SELECT`, `UPDATE … FROM`, subqueries | Each relation is checked for the action done to it, wherever it appears |
| `SELECT … FOR UPDATE/SHARE` | UPDATE (or DELETE, …) on the locked table, not only SELECT |
| Triggers | `CREATE TRIGGER` needs TRIGGER on the table. A trigger body runs with the function's rights: definer if `SECURITY DEFINER`, else the caller's, as PostgreSQL |
| Foreign keys | Creating one needs REFERENCES on the referenced columns. Checks and cascades run as the referenced table's owner, as PostgreSQL's internal triggers do |
| Sequences | `nextval` needs USAGE or UPDATE; `currval` needs USAGE or SELECT; `setval` needs UPDATE. Serial defaults are checked as the inserting role |
| Functions | EXECUTE, granted to PUBLIC by default as PostgreSQL |
| COPY | `FROM` needs INSERT; `TO` needs SELECT, on the listed columns |
| KV | Data commands need the database's `KV` privilege |
| Internal writes | Catalogue writes, replication apply and the engine's own `internal_run` act with an explicit **system capability**, never through the user SQL path. A user statement can never reach the catalogue tables |
| `trust` | Authenticates as the named role, with **that role's** attributes. It is never an administrator by itself. Native authentication with no credentials refuses to start, unless an explicit flag allows it, and then on loopback only |

## 6. Performance budget

- **Per database:** an `ArcSwap<AclSnapshot>`, rebuilt off the hot path
  when a catalogue change commits, with its generation bumped in the same
  transaction.
- **Per statement:** one load, then one hash lookup per object touched,
  comparing bitsets.
- **Budget:** ≤ 1% of a prepared point read's server instructions (~1k of
  ~95k), **zero allocations**, no catalogue query, no task spawn. Measured
  like every perf gate here (`perf stat`, alternating A/B,
  `how-to-profile-this-repo`).
- **Learned prepared statements and the native fast path** cache the
  authorization result keyed by `(principal generation, acl generation)`.
  They keep their fast path for every role, not only superusers.
- **Effect of changes.** A `REVOKE`, `ALTER ROLE NOSUPERUSER` or `DROP ROLE`
  takes effect at each session's **next statement**. Today the superuser
  flag is a snapshot from login.

## 7. Introspection and tools

- **System catalogues:**
  - `pg_authid` (superuser only; `rolpassword` always NULL through SQL);
  - `pg_roles` with real attributes;
  - `pg_auth_members`;
  - `relacl`, `proacl`, `typacl`, `datacl` as `aclitem[]` text, so
    `\du`, `\dp`, `\ddp` and `\z` work;
  - `information_schema.table_privileges`, `role_table_grants`,
    `column_privileges`, `usage_privileges`, `routine_privileges`;
  - `has_table_privilege`, `has_column_privilege`,
    `has_sequence_privilege`, `has_function_privilege`,
    `has_database_privilege`, `pg_has_role`.
- **Client** (`rasterizeddb_client::admin()`):
  - create, alter, drop and rename role;
  - grant and revoke builders, and membership;
  - listing roles and grants.
- **ORM migrator:** grant steps (`Step::grant(..)`), so a migration can
  carry its permissions.
- **Studio:** the Roles tab gains attributes, memberships, a grants matrix
  per table and default privileges.
- **Docs:** rewrite the stale sections of `postgres-compatibility.md`.

## 8. Cargo features and knobs

| Piece | Feature | Default |
|---|---|---|
| The role catalogue, authentication, `authorize`, ownership, grants, membership, the process-admin split, the catalogue protection | **none: always built** | — |
| Row-level security (R3) | `row_level_security` | in `default`, dormant until a table enables RLS |
| Column privileges | `column_privileges` | in `default`, dormant until a column grant exists |

**Runtime settings (`SET CONFIG`, saved):**
- `auth.password_encryption = scram-sha-256 | md5`;
- `auth.allow_md5_login` (default off for new installs; on, with warnings,
  for migrated md5 verifiers until they are reset);
- `roles.create_database_roles = owner | all` (see RD1).

## 9. Phases

| Phase | Ships | Size | Gate |
|---|---|---|---|
| **R0: security floor** (the sharding plan's T0) | C1–C5 closed: user SQL cannot read or write `rdb_roles` or `__rdb_*`; checks fail closed; `CREATE VIEW` checks the creator; `OWNER TO` needs ownership or superuser, with unowned objects assigned at open; whole-word role options; `DROP ROLE` removes its ACL rows; `trust` is not admin; the IMPORT gate (`pgwire.rs:2436` vs `migrate/sql.rs:57`); MySQL/TDS `USE` re-authenticates; `CREATE DATABASE` copies only the owner (RD1) | ~1–1.5k LOC | An attack suite, one mutation-checked test per hole and per protocol (pgwire, native, MySQL, TDS). **Ships in 1.1.0** (RD5) |
| **R1: model v2 and cache** | Role and object ids; `__rdb_authid`, `__rdb_auth_members`, ACL v2 bitsets; the migration and `roles_v2`; `AclSnapshot`; `authorize` at object resolution on every path; ownership checks on every object kind; real sequence, function and type privileges; the CTE and `FOR UPDATE` rules; the system capability for internal writes; the process-admin attribute | ~5–7k LOC | The §6 budget; the pgdiff privilege corpus green; crash images of the migration |
| **R2: PostgreSQL completeness** | SET ROLE; INHERIT/ADMIN/SET options; CREATEROLE/CREATEDB/CONNECTION LIMIT/VALID UNTIL; ALTER ROLE SET/RENAME; changing your own password; storing verifiers as given; REASSIGN/DROP OWNED; dependency-checked DROP ROLE; default privileges for every kind; REVOKE CASCADE; column privileges; SECURITY DEFINER; §7's introspection; MySQL/T-SQL mappings | ~5–6k LOC | psql `\du \dp \ddp`, `pg_dumpall --roles-only` restore, the ORM drivers' role probes |
| **R3: row-level security** | Policies, `FORCE`, `BYPASSRLS`. Policies are injected as predicates before the scan, so the prefilter and indexes still apply; the learned-statement and query caches are keyed by policy generation | ~4–5k LOC | Policy correctness against PG17; a point read on an RLS table within 5% of the same query with the predicate written by hand |
| **R4: tools** | The client admin API, ORM grant steps, Studio screens, docs | ~2–3k LOC | — |

**Order.**
- **R0 first, and on its own.** It fixes security bugs in deployed builds
  and should not wait for the rest.
- R1 before anything else uses roles: the licensed tenant mode, ledger
  admin, and the gateway fetching verifiers (sharding plan G9) through an
  internal API, never through SQL.
- R2 and R3 can go in parallel after R1.

**Coordination.**
- `pgwire.rs` and `executor.rs` are shared hot files: message their owners
  with each hunk.
- The ledger plan (`rasterizeddb-17`) registers `LEDGER ADMIN` through the
  privilege registry, and its admin operations wait for R0.
- The sharding plan's T1 becomes "use R1's process-admin attribute".

## 10. Decisions (decided by the owner 2026-11-23)

RD1–RD4 accepted as recommended. RD5 changed: R0 ships in 1.1.0, not as a patch.

| # | Decision | Outcome |
|---|---|---|
| RD1 | What `CREATE DATABASE` copies. Today: every role and verifier from the default database | **The creating role and the `OWNER` only** (`roles.create_database_roles = owner`). `all` stays available as a setting for those who rely on today's behaviour |
| RD2 | Whether verifiers are readable through SQL | **Never**, not even by superusers. Internal APIs (the gateway, replication) get them by capability |
| RD3 | md5 | Keep accepting it where configured, warn at each md5 login, and make SCRAM the default for new passwords. Off for new installs |
| RD4 | `KV` privilege default | Granted to PUBLIC, so nothing changes for existing apps. Revocable per database |
| RD5 | Release target | **DECIDED: R0 ships in 1.1.0**, not a 1.0.x patch (owner's call; recommendation was 1.0.1). R1 and R2 also target 1.1.0 |
