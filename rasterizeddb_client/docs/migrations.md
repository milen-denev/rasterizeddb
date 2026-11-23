# Migrations

A [`Migrator`](crate::orm::Migrator) holds ordered steps, each applied once
and recorded in a table, and the models whose tables it keeps matching their
structs.

## Automatically, on connect

[`Options::auto_migrate`](crate::Options::auto_migrate) is the knob for
[`Options::migrations`](crate::Options::migrations):

| [`AutoMigrate`](crate::orm::AutoMigrate) | `connect` does |
|---|---|
| `Apply` (default) | applies what is pending before returning; a failure is `connect`'s error |
| `Verify` | changes nothing, and fails if anything is pending, edited, unknown or drifted |
| `Off` | nothing: call [`Client::migrate`](crate::Client::migrate) yourself |

```rust,no_run
use rasterizeddb_client::prelude::*;
use rasterizeddb_client::orm::{AutoMigrate, Migrator, SyncPolicy};

#[derive(Model)]
struct Account { #[rdb(primary_key, serial)] id: i64, email: String }

fn migrations() -> Migrator {
    Migrator::new()
        .step("0001_settings", &[
            "CREATE TABLE IF NOT EXISTS settings (k TEXT NOT NULL, v TEXT, PRIMARY KEY (k))",
        ])
        .down(&["DROP TABLE IF EXISTS settings"])
        .code_step("0002_defaults", "v1", |c| Box::pin(async move {
            c.exec("INSERT INTO settings (k, v) VALUES ($1, $2) ON CONFLICT (k) DO NOTHING",
                   params!["theme", "dark"]).await?;
            Ok(())
        }))
        .repeatable("r_vacuum", &["SET CONFIG vacuum.enabled = on"])
        .model::<Account>()
        .sync_policy(SyncPolicy::default())
}

# async fn demo() -> rasterizeddb_client::Result<()> {
// A service: migrate on start.
let client = Client::connect("unix:/run/app/rdb.sock", Options {
    migrations: Some(migrations()),
    ..Options::default()
}).await?;

// Production, where a deploy job migrates: refuse to start out of date.
let strict = Client::connect("unix:/run/app/rdb.sock", Options {
    migrations: Some(migrations()),
    auto_migrate: AutoMigrate::Verify,
    ..Options::default()
}).await?;
# let _ = (client, strict); Ok(()) }
```

## Steps

| kind | added with | runs |
|---|---|---|
| SQL | [`step`](crate::orm::Migrator::step) | once, in order |
| Rust | [`code_step`](crate::orm::Migrator::code_step) | once, in order; its version string is its checksum |
| repeatable | [`repeatable`](crate::orm::Migrator::repeatable) | after the others, again whenever its text changes |
| a model's table | [`create_table`](crate::orm::Migrator::create_table) | once, as the struct is when added |
| undo | [`down`](crate::orm::Migrator::down), [`down_code`](crate::orm::Migrator::down_code) | by [`rollback_to`](crate::orm::Migrator::rollback_to), newest first |

A step is recorded (name, checksum, kind, time) only once all its statements
have run. The native protocol has no transactions and DDL is not undone by a
rollback anyway, so a step that fails part-way runs again from its first
statement next time: write statements that are safe to repeat
(`IF NOT EXISTS`), or give each its own step.

Each string in a step is one statement. The server refuses a request
holding several `;`-separated statements, so list them separately. Before
anything runs, `plan`, `apply`, `verify` and `rollback_to` check every
statement of every step and down step, so a step can't fail on its third
statement after committing its first two. A string with a second
statement, or one of `BEGIN`/`COMMIT`/`ROLLBACK`, is refused with the
step's name and the statement's position.

## Scenarios

| situation | default | to allow it |
|---|---|---|
| a fresh database | every step, then every model is created | |
| several instances starting together | one migrates under a lock row; the others wait, then find nothing to do | [`lock_timeout`](crate::orm::Migrator::lock_timeout) |
| a process died holding the lock | the lock is taken over once older than 15 minutes | [`lock_stale_after`](crate::orm::Migrator::lock_stale_after) |
| an existing database adopting migrations | | [`baseline("0003")`](crate::orm::Migrator::baseline) records steps up to it without running them |
| a step edited after it ran | error | [`accept_changed(true)`](crate::orm::Migrator::accept_changed) records the new checksum |
| an older build on a newer database | error (steps it does not know are recorded) | [`ignore_unknown(true)`](crate::orm::Migrator::ignore_unknown) |
| a step merged from a branch, sorting before applied ones | error | [`allow_out_of_order(true)`](crate::orm::Migrator::allow_out_of_order) |
| several applications in one database | | [`table("app2_migrations")`](crate::orm::Migrator::table) |
| going back | | [`rollback_to(client, Some("0002"))`](crate::orm::Migrator::rollback_to) |
| seeing before doing | | [`plan`](crate::orm::Migrator::plan), [`status`](crate::orm::Migrator::status) |

## Keeping tables matching their structs

For each [`model`](crate::orm::Migrator::model), every run compares the
table with the struct and acts under the [`SyncPolicy`](crate::orm::SyncPolicy):

| difference | default | policy field |
|---|---|---|
| no table | created, with keys and indexes | `create_tables` |
| a field with no column | `ADD COLUMN` (with its `DEFAULT`) | `add_columns` |
| `#[rdb(renamed_from = "old")]` on a field or the struct | `RENAME COLUMN` / `RENAME TO`, keeping the data | `renames` |
| a `unique` or `index` field without one | created | `keys` |
| a column the struct does not have | reported, kept | `drop_columns` |
| a type that differs | reported | `retype` (`ALTER COLUMN … TYPE`) |
| nullability that differs | reported | `nullability` (`SET`/`DROP NOT NULL`) |
| a partitioned model over a plain table | reported | `convert_partitioning` (`ALTER TABLE … PARTITION BY HASH`, offline) |
| a partitioned table the model does not partition, or partitioned by another key | reported, always | none: the server converts plain to partitioned only |
| a new field with `references = "t(c)"` | `ADD COLUMN … REFERENCES` | `add_columns` |

Reported differences are warnings in the
[`MigrationReport`](crate::orm::MigrationReport), or an error with
`fail_on_drift`. Presets: [`SyncPolicy::additive`](crate::orm::SyncPolicy::additive)
(create and add only), [`SyncPolicy::exact`](crate::orm::SyncPolicy::exact)
(everything, dropping included: for development databases) and
[`SyncPolicy::report_only`](crate::orm::SyncPolicy::report_only).

**Partitioning.** The table's partition key is read from `SHOW TABLE
STATS`. Converting a plain table is offline: the statement holds the table
exclusively, so readers and writers wait, while it copies every row into
its partition. No preset turns it on, `exact()` included; set
`convert_partitioning: true` for the run that should do it, at a quiet
time. The number of partitions is compared at creation only: splits,
merges and isolated keys change it afterwards.

**Foreign keys** on columns that already exist are not compared: the native
protocol has no catalogue of constraints to read them from.

A `NOT NULL` field added to a table that holds rows needs a
`#[rdb(default = "…")]`; without one the server refuses the column and says
so.
