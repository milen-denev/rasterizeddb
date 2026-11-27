//! Schema migrations: ordered steps applied once each, and models kept in
//! step with their structs. Run by hand, or automatically when a
//! [`Client`] connects ([`Options::migrations`](crate::Options::migrations),
//! with [`AutoMigrate`] the knob).
//!
//! ```no_run
//! # use rasterizeddb_client::prelude::*;
//! # #[derive(Model)] struct Account { #[rdb(primary_key, serial)] id: i64, email: String }
//! # async fn demo() -> rasterizeddb_client::Result<()> {
//! use rasterizeddb_client::orm::{AutoMigrate, Migrator, SyncPolicy};
//!
//! let migrations = Migrator::new()
//!     // Plain SQL, run once, in order; `down` for rollback_to.
//!     .step("0001_accounts", &["CREATE TABLE IF NOT EXISTS accounts (id BIGSERIAL, \
//!                               email TEXT NOT NULL, PRIMARY KEY (id))"])
//!     .down(&["DROP TABLE IF EXISTS accounts"])
//!     // Rust for data migrations; the version string is its checksum.
//!     .code_step("0002_backfill", "v1", |c| Box::pin(async move {
//!         c.execute("UPDATE accounts SET email = lower(email) WHERE id > 0", &[]).await?;
//!         Ok(())
//!     }))
//!     // Re-run whenever its text changes (views, functions, grants).
//!     .repeatable("r_settings", &["SET CONFIG vacuum.enabled = on"])
//!     // Keep these tables matching their structs on every start.
//!     .model::<Account>()
//!     .sync_policy(SyncPolicy::default());
//!
//! let client = Client::connect("unix:/run/app/rdb.sock", Options {
//!     migrations: Some(migrations),
//!     auto_migrate: AutoMigrate::Apply,
//!     ..Options::default()
//! }).await?;
//! # let _ = client; Ok(()) }
//! ```
//!
//! # Scenarios
//!
//! * **A fresh database**: every step runs, then every model is created.
//! * **Several instances starting at once**: a lock row (in
//!   `<table>_lock`) lets one migrate while the others wait for it, then find
//!   nothing left to do. A lock older than
//!   [`lock_stale_after`](Migrator::lock_stale_after) (a process that died
//!   holding it) is taken over.
//! * **An existing database adopting migrations**:
//!   [`baseline`](Migrator::baseline) records the steps up to a name as
//!   applied, without running them, when the record table is empty.
//! * **A step edited after it ran**: an error by default (the database holds
//!   what the old text made); [`accept_changed`](Migrator::accept_changed)
//!   records the new checksum instead.
//! * **An older build against a newer database** (recorded steps this code
//!   does not have): an error by default;
//!   [`ignore_unknown`](Migrator::ignore_unknown) lets it start.
//! * **A step merged from another branch** that sorts before steps already
//!   applied: an error by default;
//!   [`allow_out_of_order`](Migrator::allow_out_of_order) applies it.
//! * **Going back**: [`rollback_to`](Migrator::rollback_to) runs the `down`
//!   statements of the steps after a name, newest first.
//! * **Production, where a deploy job migrates**: [`AutoMigrate::Verify`]
//!   makes a connecting service refuse to start against a database that is
//!   behind or has drifted, and changes nothing.
//! * **A model gained a field**: the column is added (with its `DEFAULT`, if
//!   the table holds rows and the column is `NOT NULL`, or the server refuses
//!   it and says so).
//! * **A field or table renamed**: `#[rdb(renamed_from = "old")]` renames it
//!   in place, keeping its data.
//! * **A field removed, retyped or made (non-)nullable**: reported as drift;
//!   applied only when [`SyncPolicy`] allows that change.
//!
//! # Not transactional
//!
//! The native protocol has no transaction blocks, and DDL is not undone by a
//! rollback anyway. A step is recorded only after all its statements have
//! run, so a failed step runs again from its first statement next time:
//! write statements that are safe to repeat (`IF NOT EXISTS`), or give each
//! its own step.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use super::{ColumnDef, Model, Storage, TableSchema, ident};
use crate::client::Client;
use crate::types::{Timestamp, Uuid};
use crate::{Error, Result, params};

/// A boxed future, for a [`Migrator::code_step`].
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

type StepFn = Arc<dyn for<'c> Fn(&'c Client) -> BoxFuture<'c, Result<()>> + Send + Sync>;

/// What [`Client::connect`] does with [`Options::migrations`](crate::Options::migrations).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AutoMigrate {
    /// Nothing: run [`Client::migrate`] yourself.
    Off,
    /// Apply what is pending (steps, then model sync) before `connect`
    /// returns. A failure is `connect`'s error.
    #[default]
    Apply,
    /// Change nothing; fail `connect` if anything is pending, edited,
    /// unknown or drifted. For a service whose database a deploy job
    /// migrates.
    Verify,
}

/// Which differences between a model and its table the sync fixes. What it
/// may not fix is reported as drift: a warning, or an error under
/// [`fail_on_drift`](Self::fail_on_drift).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncPolicy {
    /// Create a model's table when there is none. On by default.
    pub create_tables: bool,
    /// Add a column a model has and its table does not. On by default.
    pub add_columns: bool,
    /// Follow `#[rdb(renamed_from = "…")]` on tables and columns. On by default.
    pub renames: bool,
    /// Create the unique keys and indexes a model declares. On by default.
    pub keys: bool,
    /// Change a column's nullability to the model's (`SET`/`DROP NOT NULL`).
    /// Off by default: making a column `NOT NULL` fails on a table with
    /// NULLs in it.
    pub nullability: bool,
    /// Change a column's type to the model's (`ALTER COLUMN … TYPE`). Off by
    /// default: a conversion can fail or lose precision.
    pub retype: bool,
    /// Drop a column the table has and the model does not. Off by default:
    /// it deletes data.
    pub drop_columns: bool,
    /// Make drift an error rather than a warning in the report.
    pub fail_on_drift: bool,
    /// Convert a plain table whose model is partitioned (`ALTER TABLE …
    /// PARTITION BY HASH`). Off by default, and in every preset: the
    /// conversion is offline, holding the table exclusively (readers and
    /// writers wait) while every row is copied into partitions. Turn it on
    /// for the run that should do it.
    pub convert_partitioning: bool,
}

impl Default for SyncPolicy {
    fn default() -> Self {
        SyncPolicy {
            create_tables: true,
            add_columns: true,
            renames: true,
            keys: true,
            nullability: false,
            retype: false,
            drop_columns: false,
            fail_on_drift: false,
            convert_partitioning: false,
        }
    }
}

impl SyncPolicy {
    /// Create and add only; never rename, alter or drop.
    pub fn additive() -> Self {
        SyncPolicy {
            renames: false,
            ..SyncPolicy::default()
        }
    }

    /// Everything, dropping columns included: the table becomes exactly the
    /// model. For development databases. (Not a partitioning conversion:
    /// that stays [`convert_partitioning`](Self::convert_partitioning).)
    pub fn exact() -> Self {
        SyncPolicy {
            nullability: true,
            retype: true,
            drop_columns: true,
            ..SyncPolicy::default()
        }
    }

    /// No schema changes at all; drift is still reported.
    pub fn report_only() -> Self {
        SyncPolicy {
            create_tables: false,
            add_columns: false,
            renames: false,
            keys: false,
            nullability: false,
            retype: false,
            drop_columns: false,
            fail_on_drift: false,
            convert_partitioning: false,
        }
    }
}

#[derive(Clone)]
enum Body {
    Sql(Vec<String>),
    Code { version: String, run: StepFn },
}

#[derive(Clone)]
struct Step {
    name: String,
    body: Body,
    down: Option<Body>,
    repeatable: bool,
}

impl fmt::Debug for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let body = |b: &Body| match b {
            Body::Sql(statements) => format!("{} statement(s)", statements.len()),
            Body::Code { version, .. } => format!("code {version}"),
        };
        f.debug_struct("Step")
            .field("name", &self.name)
            .field("body", &body(&self.body))
            .field("down", &self.down.as_ref().map(body))
            .field("repeatable", &self.repeatable)
            .finish()
    }
}

/// FNV-1a over a step's body, each statement ended by a NUL so that moving
/// text between two statements changes it.
fn checksum(body: &Body) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut feed = |bytes: &[u8]| {
        for &b in bytes.iter().chain(std::iter::once(&0)) {
            hash ^= u64::from(b);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
    };
    match body {
        Body::Sql(statements) => statements.iter().for_each(|s| feed(s.as_bytes())),
        Body::Code { version, .. } => {
            feed(b"code");
            feed(version.as_bytes());
        }
    }
    hash as i64
}

/// Where a step stands against a database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepState {
    /// Recorded, with the checksum it has now.
    Applied,
    /// Not recorded yet.
    Pending,
    /// Applied with other statements than it has now.
    Changed,
    /// Recorded in the database, absent from this code.
    Unknown,
}

/// One step's standing, from [`Migrator::status`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepStatus {
    /// The step's name.
    pub name: String,
    /// Where it stands.
    pub state: StepState,
    /// When it was recorded, if it was.
    pub applied_at: Option<Timestamp>,
}

/// What [`Migrator::plan`] found: what applying would do, and what is wrong.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MigrationPlan {
    /// Steps that would run, in order.
    pub pending: Vec<String>,
    /// Repeatable steps whose text changed, which would run again.
    pub repeatable: Vec<String>,
    /// Steps edited since they ran.
    pub changed: Vec<String>,
    /// Steps recorded in the database that this code does not have.
    pub unknown: Vec<String>,
    /// Pending steps that sort before an applied one.
    pub out_of_order: Vec<String>,
    /// Steps a baseline would record without running.
    pub baseline: Vec<String>,
    /// The schema statements the model sync would run.
    pub schema_changes: Vec<String>,
    /// Index statements the sync re-issues on every run (`CREATE INDEX IF
    /// NOT EXISTS`): the native protocol cannot list a table's indexes, so
    /// whether one exists is not known here, and these do not count against
    /// [`is_up_to_date`](Self::is_up_to_date).
    pub ensured: Vec<String>,
    /// Differences the sync policy does not allow it to fix.
    pub drift: Vec<String>,
}

impl MigrationPlan {
    /// Whether the database is exactly what this code expects: nothing to
    /// run, nothing edited, unknown or drifted.
    pub fn is_up_to_date(&self) -> bool {
        self.pending.is_empty()
            && self.repeatable.is_empty()
            && self.changed.is_empty()
            && self.unknown.is_empty()
            && self.out_of_order.is_empty()
            && self.baseline.is_empty()
            && self.schema_changes.is_empty()
            && self.drift.is_empty()
    }

    fn summary(&self) -> String {
        let mut parts = Vec::new();
        let mut list = |label: &str, items: &[String]| {
            if !items.is_empty() {
                parts.push(format!("{label}: {}", items.join(", ")));
            }
        };
        list("pending", &self.pending);
        list("repeatable to re-run", &self.repeatable);
        list("edited since applied", &self.changed);
        list("unknown to this code", &self.unknown);
        list("out of order", &self.out_of_order);
        list("to baseline", &self.baseline);
        list("schema changes", &self.schema_changes);
        list("drift", &self.drift);
        parts.join("; ")
    }
}

/// What [`Migrator::apply`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MigrationReport {
    /// Steps run by this call.
    pub applied: Vec<String>,
    /// Repeatable steps run again because their text changed.
    pub reapplied: Vec<String>,
    /// Steps recorded without running them, by a [`baseline`](Migrator::baseline).
    pub baselined: Vec<String>,
    /// Edited steps whose new checksum was recorded
    /// ([`accept_changed`](Migrator::accept_changed)).
    pub accepted: Vec<String>,
    /// Schema statements the model sync ran.
    pub schema_changes: Vec<String>,
    /// Index statements re-issued (no-ops when the index exists).
    pub ensured: Vec<String>,
    /// Drift left alone, and unknown steps ignored.
    pub warnings: Vec<String>,
}

/// Ordered migration steps, and the models to keep in sync.
#[derive(Clone)]
pub struct Migrator {
    steps: Vec<Step>,
    models: Vec<(TableSchema, Storage)>,
    table: String,
    baseline: Option<String>,
    accept_changed: bool,
    ignore_unknown: bool,
    allow_out_of_order: bool,
    policy: SyncPolicy,
    compression: Option<bool>,
    lock_timeout: Duration,
    lock_stale_after: Duration,
}

impl fmt::Debug for Migrator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Migrator")
            .field("steps", &self.steps)
            .field(
                "models",
                &self.models.iter().map(|m| m.0.table).collect::<Vec<_>>(),
            )
            .field("table", &self.table)
            .field("baseline", &self.baseline)
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl Default for Migrator {
    fn default() -> Self {
        Migrator::new()
    }
}

/// The record table's default name.
pub const MIGRATIONS_TABLE: &str = "rdb_client_migrations";

impl Migrator {
    /// A migrator with no steps and no models, recording in `rdb_client_migrations`.
    pub fn new() -> Self {
        Migrator {
            steps: Vec::new(),
            models: Vec::new(),
            table: MIGRATIONS_TABLE.to_string(),
            baseline: None,
            accept_changed: false,
            ignore_unknown: false,
            allow_out_of_order: false,
            policy: SyncPolicy::default(),
            compression: None,
            lock_timeout: Duration::from_secs(60),
            lock_stale_after: Duration::from_secs(15 * 60),
        }
    }

    fn push(mut self, name: &str, body: Body, repeatable: bool) -> Self {
        self.steps.push(Step {
            name: name.to_string(),
            body,
            down: None,
            repeatable,
        });
        self
    }

    /// A step of SQL statements, run once, in order with the others.
    pub fn step(self, name: &str, statements: &[&str]) -> Self {
        self.push(
            name,
            Body::Sql(statements.iter().map(|s| s.to_string()).collect()),
            false,
        )
    }

    /// A step of Rust, run once. `version` stands for its body in the
    /// checksum: change it when the code's effect changes.
    pub fn code_step<F>(self, name: &str, version: &str, run: F) -> Self
    where
        F: for<'c> Fn(&'c Client) -> BoxFuture<'c, Result<()>> + Send + Sync + 'static,
    {
        self.push(
            name,
            Body::Code {
                version: version.to_string(),
                run: Arc::new(run),
            },
            false,
        )
    }

    /// A step run again whenever its statements change (and first when
    /// added), after the ordinary steps. For definitions that are replaced
    /// whole: settings, views, grants.
    pub fn repeatable(self, name: &str, statements: &[&str]) -> Self {
        self.push(
            name,
            Body::Sql(statements.iter().map(|s| s.to_string()).collect()),
            true,
        )
    }

    /// A repeatable step of grants, run again whenever they change, so a
    /// migration carries its permissions:
    /// `.grants("app grants", &[Grant::privileges(&["SELECT"]).on_table("orders").to("app")])`.
    /// Its undo revokes them.
    pub fn grants(self, name: &str, grants: &[crate::admin::Grant]) -> Result<Self> {
        let up = grants.iter().map(|g| g.to_sql(true)).collect::<Result<Vec<_>>>()?;
        let down = grants.iter().map(|g| g.to_sql(false)).collect::<Result<Vec<_>>>()?;
        let mut migrator = self.push(name, Body::Sql(up), true);
        if let Some(step) = migrator.steps.last_mut() {
            step.down = Some(Body::Sql(down));
        }
        Ok(migrator)
    }

    /// The statements that undo the step added last, for
    /// [`rollback_to`](Self::rollback_to).
    pub fn down(mut self, statements: &[&str]) -> Self {
        if let Some(step) = self.steps.last_mut() {
            step.down = Some(Body::Sql(
                statements.iter().map(|s| s.to_string()).collect(),
            ));
        }
        self
    }

    /// Rust that undoes the step added last.
    pub fn down_code<F>(mut self, run: F) -> Self
    where
        F: for<'c> Fn(&'c Client) -> BoxFuture<'c, Result<()>> + Send + Sync + 'static,
    {
        if let Some(step) = self.steps.last_mut() {
            step.down = Some(Body::Code {
                version: String::new(),
                run: Arc::new(run),
            });
        }
        self
    }

    /// A step that creates model `M`'s table, its keys and indexes once, as
    /// they are when the step is added. (For a table kept in sync on every
    /// start, use [`model`](Self::model).)
    pub fn create_table<M: Model>(self, name: &str) -> Result<Self> {
        let statements = TableSchema::of::<M>().create_sql_with(&Storage::of::<M>())?;
        Ok(self.push(name, Body::Sql(statements), false))
    }

    /// Keeps model `M`'s table matching the struct on every run, after the
    /// steps, under the [`SyncPolicy`].
    pub fn model<M: Model>(mut self) -> Self {
        self.models.push((TableSchema::of::<M>(), Storage::of::<M>()));
        self
    }

    /// Whether the model sync changes how a table stores its values to the
    /// model's: a column's `#[rdb(compression)]` (`ALTER COLUMN … SET
    /// COMPRESSION`) and the struct's `row_compression` (`ALTER TABLE … SET
    /// (row_compression = …)`). Either rewrites nothing: it applies to
    /// values written from then on. Unset, it follows the [`SyncPolicy`]:
    /// on when the policy alters existing tables at all (`renames`,
    /// `nullability`, `retype` or `drop_columns`), so under the default and
    /// [`SyncPolicy::exact`], and off under [`SyncPolicy::additive`] and
    /// [`SyncPolicy::report_only`], where a difference is reported as drift.
    pub fn sync_compression(mut self, on: bool) -> Self {
        self.compression = Some(on);
        self
    }

    pub(crate) fn compression_sync(&self) -> bool {
        self.compression.unwrap_or(
            self.policy.renames || self.policy.nullability || self.policy.retype || self.policy.drop_columns,
        )
    }

    /// What the model sync may change (the default: create, add, rename, keys).
    pub fn sync_policy(mut self, policy: SyncPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// The table steps are recorded in (default `rdb_client_migrations`), for
    /// several applications sharing one database.
    pub fn table(mut self, name: &str) -> Self {
        self.table = name.to_string();
        self
    }

    /// For a database that predates its migrations: when nothing is recorded
    /// yet, record the steps up to and including `name` as applied without
    /// running them.
    pub fn baseline(mut self, name: &str) -> Self {
        self.baseline = Some(name.to_string());
        self
    }

    /// Record an edited step's new checksum instead of failing.
    pub fn accept_changed(mut self, accept: bool) -> Self {
        self.accept_changed = accept;
        self
    }

    /// Start even when the database records steps this code does not have.
    pub fn ignore_unknown(mut self, ignore: bool) -> Self {
        self.ignore_unknown = ignore;
        self
    }

    /// Apply a pending step that sorts before an applied one, instead of
    /// failing.
    pub fn allow_out_of_order(mut self, allow: bool) -> Self {
        self.allow_out_of_order = allow;
        self
    }

    /// How long to wait for another process's migration lock (default 60 s).
    pub fn lock_timeout(mut self, timeout: Duration) -> Self {
        self.lock_timeout = timeout;
        self
    }

    /// When a lock is old enough to be a dead process's (default 15 min).
    pub fn lock_stale_after(mut self, age: Duration) -> Self {
        self.lock_stale_after = age;
        self
    }

    fn lock_table(&self) -> String {
        format!("{}_lock", self.table)
    }

    fn check_names(&self) -> Result<()> {
        let mut seen = HashSet::new();
        for step in &self.steps {
            if step.name.is_empty() || !seen.insert(step.name.as_str()) {
                return Err(Error::Param(format!(
                    "migration step {:?} is empty or listed twice",
                    step.name
                )));
            }
        }
        if let Some(baseline) = &self.baseline
            && !seen.contains(baseline.as_str())
        {
            return Err(Error::Param(format!("baseline {baseline:?} names no step")));
        }
        // Every statement of every step is checked before any runs: a step
        // refused at its third statement would leave its first two
        // committed and the step unrecorded.
        for step in &self.steps {
            for (body, which) in [
                (Some(&step.body), "step"),
                (step.down.as_ref(), "down step"),
            ] {
                if let Some(Body::Sql(statements)) = body {
                    for (i, statement) in statements.iter().enumerate() {
                        crate::params::check_statement(statement).map_err(|e| match e {
                            Error::Unsupported(m) => Error::Unsupported(format!(
                                "migration {which} {:?}, statement {}: {m} (list each \
                                 statement separately in the step)",
                                step.name,
                                i + 1
                            )),
                            other => other,
                        })?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn ensure_tables(&self, client: &Client) -> Result<()> {
        client
            .run(&format!(
                "CREATE TABLE IF NOT EXISTS {} (name TEXT NOT NULL, checksum BIGINT NOT NULL, \
                 kind TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL, PRIMARY KEY (name))",
                ident(&self.table)?
            ))
            .await?;
        client
            .run(&format!(
                "CREATE TABLE IF NOT EXISTS {} (name TEXT NOT NULL, holder TEXT NOT NULL, \
                 acquired_at TIMESTAMPTZ NOT NULL, PRIMARY KEY (name))",
                ident(&self.lock_table())?
            ))
            .await
            .map(drop)
    }

    /// `name → (checksum, applied_at)`, or nothing when the record table is
    /// absent (a plan must not create it).
    async fn recorded(&self, client: &Client) -> Result<HashMap<String, (i64, Timestamp)>> {
        if !client.table_exists(&self.table).await? {
            return Ok(HashMap::new());
        }
        let rows: Vec<(String, i64, Timestamp)> = client
            .fetch_all(
                &format!(
                    "SELECT name, checksum, applied_at FROM {}",
                    ident(&self.table)?
                ),
                params![],
            )
            .await?;
        Ok(rows.into_iter().map(|(n, c, t)| (n, (c, t))).collect())
    }

    /// Where every step stands, and the recorded steps this code lacks.
    pub async fn status(&self, client: &Client) -> Result<Vec<StepStatus>> {
        self.check_names()?;
        let recorded = self.recorded(client).await?;
        let known: HashSet<&str> = self.steps.iter().map(|s| s.name.as_str()).collect();
        let mut out: Vec<StepStatus> = self
            .steps
            .iter()
            .map(|step| {
                let record = recorded.get(&step.name);
                let state = match record {
                    None => StepState::Pending,
                    Some((sum, _)) if *sum != checksum(&step.body) => StepState::Changed,
                    Some(_) => StepState::Applied,
                };
                StepStatus {
                    name: step.name.clone(),
                    state,
                    applied_at: record.map(|r| r.1),
                }
            })
            .collect();
        let mut unknown: Vec<(&String, &(i64, Timestamp))> = recorded
            .iter()
            .filter(|(n, _)| !known.contains(n.as_str()))
            .collect();
        unknown.sort_by_key(|(n, _)| n.as_str());
        out.extend(unknown.into_iter().map(|(name, (_, at))| StepStatus {
            name: name.clone(),
            state: StepState::Unknown,
            applied_at: Some(*at),
        }));
        Ok(out)
    }

    /// What [`apply`](Self::apply) would do, changing nothing.
    pub async fn plan(&self, client: &Client) -> Result<MigrationPlan> {
        self.check_names()?;
        let recorded = self.recorded(client).await?;
        let mut plan = MigrationPlan::default();
        self.plan_steps(&recorded, &mut plan);
        for (model, storage) in &self.models {
            let sync = plan_model(client, model, storage, &self.policy, self.compression_sync()).await?;
            plan.schema_changes.extend(sync.changes);
            plan.ensured.extend(sync.ensure);
            plan.drift.extend(sync.drift);
        }
        Ok(plan)
    }

    fn plan_steps(&self, recorded: &HashMap<String, (i64, Timestamp)>, plan: &mut MigrationPlan) {
        if recorded.is_empty()
            && let Some(baseline) = &self.baseline
        {
            for step in &self.steps {
                plan.baseline.push(step.name.clone());
                if &step.name == baseline {
                    break;
                }
            }
        }
        let known: HashSet<&str> = self.steps.iter().map(|s| s.name.as_str()).collect();
        let mut unknown: Vec<String> = recorded
            .keys()
            .filter(|n| !known.contains(n.as_str()))
            .cloned()
            .collect();
        unknown.sort();
        plan.unknown = unknown;
        // The last ordinary step already applied: a pending one before it
        // is out of order.
        let last_applied = self
            .steps
            .iter()
            .rposition(|s| !s.repeatable && recorded.contains_key(&s.name));
        for (i, step) in self.steps.iter().enumerate() {
            if plan.baseline.contains(&step.name) {
                continue;
            }
            match recorded.get(&step.name) {
                None if step.repeatable => plan.repeatable.push(step.name.clone()),
                None => {
                    if last_applied.is_some_and(|last| i < last) {
                        plan.out_of_order.push(step.name.clone());
                    }
                    plan.pending.push(step.name.clone());
                }
                Some((sum, _)) if *sum != checksum(&step.body) => {
                    if step.repeatable {
                        plan.repeatable.push(step.name.clone());
                    } else {
                        plan.changed.push(step.name.clone());
                    }
                }
                Some(_) => {}
            }
        }
    }

    /// [`apply`](Self::apply) over a router's every shard: the home shard
    /// first, then the others in id order, each recording its own ledger in
    /// its own migration table. A failure stops there and names the shard;
    /// shards before it are migrated, and the next call resumes from each
    /// shard's own ledger (DDL is not transactional, so nothing is undone).
    /// Every model's table is made on every shard, so schemas stay
    /// identical; a global model's table stays empty off the home shard.
    #[cfg(feature = "sharding")]
    pub async fn apply_sharded(
        &self,
        router: &crate::shard::Router,
    ) -> Result<Vec<(crate::shard::ShardId, MigrationReport)>> {
        let mut out = Vec::new();
        for id in router.migration_order() {
            let client = router.for_shard(id)?;
            let report = self.apply(client).await.map_err(|e| match e {
                Error::Server(m) => Error::Server(format!(
                    "migration stopped at shard {id} (the shards before it are migrated; run again to resume): {m}"
                )),
                Error::Unsupported(m) => Error::Unsupported(format!("migration stopped at shard {id}: {m}")),
                other => other,
            })?;
            out.push((id, report));
        }
        Ok(out)
    }

    /// Applies what is pending: steps in order, repeatable steps whose text
    /// changed, then the model sync. Holds the migration lock throughout.
    pub async fn apply(&self, client: &Client) -> Result<MigrationReport> {
        self.check_names()?;
        self.ensure_tables(client).await?;
        let holder = self.lock(client).await?;
        let result = self.apply_locked(client).await;
        let released = self.unlock(client, &holder).await;
        let report = result?;
        released?;
        Ok(report)
    }

    async fn apply_locked(&self, client: &Client) -> Result<MigrationReport> {
        let recorded = self.recorded(client).await?;
        let mut plan = MigrationPlan::default();
        self.plan_steps(&recorded, &mut plan);
        let mut report = MigrationReport::default();

        if !plan.unknown.is_empty() {
            let message = format!(
                "the database records steps this code does not have: {} (a newer build migrated \
                 it, or steps were removed)",
                plan.unknown.join(", ")
            );
            if !self.ignore_unknown {
                return Err(Error::Unsupported(format!(
                    "{message}; ignore_unknown(true) to start anyway"
                )));
            }
            report.warnings.push(message);
        }
        if !plan.changed.is_empty() && !self.accept_changed {
            return Err(Error::Unsupported(format!(
                "migration step(s) {} were edited after they were applied; add a new step instead, \
                 or accept_changed(true) to record the new text",
                plan.changed.join(", ")
            )));
        }
        if !plan.out_of_order.is_empty() && !self.allow_out_of_order {
            return Err(Error::Unsupported(format!(
                "step(s) {} are pending but sort before steps already applied; \
                 allow_out_of_order(true) to apply them",
                plan.out_of_order.join(", ")
            )));
        }

        for name in &plan.baseline {
            let step = self.find(name);
            self.record(client, step, "baseline", false).await?;
            report.baselined.push(name.clone());
        }
        for name in &plan.changed {
            let step = self.find(name);
            self.record(client, step, "accepted", true).await?;
            report.accepted.push(name.clone());
        }
        for name in &plan.pending {
            let step = self.find(name);
            self.run_body(client, &step.name, &step.body).await?;
            self.record(client, step, "applied", false).await?;
            report.applied.push(name.clone());
        }
        for name in &plan.repeatable {
            let step = self.find(name);
            self.run_body(client, &step.name, &step.body).await?;
            let again = recorded.contains_key(name);
            self.record(client, step, "repeatable", again).await?;
            report.reapplied.push(name.clone());
        }
        for (model, storage) in &self.models {
            let sync = plan_model(client, model, storage, &self.policy, self.compression_sync()).await?;
            if !sync.drift.is_empty() && self.policy.fail_on_drift {
                return Err(Error::Unsupported(format!(
                    "{} has drifted from its model: {}",
                    model.table,
                    sync.drift.join("; ")
                )));
            }
            let run = |statement: String| async move {
                client.run(&statement).await.map_err(|e| match e {
                    Error::Server(m) => {
                        Error::Server(format!("syncing {}: {statement}: {m}", model.table))
                    }
                    other => other,
                })?;
                Ok::<String, Error>(statement)
            };
            for statement in sync.changes {
                report.schema_changes.push(run(statement).await?);
            }
            for statement in sync.ensure {
                report.ensured.push(run(statement).await?);
            }
            report.warnings.extend(sync.drift);
        }
        Ok(report)
    }

    /// Fails like [`AutoMigrate::Verify`] does when anything is not up to date.
    pub async fn verify(&self, client: &Client) -> Result<MigrationPlan> {
        let plan = self.plan(client).await?;
        if plan.is_up_to_date() {
            Ok(plan)
        } else {
            Err(Error::Unsupported(format!(
                "the database is not migrated to what this code expects: {}",
                plan.summary()
            )))
        }
    }

    /// Undoes the steps applied after `target` (all of them for `None`),
    /// newest first, with their `down` statements, and removes their records.
    /// A step to undo without a `down` stops it, before anything is undone,
    /// as do recorded steps this code does not have (unless
    /// [`ignore_unknown`](Self::ignore_unknown)): they may depend on what
    /// would be undone.
    /// The names undone.
    pub async fn rollback_to(&self, client: &Client, target: Option<&str>) -> Result<Vec<String>> {
        self.check_names()?;
        let keep = match target {
            None => 0,
            Some(name) => {
                self.steps
                    .iter()
                    .position(|s| s.name == name)
                    .ok_or_else(|| {
                        Error::Param(format!("rollback target {name:?} names no step"))
                    })?
                    + 1
            }
        };
        self.ensure_tables(client).await?;
        let holder = self.lock(client).await?;
        let result = async {
            let recorded = self.recorded(client).await?;
            let known: HashSet<&str> = self.steps.iter().map(|s| s.name.as_str()).collect();
            let mut unknown: Vec<&String> = recorded
                .keys()
                .filter(|n| !known.contains(n.as_str()))
                .collect();
            if !unknown.is_empty() && !self.ignore_unknown {
                unknown.sort();
                return Err(Error::Unsupported(format!(
                    "the database records steps this code does not have ({}), which a rollback \
                     could not undo; nothing was undone. ignore_unknown(true) to undo only the \
                     steps listed here",
                    unknown
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
            let undo: Vec<&Step> = self.steps[keep..]
                .iter()
                .filter(|s| !s.repeatable && recorded.contains_key(&s.name))
                .rev()
                .collect();
            if let Some(step) = undo.iter().find(|s| s.down.is_none()) {
                return Err(Error::Unsupported(format!(
                    "step {:?} has no down() to undo it with; nothing was undone",
                    step.name
                )));
            }
            let mut undone = Vec::new();
            for step in undo {
                self.run_body(client, &step.name, step.down.as_ref().expect("checked"))
                    .await?;
                client
                    .exec(
                        &format!("DELETE FROM {} WHERE name = $1", ident(&self.table)?),
                        params![step.name],
                    )
                    .await?;
                undone.push(step.name.clone());
            }
            Ok(undone)
        }
        .await;
        let released = self.unlock(client, &holder).await;
        let undone = result?;
        released?;
        Ok(undone)
    }

    fn find(&self, name: &str) -> &Step {
        self.steps
            .iter()
            .find(|s| s.name == name)
            .expect("planned from these steps")
    }

    async fn run_body(&self, client: &Client, name: &str, body: &Body) -> Result<()> {
        let tag = |e: Error| match e {
            Error::Server(m) => Error::Server(format!("migration {name:?}: {m}")),
            other => other,
        };
        match body {
            Body::Sql(statements) => {
                for statement in statements {
                    client.run(statement).await.map_err(tag)?;
                }
                Ok(())
            }
            Body::Code { run, .. } => run(client).await.map_err(tag),
        }
    }

    async fn record(&self, client: &Client, step: &Step, kind: &str, replace: bool) -> Result<()> {
        let table = ident(&self.table)?;
        let sum = checksum(&step.body);
        if replace {
            client
                .exec(
                    &format!("UPDATE {table} SET checksum = $1, kind = $2, applied_at = $3 WHERE name = $4"),
                    params![sum, kind, Timestamp::now(), step.name],
                )
                .await?;
        } else {
            client
                .exec(
                    &format!(
                        "INSERT INTO {table} (name, checksum, kind, applied_at) VALUES ($1, $2, $3, $4)"
                    ),
                    params![step.name, sum, kind, Timestamp::now()],
                )
                .await?;
        }
        Ok(())
    }

    /// Takes the lock row, waiting for another holder, and taking over one
    /// older than `lock_stale_after`. The holder id written.
    async fn lock(&self, client: &Client) -> Result<String> {
        let table = ident(&self.lock_table())?;
        let holder = format!("pid {} {}", std::process::id(), Uuid::new_v4()?);
        let started = std::time::Instant::now();
        loop {
            let taken = client
                .exec(
                    &format!(
                        "INSERT INTO {table} (name, holder, acquired_at) VALUES ('migrate', $1, $2) \
                         ON CONFLICT (name) DO NOTHING"
                    ),
                    params![holder, Timestamp::now()],
                )
                .await?;
            if taken == 1 {
                return Ok(holder);
            }
            let current: Option<(String, Timestamp)> = client
                .fetch_optional(
                    &format!("SELECT holder, acquired_at FROM {table} WHERE name = 'migrate'"),
                    params![],
                )
                .await?;
            if let Some((other, since)) = current {
                let age = Timestamp::now().micros.saturating_sub(since.micros);
                if age > self.lock_stale_after.as_micros() as i64 {
                    // The holder is presumed dead. Deleting by its id means
                    // two waiters cannot both take over one stale lock.
                    client
                        .exec(
                            &format!("DELETE FROM {table} WHERE name = 'migrate' AND holder = $1"),
                            params![other],
                        )
                        .await?;
                    continue;
                }
                if started.elapsed() > self.lock_timeout {
                    return Err(Error::Unsupported(format!(
                        "another process ({other}) has held the migration lock since {since}; \
                         waited {:?}",
                        self.lock_timeout
                    )));
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn unlock(&self, client: &Client, holder: &str) -> Result<()> {
        client
            .exec(
                &format!(
                    "DELETE FROM {} WHERE name = 'migrate' AND holder = $1",
                    ident(&self.lock_table())?
                ),
                params![holder],
            )
            .await
            .map(drop)
    }
}

// ---------------------------------------------------------------- model sync

/// `information_schema.columns.data_type` for a DDL type, when the two can be
/// compared; `None` for a type whose catalogue name this client does not
/// know, which is then never reported as drift.
fn catalogue_type(sql_type: &str) -> Option<&'static str> {
    let upper = sql_type.trim().to_ascii_uppercase();
    if upper.ends_with("[]") {
        return Some("ARRAY");
    }
    let base = upper.split('(').next().unwrap_or("").trim();
    Some(match base {
        "BIGINT" | "INT8" | "BIGSERIAL" | "SERIAL8" => "bigint",
        "INTEGER" | "INT" | "INT4" | "SERIAL" | "SERIAL4" => "integer",
        "SMALLINT" | "INT2" | "SMALLSERIAL" | "SERIAL2" => "smallint",
        "BOOLEAN" | "BOOL" => "boolean",
        "REAL" | "FLOAT4" => "real",
        "DOUBLE PRECISION" | "FLOAT8" | "FLOAT" | "DOUBLE" => "double precision",
        "NUMERIC" | "DECIMAL" | "UBIGINT" | "HUGEINT" | "UHUGEINT" => "numeric",
        "TEXT" => "text",
        "VARCHAR" | "CHARACTER VARYING" => "character varying",
        "CHAR" | "CHARACTER" | "BPCHAR" => "character",
        "BYTEA" => "bytea",
        "UUID" => "uuid",
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => "timestamp with time zone",
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => "timestamp without time zone",
        "DATE" => "date",
        "TIME" | "TIME WITHOUT TIME ZONE" => "time without time zone",
        "INTERVAL" => "interval",
        "JSONB" => "jsonb",
        "JSON" => "json",
        _ => return None,
    })
}

struct Existing {
    data_type: String,
    nullable: bool,
    /// `pg_attribute.attcompression`: `zstd`, `zstd(9)`, or empty for none.
    /// `None` when the server could not say (one that predates it reports
    /// empty, which is right: it compresses nothing).
    compression: Option<String>,
}

async fn existing_columns(client: &Client, table: &str) -> Result<HashMap<String, Existing>> {
    let rows: Vec<(String, String, String)> = client
        .fetch_all(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
             WHERE table_name = $1",
            params![table],
        )
        .await?;
    // How each column stores its values. A failure here leaves the
    // compression unknown, and the comparison says so rather than guess.
    let compression: Option<HashMap<String, String>> = client
        .fetch_all::<(String, String)>(
            "SELECT a.attname, a.attcompression FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = $1",
            params![table],
        )
        .await
        .ok()
        .map(|rows| rows.into_iter().collect());
    Ok(rows
        .into_iter()
        .map(|(name, data_type, nullable)| {
            let compressed = compression.as_ref().map(|c| c.get(&name).cloned().unwrap_or_default());
            (
                name,
                Existing {
                    data_type,
                    nullable: nullable == "YES",
                    compression: compressed,
                },
            )
        })
        .collect())
}

/// What the sync would do to one model's table.
struct ModelSync {
    changes: Vec<String>,
    ensure: Vec<String>,
    drift: Vec<String>,
}

/// The statements that bring `model`'s table to the model under `policy`,
/// and the differences it may not fix.
async fn plan_model(
    client: &Client,
    model: &TableSchema,
    storage: &Storage,
    policy: &SyncPolicy,
    compression: bool,
) -> Result<ModelSync> {
    let mut changes = Vec::new();
    let mut ensure = Vec::new();
    let mut drift = Vec::new();
    let table = ident(model.table)?;

    let mut source = None;
    if client.table_exists(model.table).await? {
        source = Some(model.table);
    } else {
        for old in model.renamed_from {
            if client.table_exists(old).await? {
                source = Some(old);
                break;
            }
        }
    }
    let Some(found) = source else {
        if policy.create_tables {
            changes.extend(model.create_sql_with(storage)?);
        } else {
            drift.push(format!("table {} does not exist", model.table));
        }
        return Ok(ModelSync {
            changes,
            ensure,
            drift,
        });
    };
    if found != model.table {
        if policy.renames {
            changes.push(format!("ALTER TABLE {} RENAME TO {table}", ident(found)?));
        } else {
            drift.push(format!("table {found} is to be renamed to {}", model.table));
            // The rest is compared against the table as it is now named.
        }
    }

    let existing = existing_columns(client, found).await?;
    let mut accounted: HashSet<&str> = HashSet::new();
    for column in model.columns {
        let name = column.name;
        let current = match existing.get(name) {
            Some(current) => {
                accounted.insert(name);
                current
            }
            None => {
                let old = column
                    .renamed_from
                    .iter()
                    .find(|old| existing.contains_key(**old));
                match old {
                    Some(old) if policy.renames => {
                        accounted.insert(old);
                        changes.push(format!(
                            "ALTER TABLE {table} RENAME COLUMN {} TO {}",
                            ident(old)?,
                            ident(name)?
                        ));
                        &existing[*old]
                    }
                    Some(old) => {
                        accounted.insert(old);
                        drift.push(format!("{}.{old} is to be renamed to {name}", model.table));
                        continue;
                    }
                    None if policy.add_columns => {
                        changes.push(format!(
                            "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {}",
                            TableSchema::column_ddl(column, storage.column(column.name))?
                        ));
                        continue;
                    }
                    None => {
                        drift.push(format!("{}.{name} is missing", model.table));
                        continue;
                    }
                }
            }
        };
        compare_column(
            model.table,
            &table,
            column,
            storage.column(column.name),
            current,
            policy,
            compression,
            &mut changes,
            &mut drift,
        )?;
    }
    plan_partitioning(
        client,
        model,
        found,
        &table,
        policy,
        &mut changes,
        &mut drift,
    )
    .await?;
    plan_row_compression(client, model, storage.row, found, &table, compression, &mut changes, &mut drift).await;
    let mut extra: Vec<&String> = existing
        .keys()
        .filter(|n| !accounted.contains(n.as_str()))
        .collect();
    extra.sort();
    for name in extra {
        if policy.drop_columns {
            changes.push(format!(
                "ALTER TABLE {table} DROP COLUMN IF EXISTS {}",
                ident(name)?
            ));
        } else {
            drift.push(format!("{}.{name} is not in the model", model.table));
        }
    }
    if policy.keys || !policy.fail_on_drift {
        // Unique keys are visible (DESCRIBE's is_unique, by any constraint
        // name); indexes are not, so their IF NOT EXISTS statements are
        // re-issued each time.
        let described = crate::admin::Admin::new(client).columns(found).await?;
        let unique: HashSet<&str> = described
            .iter()
            .filter(|c| c.unique && !c.dropped)
            .map(|c| c.name.as_str())
            .collect();
        for column in model.columns {
            // A column renamed by this plan is described under its old name.
            let names = std::iter::once(column.name).chain(column.renamed_from.iter().copied());
            let has_unique = names.clone().any(|n| unique.contains(n));
            if column.unique && !column.primary_key && !has_unique {
                if policy.keys {
                    let index = crate::admin::Index::on(model.table, &[column.name])
                        .unique()
                        .if_not_exists();
                    changes.push(index.to_sql()?);
                } else {
                    drift.push(format!("{}.{} has no unique key", model.table, column.name));
                }
            }
            if let Some(method) = column.index
                && policy.keys
            {
                let index = crate::admin::Index::on(model.table, &[column.name])
                    .using(method)
                    .if_not_exists();
                ensure.push(index.to_sql()?);
            }
        }
    }
    // A ledger table takes only what adds: new nullable columns and indexes.
    // Anything else would be refused by the server halfway through an apply,
    // so it is reported here instead, and the engine's hidden insert-time
    // column is never a difference.
    if storage.ledger.is_some() {
        drift.retain(|d| !d.contains("__ledger_ts"));
        let (keep, blocked): (Vec<String>, Vec<String>) = changes.into_iter().filter(|c| !c.contains("__ledger_ts")).partition(|c| {
            let u = c.to_ascii_uppercase();
            u.starts_with("CREATE ") || (u.contains(" ADD COLUMN ") && !u.contains("NOT NULL") && !u.contains(" DEFAULT "))
        });
        changes = keep;
        drift.extend(blocked.into_iter().map(|c| format!("blocked on ledger table {}: {c}", model.table)));
    }
    Ok(ModelSync {
        changes,
        ensure,
        drift,
    })
}

/// The partition key `SHOW TABLE STATS` reports for `table`, if it is
/// partitioned.
async fn partition_key(client: &Client, table: &str) -> Result<Option<Vec<String>>> {
    let stats = crate::admin::Admin::new(client).table_stats(table).await?;
    Ok(stats
        .into_iter()
        .find(|m| m.metric == "partition_key")
        .and_then(|m| m.value)
        .map(|key| key.split(',').map(|c| c.trim().to_string()).collect()))
}

/// Partitioning drift: a model partitioned over a plain table is converted
/// under [`SyncPolicy::convert_partitioning`], and reported otherwise; the
/// other differences (a partitioned table the model does not partition, a
/// different key) have no conversion and are always drift.
#[allow(clippy::too_many_arguments)]
async fn plan_partitioning(
    client: &Client,
    model: &TableSchema,
    found: &str,
    table: &str,
    policy: &SyncPolicy,
    changes: &mut Vec<String>,
    drift: &mut Vec<String>,
) -> Result<()> {
    let current = partition_key(client, found).await?;
    let wanted: Option<Vec<String>> = model
        .partition
        .map(|p| p.columns.iter().map(|c| c.to_string()).collect());
    match (wanted, current) {
        (None, None) => {}
        (Some(_), None) => {
            let clause = model.partition.expect("wanted").clause()?;
            if policy.convert_partitioning {
                changes.push(format!("ALTER TABLE {table} {clause}"));
            } else {
                drift.push(format!(
                    "{} is a plain table; the model says {clause} (an offline conversion that \
                     holds the table exclusively while it copies every row: allow it with \
                     SyncPolicy::convert_partitioning)",
                    model.table
                ));
            }
        }
        (None, Some(key)) => drift.push(format!(
            "{} is partitioned by HASH ({}); the model is not partitioned, and a partitioned \
             table cannot be converted back",
            model.table,
            key.join(", ")
        )),
        (Some(wanted), Some(key)) if wanted != key => drift.push(format!(
            "{} is partitioned by HASH ({}); the model says HASH ({}), and a table cannot be \
             partitioned again by another key",
            model.table,
            key.join(", "),
            wanted.join(", ")
        )),
        (Some(_), Some(_)) => {}
    }
    Ok(())
}

/// The table's row compression from `pg_class.reloptions`
/// (`row_compression=zstd(9)`): `Some(None)` for none, `None` when the
/// server could not say.
async fn current_row_compression(client: &Client, table: &str) -> Option<Option<String>> {
    let rows: Vec<(Option<String>,)> = client
        .fetch_all("SELECT reloptions::text FROM pg_class WHERE relname = $1", params![table])
        .await
        .ok()?;
    let options = rows.into_iter().next().and_then(|(o,)| o).unwrap_or_default();
    let found = options.split_once("row_compression=").map(|(_, rest)| {
        // Up to the element's end: a comma, the array's brace or a quote.
        let mut depth = 0i32;
        rest.chars()
            .take_while(|c| match c {
                '(' => {
                    depth += 1;
                    true
                }
                ')' => {
                    depth -= 1;
                    depth >= 0
                }
                ',' | '}' | '"' => depth > 0,
                _ => true,
            })
            .collect::<String>()
    });
    Some(found)
}

/// Row compression drift: the model's `#[rdb(row_compression = …)]` against
/// the table's, fixed with `SET (row_compression = …)` / `RESET
/// (row_compression)` under [`Migrator::sync_compression`] (rows written from
/// then on; `ALTER TABLE … RECOMPRESS` rewrites the rest).
#[allow(clippy::too_many_arguments)]
async fn plan_row_compression(
    client: &Client,
    model: &TableSchema,
    wanted: Option<&'static str>,
    found: &str,
    table: &str,
    sync: bool,
    changes: &mut Vec<String>,
    drift: &mut Vec<String>,
) {
    match current_row_compression(client, found).await {
        Some(now) if now.as_deref() != wanted => {
            let statement = match wanted {
                Some(method) => format!("ALTER TABLE {table} SET (row_compression = '{method}')"),
                None => format!("ALTER TABLE {table} RESET (row_compression)"),
            };
            if sync {
                changes.push(statement);
            } else {
                drift.push(format!(
                    "{}'s rows are compressed with {}, the model says {}",
                    model.table,
                    now.as_deref().unwrap_or("none"),
                    wanted.unwrap_or("none")
                ));
            }
        }
        Some(_) => {}
        None if wanted.is_some() => drift.push(format!(
            "{}: the server did not say how its rows are compressed; the model says {}",
            model.table,
            wanted.unwrap_or("none")
        )),
        None => {}
    }
}

fn compare_column(
    table_name: &str,
    table: &str,
    column: &ColumnDef,
    compression: Option<&str>,
    current: &Existing,
    policy: &SyncPolicy,
    sync_compression: bool,
    changes: &mut Vec<String>,
    drift: &mut Vec<String>,
) -> Result<()> {
    let name = ident(column.name)?;
    if let Some(wanted) = catalogue_type(column.sql_type)
        && wanted != current.data_type
    {
        if policy.retype && !column.serial {
            changes.push(format!(
                "ALTER TABLE {table} ALTER COLUMN {name} TYPE {}",
                column.sql_type
            ));
        } else {
            drift.push(format!(
                "{table_name}.{} is {}, the model says {}",
                column.name, current.data_type, column.sql_type
            ));
        }
    }
    // How it stores its values: the model's method, or none.
    let wanted = compression.unwrap_or("");
    match current.compression.as_deref() {
        Some(now) if now != wanted => {
            let method = if wanted.is_empty() { "default" } else { wanted };
            if sync_compression {
                changes.push(format!(
                    "ALTER TABLE {table} ALTER COLUMN {name} SET COMPRESSION {method}"
                ));
            } else {
                drift.push(format!(
                    "{table_name}.{} is compressed with {}, the model says {}",
                    column.name,
                    if now.is_empty() { "none" } else { now },
                    if wanted.is_empty() { "none" } else { wanted }
                ));
            }
        }
        Some(_) => {}
        None if !wanted.is_empty() => drift.push(format!(
            "{table_name}.{}: the server did not say how it is compressed; the model says {wanted}",
            column.name
        )),
        None => {}
    }
    // A serial or key column is NOT NULL by construction; the model's Option
    // does not decide it.
    if !column.serial && !column.primary_key && column.nullable != current.nullable {
        let (verb, says) = if column.nullable {
            ("DROP", "nullable")
        } else {
            ("SET", "NOT NULL")
        };
        if policy.nullability {
            changes.push(format!(
                "ALTER TABLE {table} ALTER COLUMN {name} {verb} NOT NULL"
            ));
        } else {
            drift.push(format!(
                "{table_name}.{}: the model says {says}",
                column.name
            ));
        }
    }
    Ok(())
}

impl Client {
    /// Applies `migrator`: see [`Migrator::apply`].
    pub async fn migrate(&self, migrator: &Migrator) -> Result<MigrationReport> {
        migrator.apply(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(names: &[(&str, &Migrator)]) -> HashMap<String, (i64, Timestamp)> {
        names
            .iter()
            .map(|(n, m)| {
                let step = m.find(n);
                (n.to_string(), (checksum(&step.body), Timestamp::UNIX_EPOCH))
            })
            .collect()
    }

    fn plan(m: &Migrator, recorded: &HashMap<String, (i64, Timestamp)>) -> MigrationPlan {
        let mut p = MigrationPlan::default();
        m.plan_steps(recorded, &mut p);
        p
    }

    #[test]
    fn steps_plan_by_their_records() {
        let m = Migrator::new()
            .step("1", &["A"])
            .step("2", &["B"])
            .repeatable("r", &["R"])
            .step("3", &["C"]);
        let p = plan(&m, &HashMap::new());
        assert_eq!(p.pending, ["1", "2", "3"]);
        assert_eq!(p.repeatable, ["r"]);

        let mut recorded = rec(&[("1", &m), ("3", &m), ("r", &m)]);
        let p = plan(&m, &recorded);
        assert_eq!(p.pending, ["2"]);
        assert_eq!(p.out_of_order, ["2"], "2 sorts before the applied 3");
        assert!(p.repeatable.is_empty());

        recorded.insert("9".into(), (0, Timestamp::UNIX_EPOCH));
        recorded.get_mut("1").unwrap().0 ^= 1;
        recorded.get_mut("r").unwrap().0 ^= 1;
        let p = plan(&m, &recorded);
        assert_eq!(p.unknown, ["9"]);
        assert_eq!(p.changed, ["1"]);
        assert_eq!(p.repeatable, ["r"], "a changed repeatable step runs again");
    }

    #[test]
    fn a_baseline_covers_a_prefix_of_an_empty_record() {
        let m = Migrator::new()
            .step("1", &["A"])
            .step("2", &["B"])
            .step("3", &["C"])
            .baseline("2");
        let p = plan(&m, &HashMap::new());
        assert_eq!(p.baseline, ["1", "2"]);
        assert_eq!(p.pending, ["3"]);
        let p = plan(&m, &rec(&[("1", &m)]));
        assert!(p.baseline.is_empty(), "only for a database with no records");
        assert!(
            Migrator::new()
                .step("1", &[])
                .baseline("x")
                .check_names()
                .is_err()
        );
        assert!(
            Migrator::new()
                .step("1", &[])
                .step("1", &[])
                .check_names()
                .is_err()
        );
    }

    #[test]
    fn a_step_that_cannot_run_over_native_is_refused_before_anything_runs() {
        let refused = |m: Migrator| match m.check_names() {
            Err(Error::Unsupported(msg)) => msg,
            other => panic!("{other:?}"),
        };
        let msg = refused(Migrator::new().step("1", &["CREATE TABLE a (x INT)"]).step(
            "2",
            &[
                "CREATE TABLE b (x INT)",
                "INSERT INTO b VALUES (1); INSERT INTO b VALUES (2)",
            ],
        ));
        assert!(msg.contains("\"2\", statement 2"), "{msg}");
        let msg = refused(Migrator::new().step("1", &["BEGIN"]));
        assert!(msg.contains("\"1\", statement 1"), "{msg}");
        let msg = refused(
            Migrator::new()
                .step("1", &["CREATE TABLE a (x INT)"])
                .down(&["DROP TABLE a; DROP TABLE b"]),
        );
        assert!(msg.contains("down step \"1\""), "{msg}");
        // A trailing semicolon, and one inside a literal, are one statement.
        assert!(
            Migrator::new()
                .step(
                    "1",
                    &[
                        "INSERT INTO a VALUES ('x;y');",
                        "UPDATE a SET x = 1 -- ; no"
                    ]
                )
                .check_names()
                .is_ok()
        );
    }

    #[test]
    fn checksums_see_statement_boundaries_and_code_versions() {
        let a = checksum(&Body::Sql(vec!["AB".into(), "C".into()]));
        let b = checksum(&Body::Sql(vec!["A".into(), "BC".into()]));
        assert_ne!(a, b);
        let run: StepFn =
            Arc::new(|_c: &Client| Box::pin(async { Ok(()) }) as BoxFuture<'_, Result<()>>);
        let v1 = checksum(&Body::Code {
            version: "v1".into(),
            run: run.clone(),
        });
        let v2 = checksum(&Body::Code {
            version: "v2".into(),
            run,
        });
        assert_ne!(v1, v2);
    }

    #[test]
    fn catalogue_names() {
        assert_eq!(catalogue_type("BIGINT"), Some("bigint"));
        assert_eq!(catalogue_type("NUMERIC(10,2)"), Some("numeric"));
        assert_eq!(catalogue_type("varchar(64)"), Some("character varying"));
        assert_eq!(catalogue_type("TEXT[]"), Some("ARRAY"));
        assert_eq!(
            catalogue_type("TIMESTAMPTZ"),
            Some("timestamp with time zone")
        );
        assert_eq!(catalogue_type("POINT"), None);
    }
}
