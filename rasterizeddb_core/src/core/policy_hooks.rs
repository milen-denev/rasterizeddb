//! The hook traits the dual-licensed crates implement (sharding-plan.md
//! §10, D12): tenancy (`rasterizeddb_enterprise`'s `tenant_isolation`) and
//! placement (`segregation_policies`).
//!
//! **These are GPL and always built.** The engine calls them at fixed
//! points; the default implementations are exactly today's behaviour, so a
//! build without the licensed crates behaves as if the hooks did not exist.
//! A licensed crate installs its implementation once at start-up
//! (`rasterizeddb_server`'s `enterprise` feature); nothing else may.
//!
//! The security floor (roles plan R0) is *not* a hook: it holds in every
//! build. A hook can only refuse more, never admit what the floor refuses.

use std::sync::OnceLock;

/// Who is asking, at the points a tenancy policy is consulted.
#[derive(Clone, Debug)]
pub struct SessionContext<'a> {
    pub role: &'a str,
    pub database: &'a str,
    /// The front end the session came in on (`pgwire`, `native`, `mysql`,
    /// `tds`, `rshard`).
    pub protocol: &'a str,
    pub superuser: bool,
}

/// Process-wide surfaces a tenant must not reach in strict tenant mode
/// (sharding-plan.md §4.10, T1–T10).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessSurface {
    /// `SET CONFIG` of a process-wide key.
    SetConfig,
    /// `STORAGE …` and other statements naming server paths.
    StoragePaths,
    /// `IMPORT … FROM POSTGRES`.
    Import,
    /// `COPY … TO/FROM` a server file or program.
    CopyFile,
    /// Query statistics and recent statements of other databases.
    QueryStats,
    /// Cancelling another session's statement.
    Cancel,
    /// `SHOW` of process-wide state (config, replication, backups).
    Show,
    /// Creating, dropping or switching databases.
    Databases,
}

/// Per-tenant resource limits; `None` fields are unlimited.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TenantLimits {
    pub max_connections: Option<u32>,
    pub max_bytes: Option<u64>,
    pub statement_timeout_ms: Option<u64>,
}

/// Tenant isolation's hook (module docs). Every method's default is today's
/// behaviour.
pub trait TenancyPolicy: Send + Sync + 'static {
    /// A session is about to open (after authentication).
    fn admit(&self, _ctx: &SessionContext<'_>) -> Result<(), String> {
        Ok(())
    }

    /// Whether `ctx` may see that database `name` exists (catalogue listings,
    /// `\l`, error messages).
    fn may_see_database(&self, _ctx: &SessionContext<'_>, _name: &str) -> bool {
        true
    }

    /// Whether `ctx` may use a process-wide surface.
    fn may_use(&self, _ctx: &SessionContext<'_>, _surface: ProcessSurface) -> Result<(), String> {
        Ok(())
    }

    /// The limits for sessions of database `name`.
    fn limits(&self, _database: &str) -> TenantLimits {
        TenantLimits::default()
    }

    /// Whether the role is a process administrator: may administer the
    /// whole server, not only its own database. Today every superuser is;
    /// the roles plan's R1 `PROCESS ADMIN` attribute narrows it, and strict
    /// tenant mode narrows it further to the default database's.
    fn is_process_admin(&self, ctx: &SessionContext<'_>) -> bool {
        ctx.superuser
    }
}

/// Placement's hook: which shards a range or a tenant may live on.
pub trait PlacementPolicy: Send + Sync + 'static {
    /// Whether level-0 blocks `[lo, hi)` may be placed on `shard`, which
    /// carries `labels` (region, hardware, compliance tier). Consulted by
    /// every map edit, move and rebalance.
    fn may_place(&self, _lo: u64, _hi: u64, _shard: u32, _labels: &[String]) -> Result<(), String> {
        Ok(())
    }

    /// Ranges a policy pins where they are: the balancer never moves them.
    fn pinned(&self) -> Vec<(u64, u64)> {
        Vec::new()
    }

    /// Whether this shard hosts per-user-shard role `role` of `database`
    /// (G11): a gateway's pinned session is admitted only then, so a stale
    /// or forged mapping lands nowhere. Nobody is hosted by default.
    fn hosts_pinned_role(&self, _database: &str, _role: &str) -> bool {
        false
    }
}

/// Today's behaviour.
struct Open;
impl TenancyPolicy for Open {}
impl PlacementPolicy for Open {}

static TENANCY: OnceLock<Box<dyn TenancyPolicy>> = OnceLock::new();
static PLACEMENT: OnceLock<Box<dyn PlacementPolicy>> = OnceLock::new();

/// Installs the tenancy policy; once per process, before the first session.
pub fn install_tenancy(policy: Box<dyn TenancyPolicy>) -> Result<(), String> {
    TENANCY.set(policy).map_err(|_| "a tenancy policy is already installed".to_string())
}

/// Installs the placement policy; once per process.
pub fn install_placement(policy: Box<dyn PlacementPolicy>) -> Result<(), String> {
    PLACEMENT.set(policy).map_err(|_| "a placement policy is already installed".to_string())
}

/// The tenancy policy in force.
pub fn tenancy() -> &'static dyn TenancyPolicy {
    match TENANCY.get() {
        Some(p) => p.as_ref(),
        None => &Open,
    }
}

/// The placement policy in force.
pub fn placement() -> &'static dyn PlacementPolicy {
    match PLACEMENT.get() {
        Some(p) => p.as_ref(),
        None => &Open,
    }
}
