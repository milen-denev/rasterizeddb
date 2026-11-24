//! Strict tenant mode (sharding-plan.md §4.10, T1–T10): one database per
//! customer, and nothing process-wide reachable from a customer's database.
//!
//! - **T1, process admin.** Only a superuser of the operator's database
//!   administers the process; a customer database's superuser administers
//!   that database alone.
//! - **T2/T3/T4, introspection.** Other databases are invisible, and
//!   process-wide `SHOW`s, query statistics and other sessions' statements
//!   need process admin.
//! - **T5, session control.** Cancelling needs process admin outside one's
//!   own sessions.
//! - **T7, limits** per database, from the configuration.
//! - **T8/T9, paths and outbound connections** (`STORAGE`, `COPY` to files,
//!   `IMPORT`) need process admin.
//! - **T10, the Redis port** is refused for customers.
//!
//! The security floor (roles plan R0) holds underneath in every build; this
//! only refuses more.

use std::collections::BTreeMap;

use rasterizeddb_core::core::policy_hooks::{ProcessSurface, SessionContext, TenancyPolicy, TenantLimits};

use crate::EnterpriseConfig;

pub struct StrictTenancy {
    operator_database: String,
    limits: BTreeMap<String, TenantLimits>,
    directory: crate::tenants::Directory,
}

impl StrictTenancy {
    pub fn new(cfg: &EnterpriseConfig) -> Result<Self, String> {
        Ok(StrictTenancy {
            operator_database: cfg.operator_database.clone(),
            limits: cfg.limits.clone(),
            directory: crate::tenants::Directory::open(cfg.tenants_file.clone())?,
        })
    }
}

impl TenancyPolicy for StrictTenancy {
    fn admit(&self, ctx: &SessionContext<'_>) -> Result<(), String> {
        if ctx.protocol == "redis" && ctx.database != self.operator_database {
            return Err("the Redis protocol is not offered to tenants in strict tenant mode".to_string());
        }
        Ok(())
    }

    fn may_see_database(&self, ctx: &SessionContext<'_>, name: &str) -> bool {
        name == ctx.database || self.is_process_admin(ctx)
    }

    fn may_use(&self, ctx: &SessionContext<'_>, surface: ProcessSurface) -> Result<(), String> {
        if self.is_process_admin(ctx) {
            return Ok(());
        }
        let what = match surface {
            ProcessSurface::SetConfig => "process-wide settings",
            ProcessSurface::StoragePaths => "server storage paths",
            ProcessSurface::Import => "importing from another server",
            ProcessSurface::CopyFile => "server files and programs",
            ProcessSurface::QueryStats => "other databases' statements and statistics",
            ProcessSurface::Cancel => "other sessions",
            ProcessSurface::Show => "process-wide state",
            ProcessSurface::Databases => "other databases",
        };
        Err(format!(
            "42501: {what} are reserved to the operator in strict tenant mode (database \"{}\")",
            self.operator_database
        ))
    }

    fn limits(&self, database: &str) -> TenantLimits {
        // The directory's (set by CREATE/ALTER TENANT) over the file's.
        self.directory
            .limits(database)
            .or_else(|| self.limits.get(database).cloned())
            .unwrap_or_default()
    }

    fn rewrite_statement(&self, ctx: &SessionContext<'_>, sql: &str) -> Option<Result<Vec<String>, String>> {
        let r = self.directory.rewrite(sql)?;
        if !self.is_process_admin(ctx) {
            return Some(Err(format!(
                "42501: tenant statements are the operator's (a superuser of \"{}\")",
                self.operator_database
            )));
        }
        Some(r)
    }

    fn is_process_admin(&self, ctx: &SessionContext<'_>) -> bool {
        ctx.superuser && ctx.database == self.operator_database
    }
}
