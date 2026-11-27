//! RasterizedDB enterprise features (licensed: see LICENSE).
//!
//! Implements the GPL hooks of `rasterizeddb_core::core::policy_hooks`:
//! - [`tenancy::StrictTenancy`], strict tenant mode for database-as-a-service
//!   (sharding-plan.md §4.10, T1–T10);
//! - [`placement::LabelPlacement`], placement policies over shard labels
//!   (enterprise segregation), and the per-user-shard placement list.
//!
//! [`install`] reads one configuration file and installs both, once, at
//! start-up, before the first session.

#[cfg(feature = "segregation_policies")]
pub mod placement;
#[cfg(feature = "tenant_isolation")]
pub mod tenancy;
#[cfg(feature = "tenant_isolation")]
pub mod tenants;

#[cfg(feature = "ledger")]
pub mod ledger;
#[cfg(feature = "encryption")]
pub mod crypto;
#[cfg(feature = "vault")]
pub mod vault;
#[cfg(feature = "vault")]
pub mod columns;

mod config;

pub use config::EnterpriseConfig;

/// Installs the hooks that hold whatever the configuration says: the ledger
/// guard. Called before the first catalogue opens, with or without an
/// enterprise configuration file, so a ledger table is never opened
/// unguarded by an enterprise build.
pub fn install_always() -> Result<(), String> {
    #[cfg(feature = "ledger")]
    ledger::install()?;
    // After the ledger guard: the cipher reads vault policies through it.
    #[cfg(feature = "vault")]
    columns::install()?;
    Ok(())
}

/// Installs the enterprise hooks described by the configuration file at
/// `path` (see [`EnterpriseConfig::parse`]).
pub fn install(path: &std::path::Path) -> Result<(), String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let cfg = EnterpriseConfig::parse(&text).map_err(|e| format!("{}: {e}", path.display()))?;
    #[cfg(feature = "tenant_isolation")]
    if cfg.tenant_mode_strict {
        rasterizeddb_core::core::policy_hooks::install_tenancy(Box::new(tenancy::StrictTenancy::new(&cfg)?))?;
        log::info!("strict tenant mode: process administration only in \"{}\"", cfg.operator_database);
    }
    #[cfg(feature = "ledger")]
    {
        if let Some(dir) = &cfg.ledger_dir {
            ledger::store::set_ledger_dir(dir.clone());
        }
        for sink in &cfg.ledger_anchors {
            ledger::anchor::add_global_sink(sink.clone());
        }
    }
    #[cfg(feature = "segregation_policies")]
    rasterizeddb_core::core::policy_hooks::install_placement(Box::new(placement::LabelPlacement::open(&cfg)?))?;
    Ok(())
}
