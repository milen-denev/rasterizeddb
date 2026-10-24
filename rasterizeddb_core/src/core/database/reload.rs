//! A reload under ACCESS EXCLUSIVE (slice B4, `docs/persistent-indexes.md`
//! §7 "Steps of a reloading DDL" and "As built (B4)").
//!
//! Every DDL that changes what a table's open instance must know (CREATE and
//! DROP INDEX, path indexes, a partial unique index's drop, every ALTER arm)
//! ends in [`Database::reload_with`], which runs §7's steps in order:
//!
//! 1. **Lock.** The caller already holds ACCESS EXCLUSIVE on the table and
//!    on the rest of its complete lock set (the FK children whose sidecars
//!    it writes, a parent it reads: finding L11), taken through
//!    `table_for` / `lock_names` before anything was written. This only
//!    asserts it ([`Database::exclusive_held`], a debug assertion), and from
//!    here to the swap no table-lock wait may happen (asserted in
//!    `lock_table`), since one would be unbounded under `lock_timeout = 0`
//!    and land a 40P01 halfway through. `forget_pause` is taken now, after
//!    the lock, not for the whole wait (N31).
//! 2. **Retire** the old instance: its maintenance stops and any vacuum work
//!    in flight on it is joined ([`Table::retire`]). A statement still
//!    holding it (only a lockless metadata reader can) keeps reading it.
//! 3. **Work**: the DDL's sidecar writes, passed in as `work` and run here,
//!    under the lock, after retirement; then the new instance is opened from
//!    disk. A failing `work` still gets a **fresh** instance opened and
//!    swapped in: a retired instance is never put back (N14, N23).
//! 4. **Swap** the new instance into the name's slot (the slot stays, so a
//!    lockless reader never sees the name missing), and only then bump
//!    `schema_generation`: a reader that sees the new generation sees the
//!    new instance. ACCESS EXCLUSIVE is released by the statement's owner
//!    when the statement ends, after its durability step.
//!
//! Steps 2-4 run on the caller's task, not a spawned one (a deviation from
//! §7, which spawns them so nothing can cancel them): cancellation here is
//! cooperative (`core/cancel.rs`: a statement future is never dropped to
//! stop it), polled only by lock waits and scans, and steps 2-4 make
//! neither. A spawned task would also lose the statement's owner scope.

use std::future::Future;
use std::io;

use rclite::Arc;

use super::{Database, SchemaChanged, seed_in};
use crate::core::row::table::Table;
use crate::core::storage_providers::backend::DbStorageProvider;

type DbTable = Table<DbStorageProvider>;

tokio::task_local! {
    /// Set while a reload's steps 2-4 run on this task.
    static RELOAD_STEPS: ();
}

/// Whether the caller runs inside a reload's steps 2-4, where §7 step 1
/// forbids a table-lock wait.
pub(crate) fn in_reload_steps() -> bool {
    RELOAD_STEPS.try_with(|_| ()).is_ok()
}

/// Why [`Database::reload_with`] failed.
#[derive(Debug)]
pub(crate) enum ReloadFailure {
    /// The DDL's own work failed; a fresh instance was opened and swapped in
    /// regardless, so the table reads whatever the work left on disk.
    Work(String),
    /// The table could not be (re)opened: absent, partitioned, or its schema
    /// unreadable (then it is absent now rather than stale).
    Reopen(io::Error),
}

impl std::fmt::Display for ReloadFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReloadFailure::Work(e) => f.write_str(e),
            ReloadFailure::Reopen(e) => write!(f, "{e}"),
        }
    }
}

impl From<ReloadFailure> for io::Error {
    fn from(failure: ReloadFailure) -> io::Error {
        match failure {
            ReloadFailure::Work(e) => io::Error::other(e),
            ReloadFailure::Reopen(e) => e,
        }
    }
}

impl Database {
    /// Reopens `table_name` from what is on disk: [`Database::reload_with`]
    /// with no work of its own, for a DDL whose sidecar writes already ran
    /// under its complete lock set.
    pub async fn reload_table(&self, table_name: &str) -> io::Result<()> {
        self.reload_with(table_name, |_| async { Ok(()) })
            .await
            .map_err(io::Error::from)
    }

    /// Runs `work` as step 3 of a reload of `table_name` under the ACCESS
    /// EXCLUSIVE the caller holds, and swaps the reopened instance in (see
    /// the module comment for the steps). `work` gets the old instance, for
    /// its `io_schema`; it must not take a table lock.
    pub(crate) async fn reload_with<T, F, Fut>(&self, table_name: &str, work: F) -> Result<T, ReloadFailure>
    where
        F: FnOnce(Arc<DbTable>) -> Fut,
        Fut: Future<Output = Result<T, String>>,
    {
        self.reload_with_instance(table_name, |old| async move { work(old).await.map(|v| (v, None)) })
            .await
    }

    /// [`Database::reload_with`] whose `work` may hand back the new instance
    /// itself, already open over the files it wrote (an ALTER rewrite fills
    /// the instance it opened, before anyone can see it): step 3 then swaps
    /// that one in instead of opening the files again. `None` opens them as
    /// `reload_with` does. A failing `work` still gets a fresh instance from
    /// disk; whatever it opened is its own to retire.
    pub(crate) async fn reload_with_instance<T, F, Fut>(
        &self,
        table_name: &str,
        work: F,
    ) -> Result<T, ReloadFailure>
    where
        F: FnOnce(Arc<DbTable>) -> Fut,
        Fut: Future<Output = Result<(T, Option<Arc<DbTable>>), String>>,
    {
        // The map key DDL mutates (`resolve_key`): where identifier case
        // folding drops in, so every mutator removes the slot a lookup finds.
        let resolved = self.resolve_key(table_name);
        let table_name = resolved.as_deref().unwrap_or(table_name);
        // Step 1: the caller's lock set, asserted.
        debug_assert!(
            self.exclusive_held(table_name),
            "reload of \"{table_name}\" without ACCESS EXCLUSIVE on it (§7 step 1)"
        );
        // Before `tables` is asked: the reopen below builds a plain table,
        // and a partitioned one with no shell must not read as missing
        // (ALTER and CREATE INDEX reach here).
        self.refuse_partitioned(table_name, "reloading a table's schema")
            .map_err(ReloadFailure::Reopen)?;
        let Some(old) = self.table_held(table_name) else {
            return Err(ReloadFailure::Reopen(io::Error::new(
                io::ErrorKind::NotFound,
                "Table not found",
            )));
        };
        // After the lock: the database's forgotten horizon must not move
        // between the retirement and the swap, or the reopened files could
        // carry ids it forgot (`ForgetRegistry`).
        let _forget_pause = self.transactions.forget_pause();
        // Every schema change comes through here, so this is where a
        // client's cached column list becomes stale (`schema_generation`).
        // Bumped again after the swap: see `SchemaChanged`.
        let changed = SchemaChanged(&self.schema_generation);
        self.schema_generation
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);

        RELOAD_STEPS
            .scope((), async move {
                // Step 2: retire.
                old.retire().await;
                #[cfg(any(test, feature = "test_tokens"))]
                hooks::reached(self, table_name, hooks::Step::Retired, &old);

                // Step 3: the DDL's work, then the reopen (unless the work
                // opened the new instance itself).
                let (worked, opened) = match work(old.clone()).await {
                    Ok((value, opened)) => (Ok(value), opened),
                    Err(e) => (Err(e), None),
                };
                let reopened = match opened {
                    Some(table) => Ok(table),
                    None => self.open_instance(table_name).await,
                };
                let table = match reopened {
                    Ok(table) => table,
                    Err(e) => {
                        // The old instance is retired and never goes back:
                        // the table is absent rather than stale, and a query
                        // says it does not exist instead of reading it with
                        // no columns.
                        self.take_table(table_name);
                        return Err(ReloadFailure::Reopen(e));
                    }
                };

                // Step 4: the swap, then the generation.
                #[cfg(any(test, feature = "test_tokens"))]
                hooks::reached(self, table_name, hooks::Step::BeforeSwap, &old);
                self.swap_instance(table_name, table);
                #[cfg(any(test, feature = "test_tokens"))]
                hooks::reached(self, table_name, hooks::Step::Swapped, &old);
                drop(changed);
                worked.map_err(ReloadFailure::Work)
            })
            .await
    }

    /// A new instance of `table_name` opened from its files where they are
    /// placed now, scoped and versioned like every registered table. Errs
    /// when its schema cannot be read: publishing it would be a table with
    /// no columns.
    pub(crate) async fn open_instance(&self, table_name: &str) -> io::Result<Arc<DbTable>> {
        let seed = seed_in(
            &self.db_io_pointers,
            &self.storage,
            &self.kv,
            &self.seeds,
            table_name,
        )
        .await;
        let mut table = Table::new(table_name, seed, Vec::new()).await;
        if table.schema_unreadable {
            return Err(io::Error::other(format!(
                "table '{}' could not be reopened: its schema could not be read",
                table_name
            )));
        }
        table.scope_cache_to_database(self.id);
        table.enable_versioning(self.transactions.clone());
        Ok(Arc::new(table))
    }

    /// Retires the instance `table_name`'s slot holds (§7 step 2), which the
    /// caller's ACCESS EXCLUSIVE covers, and returns it; the slot keeps it
    /// until a swap or a removal. `None` when nothing is registered.
    pub(crate) async fn retire_current(&self, table_name: &str) -> Option<Arc<DbTable>> {
        let table = self.table_held(table_name)?;
        table.retire().await;
        Some(table)
    }

    /// After a DROP, RENAME or MOVE failed past retirement: a fresh instance
    /// from disk is swapped into the slot, never the retired one (N14, N23).
    /// If that cannot be opened, the slot is removed: absent, not stale.
    pub(crate) async fn reopen_fresh(&self, table_name: &str) {
        // The key its caller resolved already; asked again so a removal
        // below takes the slot a lookup by this name finds.
        let resolved = self.resolve_key(table_name);
        let table_name = resolved.as_deref().unwrap_or(table_name);
        match self.open_instance(table_name).await {
            Ok(table) => {
                self.swap_instance(table_name, table);
            }
            Err(e) => {
                log::error!(
                    "{} could not be reopened after a failed change: {}; it is absent until the next start",
                    table_name,
                    e
                );
                self.take_table(table_name);
            }
        }
    }
}

/// Test probes at a reload's steps, so a test can look at the slot and the
/// generation between them. Off in every server build.
#[cfg(any(test, feature = "test_tokens"))]
pub mod hooks {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Step {
        /// Step 2 done: the old instance is retired.
        Retired,
        /// Step 3 done: the new instance is open, not yet in the slot.
        BeforeSwap,
        /// The new instance is in the slot.
        Swapped,
    }

    /// What a probe saw: the step, `schema_generation` then, and whether
    /// the slot still held the old instance.
    #[derive(Debug, Clone, Copy)]
    pub struct Seen {
        pub step: Step,
        pub generation: u64,
        pub slot_holds_old: bool,
        pub old_retired: bool,
    }

    type Probe = std::sync::Arc<dyn Fn(Seen) + Send + Sync>;

    static PROBES: parking_lot::Mutex<Vec<(String, Probe)>> = parking_lot::Mutex::new(Vec::new());

    /// Calls `probe` at each step of every reload of `table` until the
    /// returned guard is dropped. Keyed by table name, so tests on other
    /// tables are not disturbed.
    pub fn watch(table: &str, probe: impl Fn(Seen) + Send + Sync + 'static) -> Watch {
        PROBES.lock().push((table.to_string(), std::sync::Arc::new(probe)));
        Watch(table.to_string())
    }

    pub struct Watch(String);

    impl Drop for Watch {
        fn drop(&mut self) {
            PROBES.lock().retain(|(t, _)| *t != self.0);
        }
    }

    pub(super) fn reached(db: &Database, table: &str, step: Step, old: &Arc<DbTable>) {
        let probes: Vec<Probe> = PROBES
            .lock()
            .iter()
            .filter(|(t, _)| t == table)
            .map(|(_, p)| p.clone())
            .collect();
        if probes.is_empty() {
            return;
        }
        let seen = Seen {
            step,
            generation: db.schema_generation.load(std::sync::atomic::Ordering::Acquire),
            slot_holds_old: db
                .table_held(table)
                .is_some_and(|current| Arc::ptr_eq(&current, old)),
            old_retired: old.is_retired(),
        };
        for probe in probes {
            probe(seen);
        }
    }
}
