//! Ledger tables (docs/ledger-and-vault-plan.md §3): the guard the GPL core
//! asks through `ledger_hooks`, the policy each table records, trusted time,
//! the hash chain every committed ledger row joins, and the ledger
//! statements.
//!
//! The guard is an allowlist. It admits what a ledger table takes (new
//! rows, row copies that keep their hidden columns, maintenance that keeps
//! every visible row, a nullable `ADD COLUMN`) and refuses the rest,
//! including write kinds the core adds later. No role is exempt.

pub mod anchor;
pub mod clauses;
pub mod clock;
pub mod policy;
#[cfg(feature = "pqc")]
pub mod sign;
pub mod store;

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use rasterizeddb_core::core::ledger_hooks::{LedgerHooks, LedgerReply, LedgerRequest, WriteKind};
use rasterizeddb_core::core::mvcc::TxId;
use rasterizeddb_core::core::row::schema::{SpecialTable, TableSchema};

use policy::LedgerPolicy;
use store::{Item, OP_DELETE, OP_INSERT, OP_UPDATE};

/// The installed guard and its state.
pub struct LedgerGuard {
    /// Items written by running transactions, by writer id.
    pending: Mutex<HashMap<TxId, Vec<Item>>>,
    /// Folded policies, by (incarnation, sidecar length, overlay length).
    policies: RwLock<HashMap<(u128, usize, usize), Arc<LedgerPolicy>>>,
    /// Policy records an `ALTER LEDGER TABLE` appended in this process,
    /// by ledger id: the open table's schema still holds the bytes it was
    /// opened with, and these follow them.
    overlay: RwLock<HashMap<u128, Vec<u8>>>,
    /// The anchors each ledger id's policy names, learned as its rows are
    /// written: a commit adds them to its store's sinks.
    anchors_of: RwLock<HashMap<u128, Vec<String>>>,
    /// A vault's changes by running transactions, to be audited at their
    /// commit (`(store, vault id, line)`): audited as they ran, a SET or
    /// DELETE a ROLLBACK undid stayed in the audit as done.
    audits: Mutex<HashMap<TxId, Vec<(String, u128, String)>>>,
}

fn what(table: &TableSchema) -> &'static str {
    match table.special {
        SpecialTable::Vault => "a secret vault",
        _ => "a ledger table",
    }
}

impl LedgerGuard {
    pub fn new() -> LedgerGuard {
        LedgerGuard {
            pending: Mutex::new(HashMap::new()),
            policies: RwLock::new(HashMap::new()),
            overlay: RwLock::new(HashMap::new()),
            anchors_of: RwLock::new(HashMap::new()),
            audits: Mutex::new(HashMap::new()),
        }
    }

    /// Audits `line` (a change to vault `vault`) when `tx` commits, and
    /// never if it aborts. Outside a transaction (`tx` 0) at once.
    #[cfg(feature = "vault")]
    pub(crate) fn audit_at_commit(&self, store_key: &str, vault: u128, tx: TxId, line: String) -> Result<(), String> {
        if tx == 0 {
            return crate::vault::audit_now(store_key, vault, &line);
        }
        self.audits.lock().entry(tx).or_default().push((store_key.to_string(), vault, line));
        Ok(())
    }

    /// The policy in force for `table`: its sidecar's records, then any this
    /// process appended since it opened.
    pub fn policy(&self, table: &TableSchema) -> Result<Arc<LedgerPolicy>, String> {
        let base_key = (table.incarnation, table.ledger_policy.len(), 0);
        let cached = self.policies.read().get(&base_key).cloned();
        let base = match cached {
            Some(p) => p,
            None => {
                let p = Arc::new(
                    policy::fold(&table.ledger_policy, table.special)
                        .map_err(|e| format!("XX001: \"{}\" is {}, and {e}", table.name, what(table)))?,
                );
                self.policies.write().insert(base_key, p.clone());
                p
            }
        };
        let extra = match self.overlay.read().get(&base.id) {
            Some(extra) if !extra.is_empty() => extra.clone(),
            _ => return Ok(base),
        };
        let key = (table.incarnation, table.ledger_policy.len(), extra.len());
        if let Some(p) = self.policies.read().get(&key).cloned() {
            return Ok(p);
        }
        let mut bytes = table.ledger_policy.clone();
        bytes.extend_from_slice(&extra);
        let p = Arc::new(
            policy::fold(&bytes, table.special)
                .map_err(|e| format!("XX001: \"{}\" is {}, and {e}", table.name, what(table)))?,
        );
        self.policies.write().insert(key, p.clone());
        Ok(p)
    }

    fn admit_drop(&self, table: &TableSchema, policy: &LedgerPolicy) -> Result<(), String> {
        let now = clock::now_micros();
        // The newest row any open chain has seen; unknown (no chain read since
        // start) counts as now, so a drop waits rather than slips through.
        let known = store::last_insert(policy.id);
        let last = known.unwrap_or(now);
        if policy.drop_idle_us == 0 || now >= last.saturating_add(policy.drop_idle_us) {
            return Ok(());
        }
        Err(format!(
            "55000: \"{}\" is {} and is not dropped until {} s pass with no row inserted (last insert {} s ago{})",
            table.name,
            what(table),
            policy.drop_idle_us / 1_000_000,
            (now - last).max(0) / 1_000_000,
            if known.is_none() { "; run SHOW LEDGER to read its chain" } else { "" }
        ))
    }
}

impl Default for LedgerGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl LedgerHooks for LedgerGuard {
    fn check_open(&self, table: &TableSchema) -> Result<(), String> {
        self.policy(table).map(drop)
    }

    fn admit(&self, table: &TableSchema, write: WriteKind<'_>) -> Result<(), String> {
        let policy = self.policy(table)?;
        let vault = policy.kind == SpecialTable::Vault;
        match write {
            // A vault takes rows only from its own statements (`VAULT … SET`),
            // never a client's INSERT: a forged version would be served.
            WriteKind::Insert if vault && !rasterizeddb_core::core::ledger_hooks::vault_write_active() => Err(format!(
                "0A000: \"{}\" is a secret vault: write it with VAULT {} SET SECRET …",
                table.name, table.name
            )),
            // A versioned table's history is its retired versions: nothing
            // that reclaims them runs on it.
            WriteKind::Maintenance if policy.versioned => Err(format!(
                "0A000: \"{}\" is a versioned ledger table: its row versions are its history and are never reclaimed",
                table.name
            )),
            WriteKind::Update if policy.versioned => Ok(()),
            WriteKind::Insert | WriteKind::RawVersion | WriteKind::Maintenance | WriteKind::Move => Ok(()),
            // Row by row, against retention and holds (`admit_row_delete`). A
            // vault's versions are never deleted: a purge shreds their key.
            WriteKind::Delete if !vault => Ok(()),
            WriteKind::Alter("ADD COLUMN NULL") if !vault => Ok(()),
            WriteKind::Drop => self.admit_drop(table, &policy),
            other => Err(format!(
                "0A000: \"{}\" is {}: {other} is not allowed on it{}",
                table.name,
                what(table),
                if vault { "; use the vault_* functions" } else { "; it is append-only" }
            )),
        }
    }

    fn admit_row_delete(&self, table: &TableSchema, inserted_at_micros: i64) -> Result<(), String> {
        let policy = self.policy(table)?;
        if let Some((hold, _)) = policy.holds.iter().next() {
            return Err(format!(
                "55000: \"{}\" is under legal hold \"{hold}\"; no row is deleted until it is released",
                table.name
            ));
        }
        if !clock::retention_passed(inserted_at_micros, policy.retention_us) {
            return Err(format!(
                "55000: a row of \"{}\" is still within its retention ({} s after insert)",
                table.name,
                policy.retention_us / 1_000_000
            ));
        }
        Ok(())
    }

    fn create_policy(&self, kind: SpecialTable, clauses: &str) -> Result<Vec<u8>, String> {
        let policy = clauses::parse(kind, clauses).map_err(|e| format!("42601: {e}"))?;
        // A vault's key provider is opened now, before the vault exists: a
        // missing PIN, key file directory or token otherwise surfaced only
        // at its first use, past a CREATE VAULT that had succeeded and
        // left a vault no key could ever be made in.
        // Not on a replica applying the origin's CREATE VAULT: it stores the
        // vault's ciphertext and needs no key to, and a key it lacks would
        // halt its whole stream.
        #[cfg(feature = "vault")]
        if let Some(uri) = policy.key_provider.as_deref()
            && !rasterizeddb_core::core::mvcc::owner::current()
                .is_some_and(|o| o.kind == rasterizeddb_core::core::mvcc::owner::OwnerKind::Replication)
        {
            crate::vault::check_provider(uri)?;
        }
        Ok(policy::record(&policy.creation_payload()))
    }

    fn now_micros(&self) -> i64 {
        clock::now_micros()
    }

    fn on_insert(&self, table: &TableSchema, tx: TxId, ts: i64, cells: &[(u32, &[u8])]) {
        let Ok(policy) = self.policy(table) else { return };
        let (width, leaf) = store::leaf(policy.id, ts, cells);
        if !policy.anchors.is_empty() && !self.anchors_of.read().contains_key(&policy.id) {
            self.anchors_of.write().insert(policy.id, policy.anchors.clone());
        }
        self.pending
            .lock()
            .entry(tx)
            .or_default()
            .push(Item { op: OP_INSERT, ledger: policy.id, row_ts: ts, width, leaf });
    }

    fn on_delete(&self, table: &TableSchema, tx: TxId, ts: i64) {
        let Ok(policy) = self.policy(table) else { return };
        self.pending
            .lock()
            .entry(tx)
            .or_default()
            .push(Item { op: OP_DELETE, ledger: policy.id, row_ts: ts, width: 0, leaf: [0; 32] });
    }

    fn admit_row_update(&self, table: &TableSchema, _ts: i64) -> Result<(), String> {
        let policy = self.policy(table)?;
        if !policy.versioned {
            return Err(format!("0A000: \"{}\" is {}: UPDATE is not allowed on it; it is append-only", table.name, what(table)));
        }
        Ok(())
    }

    fn on_update(&self, table: &TableSchema, tx: TxId, ts: i64, cells: &[(u32, &[u8])]) {
        let Ok(policy) = self.policy(table) else { return };
        let (width, leaf) = store::leaf(policy.id, ts, cells);
        self.pending.lock().entry(tx).or_default().push(Item { op: OP_UPDATE, ledger: policy.id, row_ts: ts, width, leaf });
    }

    fn keeps_versions(&self, table: &TableSchema) -> bool {
        self.policy(table).is_ok_and(|p| p.versioned)
    }

    fn has_pending(&self, ids: &[TxId]) -> bool {
        let pending = self.pending.lock();
        let audits = self.audits.lock();
        ids.iter().any(|id| pending.contains_key(id) || audits.contains_key(id))
    }

    fn discard(&self, tx: TxId) {
        self.pending.lock().remove(&tx);
        self.audits.lock().remove(&tx);
    }

    fn prepare_commit(&self, store_key: &str, members: &[(TxId, bool)]) -> Result<(), String> {
        let mut items = Vec::new();
        {
            let mut pending = self.pending.lock();
            for (id, survives) in members {
                if let Some(mut mine) = pending.remove(id) {
                    if *survives {
                        items.append(&mut mine);
                    }
                }
            }
        }
        // The vault changes of the members that survive: audited now, in
        // the same chain entry as their rows. A rolled-back savepoint's are
        // dropped with it.
        let mut audits = Vec::new();
        {
            let mut staged = self.audits.lock();
            for (id, survives) in members {
                if let Some(mine) = staged.remove(id) {
                    if *survives {
                        audits.extend(mine.into_iter().filter(|(s, _, _)| s == store_key));
                    }
                }
            }
        }
        if items.is_empty() && audits.is_empty() {
            return Ok(());
        }
        // Refused before anything is appended when the unanchored window is
        // used up and the policy is to stop (plan §3.4, Q2).
        {
            let anchors_of = self.anchors_of.read();
            let mut seen = std::collections::HashSet::new();
            for i in &items {
                if seen.insert(i.ledger) {
                    if let Some(sinks) = anchors_of.get(&i.ledger) {
                        anchor::add_sinks(store_key, sinks);
                    }
                }
            }
        }
        anchor::admit_writes(store_key)?;
        // Written before the chain entry that covers them, as `audit` does:
        // a line that cannot be written fails the commit.
        #[cfg(feature = "vault")]
        for (_, vault, line) in &audits {
            items.push(crate::vault::audit_line(store_key, *vault, line)?);
        }
        let top = members.iter().map(|(id, _)| *id).min().unwrap_or(0);
        let chain = store::chain(store_key)?;
        let hash = chain.lock().append(top, &items)?;
        anchor::note_append(store_key, &chain, hash);
        Ok(())
    }

    fn database_opened(&self, store_key: &str) {
        // Reading the chain raises the trusted clock past every stamp in it.
        if let Err(e) = store::reopen(store_key) {
            log::error!("ledger chain of {store_key}: {e}");
        }
    }

    fn statement(&self, store_key: &str, request: LedgerRequest<'_>) -> Result<LedgerReply, String> {
        statements::run(self, store_key, request)
    }
}

mod statements;

/// Installs the ledger guard and makes the ledger and vault directory flags
/// known. Called by the enterprise server and embedded builds before the
/// first catalogue opens, whether or not an enterprise configuration file
/// is given: a ledger table must never be opened unguarded.
pub fn install() -> Result<(), String> {
    use rasterizeddb_core::core::database::incompat::LEDGER_V1;
    // A vault directory is known only to a build that can serve vaults.
    #[cfg(feature = "vault")]
    let flags = [LEDGER_V1, rasterizeddb_core::core::database::incompat::VAULT_V1];
    #[cfg(not(feature = "vault"))]
    let flags = [LEDGER_V1];
    let guard: &'static LedgerGuard = GUARD.get_or_init(|| Box::leak(Box::new(LedgerGuard::new())));
    rasterizeddb_core::core::ledger_hooks::install(Box::new(guard), &flags)
}

/// The guard [`install`] put in place: the vault and the column cipher read
/// policies through it, so they see what an `ALTER` changed in this process.
static GUARD: std::sync::OnceLock<&'static LedgerGuard> = std::sync::OnceLock::new();

pub(crate) fn guard() -> Option<&'static LedgerGuard> {
    GUARD.get().copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ledger(retention_us: i64) -> TableSchema {
        let mut s = TableSchema::new("payments".into(), false);
        s.special = SpecialTable::Ledger;
        s.ledger_policy = policy::record(&LedgerPolicy::new(SpecialTable::Ledger, retention_us, 0).creation_payload());
        s
    }

    #[test]
    fn the_allowlist_admits_appends_and_refuses_changes() {
        let t = ledger(1_000_000);
        let g = LedgerGuard::new();
        for ok in [
            WriteKind::Insert,
            WriteKind::Maintenance,
            WriteKind::Move,
            WriteKind::Delete,
            WriteKind::Alter("ADD COLUMN NULL"),
            WriteKind::Drop,
        ] {
            assert!(g.admit(&t, ok).is_ok(), "{ok}");
        }
        for refused in [
            WriteKind::Update,
            WriteKind::InsertOnConflict,
            WriteKind::Truncate,
            WriteKind::Alter("DROP COLUMN"),
            WriteKind::Alter("ADD COLUMN"),
            WriteKind::Rewind,
            WriteKind::Other("anything new"),
        ] {
            assert!(g.admit(&t, refused).is_err(), "{refused}");
        }
    }

    #[test]
    fn a_row_inside_its_retention_or_a_hold_is_not_deleted() {
        let g = LedgerGuard::new();
        let now = clock::now_micros();
        let t = ledger(3_600_000_000);
        assert!(g.admit_row_delete(&t, now).unwrap_err().starts_with("55000"));
        assert!(g.admit_row_delete(&t, now - 7_200_000_000).is_ok());
        let mut held = t.clone();
        held.ledger_policy.extend(policy::record("v1\nhold audit set 1\n"));
        assert!(g.admit_row_delete(&held, now - 7_200_000_000).unwrap_err().contains("legal hold"));
    }

    #[test]
    fn a_table_with_no_policy_does_not_open() {
        let mut t = ledger(1);
        t.ledger_policy.clear();
        assert!(LedgerGuard::new().check_open(&t).unwrap_err().starts_with("XX001"));
    }

    #[test]
    fn committed_rows_chain_and_aborted_ones_do_not() {
        let g = LedgerGuard::new();
        let t = ledger(1);
        g.on_insert(&t, 10, 100, &[(0, b"a")]);
        g.on_insert(&t, 11, 101, &[(0, b"b")]);
        assert!(g.has_pending(&[10]));
        g.discard(11);
        g.prepare_commit("memory:unit", &[(10, true)]).unwrap();
        let chain = store::chain("memory:unit").unwrap();
        let read = chain.lock().reread().unwrap();
        assert_eq!(read.entries.len(), 1);
        assert_eq!(read.entries[0].items.len(), 1);
        assert!(!g.has_pending(&[10, 11]));
    }
}
