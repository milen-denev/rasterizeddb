//! The hooks the licensed ledger, encryption and vault features implement
//! (docs/ledger-and-vault-plan.md §2). This module is GPL and always built,
//! like [`super::policy_hooks`].
//!
//! **Fail closed.** A table whose header says it is a ledger table or a
//! vault ([`SpecialTable`]) is governed by a licensed policy. In a process
//! where nothing installed one, every write to such a table is refused, and
//! so is opening it ([`check_open`]). Without that rule, a GPL build handed a
//! ledger directory would let `UPDATE` and `DELETE` straight through. The
//! directory-level fence is the `ledger_v1`/`vault_v1` flag in
//! `incompat_features`; this module is the per-table fence behind it, and
//! it holds even when that file is removed.
//!
//! **Allowlist.** Each write entry point on a table names its
//! [`WriteKind`]. The installed policy admits what it knows and refuses the
//! rest, so a write kind added later is refused until the policy is taught
//! about it.
//!
//! Plain tables pay one branch on `schema.special` per write and nothing
//! else.

use std::sync::OnceLock;

use crate::core::row::schema::{SpecialTable, TableSchema};

/// What a write to a table is about to do. One variant per entry point
/// class; `Other` names the rest so a refusal can say what was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteKind<'a> {
    /// New rows (`INSERT`, `COPY`, bulk load).
    Insert,
    /// The conflict arm of `INSERT … ON CONFLICT`: writes or updates.
    InsertOnConflict,
    /// A row version written with given stamps (partition moves, conversion,
    /// replication of versions). It copies rows and their hidden columns
    /// as they are.
    RawVersion,
    /// `UPDATE`, in any of its forms.
    Update,
    /// `DELETE`. A ledger policy admits it table-wide only to check each
    /// row against retention ([`LedgerHooks::admit_row_delete`]).
    Delete,
    /// `TRUNCATE`.
    Truncate,
    /// `DROP TABLE`.
    Drop,
    /// `ALTER TABLE`: the clause, as the statement said it (`ADD COLUMN`,
    /// `DROP COLUMN`, `ALTER COLUMN TYPE`, `RENAME`, …).
    Alter(&'a str),
    /// Reclamation that leaves every visible row as it was (`VACUUM`,
    /// automatic compaction, `VACUUM FULL`).
    Maintenance,
    /// Moving the table's files (`MOVE TABLE`, rebalance).
    Move,
    /// Branch, restore or other whole-database rewrites (`RESET BRANCH`,
    /// `PROMOTE`, a point-in-time restore).
    Rewind,
    /// Anything else, by name.
    Other(&'static str),
}

impl std::fmt::Display for WriteKind<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteKind::Insert => f.write_str("INSERT"),
            WriteKind::InsertOnConflict => f.write_str("INSERT … ON CONFLICT"),
            WriteKind::RawVersion => f.write_str("a row version copy"),
            WriteKind::Update => f.write_str("UPDATE"),
            WriteKind::Delete => f.write_str("DELETE"),
            WriteKind::Truncate => f.write_str("TRUNCATE"),
            WriteKind::Drop => f.write_str("DROP TABLE"),
            WriteKind::Alter(what) => write!(f, "ALTER TABLE … {what}"),
            WriteKind::Maintenance => f.write_str("maintenance"),
            WriteKind::Move => f.write_str("moving the table"),
            WriteKind::Rewind => f.write_str("rewinding the database"),
            WriteKind::Other(what) => f.write_str(what),
        }
    }
}

/// The licensed ledger/vault policy. Installed once per process, before
/// the first catalogue opens.
pub trait LedgerHooks: Send + Sync + 'static {
    /// Whether `table` may be opened in this process: its sidecars are
    /// present and verify, and this build knows its kind. Called once per
    /// open, after the schema is read.
    fn check_open(&self, table: &TableSchema) -> Result<(), String>;

    /// Whether `write` may run against `table`. A refusal is a message
    /// starting with a SQLSTATE and `": "`: `55000` for a retention lock or
    /// a legal hold, `0A000` for a write a ledger table never takes,
    /// `42501` for a privilege.
    fn admit(&self, table: &TableSchema, write: WriteKind<'_>) -> Result<(), String>;

    /// Whether one row whose trusted insert time is `inserted_at_micros`
    /// (`__ledger_ts`) may be deleted now. Asked for each row a `DELETE`
    /// selected, before anything is stamped, so one locked row fails the
    /// whole statement.
    fn admit_row_delete(&self, table: &TableSchema, inserted_at_micros: i64) -> Result<(), String>;

    /// The policy sidecar's first record for a new table of `kind`, from
    /// the ledger clauses its `CREATE` statement wrote after the column list
    /// (`NO DELETE UNTIL …`, `NO DROP UNTIL …`, `HASHING USING …`,
    /// `ANCHOR TO …`, `RETAIN DELETED …`), as the statement wrote them. A
    /// clause it does not know is refused.
    fn create_policy(&self, kind: SpecialTable, clauses: &str) -> Result<Vec<u8>, String>;

    /// Trusted time now, in microseconds since the epoch: what a new row's
    /// [`LEDGER_TS`] is stamped with. Strictly later than any time it gave
    /// before, so it also names the row.
    fn now_micros(&self) -> i64;

    /// A row was written into `table` by `tx`: its stamp and its stored
    /// cells by write order, as the write put them. Kept pending until the
    /// transaction's family commits ([`Self::prepare_commit`]) or `tx` is
    /// discarded.
    fn on_insert(&self, table: &TableSchema, tx: TxId, ts: i64, cells: &[(u32, &[u8])]);

    /// The row stamped `ts` of `table` was deleted by `tx`, past its
    /// retention. Pending as [`Self::on_insert`] is.
    fn on_delete(&self, table: &TableSchema, tx: TxId, ts: i64);

    /// Whether the row stamped `ts` of `table` may be updated now (a
    /// versioned ledger table; never an append-only one).
    fn admit_row_update(&self, table: &TableSchema, ts: i64) -> Result<(), String>;

    /// The row stamped `ts` got a new version from `tx`, with these cells.
    /// Pending as [`Self::on_insert`] is.
    fn on_update(&self, table: &TableSchema, tx: TxId, ts: i64, cells: &[(u32, &[u8])]);

    /// Whether `table` keeps every row version (a versioned ledger table):
    /// its retired versions are then never reclaimed.
    fn keeps_versions(&self, table: &TableSchema) -> bool;

    /// The family `members` (each id with whether its writes survive) is
    /// about to commit in the database whose ledger lives under `store`.
    /// Its surviving rows' entries are chained and made durable here; an
    /// error means nothing commits and the caller aborts.
    fn prepare_commit(&self, store: &str, members: &[(TxId, bool)]) -> Result<(), String>;

    /// Forgets anything `tx` left pending (it aborted).
    fn discard(&self, tx: TxId);

    /// Whether any of `ids` has something pending: a commit of a family
    /// with nothing to chain skips [`Self::prepare_commit`].
    fn has_pending(&self, ids: &[TxId]) -> bool;

    /// A database holding a special table opened from `store`: its chain is
    /// read now, before the first stamp.
    fn database_opened(&self, _store: &str) {}

    /// A ledger statement (`VERIFY LEDGER`, `ALTER LEDGER TABLE`, `SHOW
    /// LEDGER`, `ACKNOWLEDGE LEDGER ROLLBACK`), with what core read for it.
    fn statement(&self, store: &str, request: LedgerRequest<'_>) -> Result<LedgerReply, String>;
}

/// One set of hooks installed here and kept by its crate too (a leaked
/// `&'static`), so the crate's other hooks share its state.
impl<T: LedgerHooks> LedgerHooks for &'static T {
    fn check_open(&self, table: &TableSchema) -> Result<(), String> {
        (**self).check_open(table)
    }
    fn admit(&self, table: &TableSchema, write: WriteKind<'_>) -> Result<(), String> {
        (**self).admit(table, write)
    }
    fn admit_row_delete(&self, table: &TableSchema, inserted_at_micros: i64) -> Result<(), String> {
        (**self).admit_row_delete(table, inserted_at_micros)
    }
    fn create_policy(&self, kind: SpecialTable, clauses: &str) -> Result<Vec<u8>, String> {
        (**self).create_policy(kind, clauses)
    }
    fn now_micros(&self) -> i64 {
        (**self).now_micros()
    }
    fn on_insert(&self, table: &TableSchema, tx: TxId, ts: i64, cells: &[(u32, &[u8])]) {
        (**self).on_insert(table, tx, ts, cells)
    }
    fn on_delete(&self, table: &TableSchema, tx: TxId, ts: i64) {
        (**self).on_delete(table, tx, ts)
    }
    fn admit_row_update(&self, table: &TableSchema, ts: i64) -> Result<(), String> {
        (**self).admit_row_update(table, ts)
    }
    fn on_update(&self, table: &TableSchema, tx: TxId, ts: i64, cells: &[(u32, &[u8])]) {
        (**self).on_update(table, tx, ts, cells)
    }
    fn keeps_versions(&self, table: &TableSchema) -> bool {
        (**self).keeps_versions(table)
    }
    fn prepare_commit(&self, store: &str, members: &[(TxId, bool)]) -> Result<(), String> {
        (**self).prepare_commit(store, members)
    }
    fn discard(&self, tx: TxId) {
        (**self).discard(tx)
    }
    fn has_pending(&self, ids: &[TxId]) -> bool {
        (**self).has_pending(ids)
    }
    fn database_opened(&self, store: &str) {
        (**self).database_opened(store)
    }
    fn statement(&self, store: &str, request: LedgerRequest<'_>) -> Result<LedgerReply, String> {
        (**self).statement(store, request)
    }
}

/// What a ledger statement hands the hooks: the statement, and the tables
/// and rows core read for it.
pub struct LedgerRequest<'a> {
    /// The statement as the client wrote it.
    pub sql: &'a str,
    /// Each special table it is about, with every row visible now: its
    /// stamp and its stored cells by write order (read for `VERIFY` only).
    pub tables: Vec<(&'a TableSchema, Vec<(i64, Vec<(u32, Vec<u8>)>)>)>,
    /// For `SHOW LEDGER HISTORY <table> <stamp>`: every version of that row
    /// still on disk, oldest first, `(xmin, xmax, stored cells)`.
    pub versions: Vec<(u64, u64, Vec<(u32, Vec<u8>)>)>,
    /// The role running it, and whether it is a superuser.
    pub role: &'a str,
    pub superuser: bool,
}

/// What a ledger statement returns.
pub struct LedgerReply {
    /// Lines of text, one row each.
    pub lines: Vec<String>,
    /// Policy records to append, by table name, before the reply is sent:
    /// `ALTER LEDGER TABLE`'s raised retention or placed hold.
    pub records: Vec<(String, Vec<u8>)>,
    /// `INSERT` statements to run in the statement's transaction before the
    /// reply is sent: a vault's new secret version, as ciphertext. Each joins
    /// the chain like any ledger row.
    pub inserts: Vec<String>,
}

use crate::core::mvcc::TxId;

tokio::task_local! {
    /// The role running the statement, and whether it is a superuser: set by
    /// the wire front end around each statement. A vault read is audited as
    /// this role, and the ledger's administrative statements are checked
    /// against it. Unset (a path that never says), the role is unknown, and
    /// those statements are refused.
    static SESSION: (String, bool);
}

tokio::task_local! {
    /// Set while core runs the inserts a vault statement returned
    /// ([`LedgerReply::inserts`]): the only inserts a vault takes. A plain
    /// `INSERT INTO <vault>` from a client never runs under it.
    static VAULT_WRITE: ();
}

/// Runs `f` as the vault's own write.
pub async fn as_vault_write<F: std::future::Future>(f: F) -> F::Output {
    VAULT_WRITE.scope((), f).await
}

/// Whether the running insert is a vault statement's own.
pub fn vault_write_active() -> bool {
    VAULT_WRITE.try_with(|_| ()).is_ok()
}

/// Runs `f` as `role`.
pub async fn with_session<F: std::future::Future>(role: String, superuser: bool, f: F) -> F::Output {
    SESSION.scope((role, superuser), f).await
}

/// The role running this statement, if the front end said.
pub fn session() -> Option<(String, bool)> {
    SESSION.try_with(|s| s.clone()).ok()
}

/// [`LedgerHooks::on_insert`], for a special table.
pub fn on_insert(table: &TableSchema, tx: TxId, ts: i64, cells: &[(u32, &[u8])]) {
    if let Some(h) = HOOKS.get() {
        h.on_insert(table, tx, ts, cells);
    }
}

/// [`LedgerHooks::on_delete`], for a special table.
pub fn on_delete(table: &TableSchema, tx: TxId, ts: i64) {
    if let Some(h) = HOOKS.get() {
        h.on_delete(table, tx, ts);
    }
}

/// [`LedgerHooks::admit_row_update`], refused without installed hooks.
pub fn admit_row_update(table: &TableSchema, ts: i64) -> Result<(), String> {
    match HOOKS.get() {
        Some(h) => h.admit_row_update(table, ts),
        None => Err(not_installed(table.special, &table.name)),
    }
}

/// [`LedgerHooks::on_update`], for a special table.
pub fn on_update(table: &TableSchema, tx: TxId, ts: i64, cells: &[(u32, &[u8])]) {
    if let Some(h) = HOOKS.get() {
        h.on_update(table, tx, ts, cells);
    }
}

/// [`LedgerHooks::keeps_versions`]; false for a plain table.
pub fn keeps_versions(table: &TableSchema) -> bool {
    table.special.is_special() && HOOKS.get().is_some_and(|h| h.keeps_versions(table))
}

/// [`LedgerHooks::prepare_commit`] when anything in `members` is pending;
/// nothing at all without installed hooks.
pub fn prepare_commit(store: &str, members: &[(TxId, bool)]) -> Result<(), String> {
    match HOOKS.get() {
        Some(h) => {
            let ids: Vec<TxId> = members.iter().map(|(id, _)| *id).collect();
            if h.has_pending(&ids) { h.prepare_commit(store, members) } else { Ok(()) }
        }
        None => Ok(()),
    }
}

/// [`LedgerHooks::database_opened`]; a no-op without installed hooks.
pub fn database_opened(store: &str) {
    if let Some(h) = HOOKS.get() {
        h.database_opened(store);
    }
}

/// [`LedgerHooks::discard`]; a no-op without installed hooks.
#[inline]
pub fn discard(tx: TxId) {
    if let Some(h) = HOOKS.get() {
        h.discard(tx);
    }
}

/// [`LedgerHooks::statement`], refused without installed hooks.
pub fn statement(store: &str, request: LedgerRequest<'_>) -> Result<LedgerReply, String> {
    match HOOKS.get() {
        Some(h) => h.statement(store, request),
        None => Err(
            "0A000: ledger statements need the enterprise build with feature `ledger`".to_string(),
        ),
    }
}

/// Whether `sql` is a ledger statement core routes to [`statement`]. Read
/// on every statement's way in (the native path asks it of each), so it
/// looks at the first words in place and allocates nothing.
pub fn is_ledger_statement(sql: &str) -> bool {
    let mut w = sql.split_ascii_whitespace();
    let is = |word: Option<&str>, want: &str| word.is_some_and(|x| x.trim_end_matches(';').eq_ignore_ascii_case(want));
    let first = w.next();
    if is(first, "VAULT") {
        return true;
    }
    let second = w.next();
    if is(first, "VERIFY") || is(first, "ACKNOWLEDGE") {
        return is(second, "LEDGER");
    }
    if is(first, "SHOW") {
        return is(second, "LEDGER") || is(second, "LEDGERS");
    }
    is(first, "ALTER") && is(second, "LEDGER") && is(w.next(), "TABLE")
}

/// The hidden column every ledger table carries: the row's trusted insert
/// time, microseconds since the epoch. The engine stamps it on every insert,
/// whatever the statement gave, and a `DELETE` reads it for retention.
pub const LEDGER_TS: &str = "__ledger_ts";

/// Trusted time now for a new row of a special table.
pub fn trusted_now(table: &TableSchema) -> Result<i64, String> {
    match HOOKS.get() {
        Some(h) => Ok(h.now_micros()),
        None => Err(not_installed(table.special, &table.name)),
    }
}

static HOOKS: OnceLock<Box<dyn LedgerHooks>> = OnceLock::new();

/// Installs the licensed policy and registers the on-disk flags it makes
/// known (`flags`, from [`crate::core::database::incompat`]'s licensed
/// set). Once per process, before the first catalogue opens.
pub fn install(hooks: Box<dyn LedgerHooks>, flags: &[&str]) -> Result<(), String> {
    for flag in flags {
        crate::core::database::incompat::register_licensed(flag)?;
    }
    HOOKS
        .set(hooks)
        .map_err(|_| "a ledger policy is already installed".to_string())
}

/// Whether a licensed ledger policy is installed.
pub fn installed() -> bool {
    HOOKS.get().is_some()
}

fn not_installed(kind: SpecialTable, table: &str) -> String {
    let what = match kind {
        SpecialTable::Vault => "a secret vault",
        _ => "a ledger table",
    };
    format!(
        "0A000: \"{table}\" is {what}, and this server has no ledger support installed \
         (the enterprise build with feature `ledger`); it refuses the table rather than \
         run it unguarded"
    )
}

/// Refuses to open a special table this process cannot guard. Plain tables
/// pass with no further work.
pub fn check_open(table: &TableSchema) -> Result<(), String> {
    if !table.special.is_special() {
        return Ok(());
    }
    match HOOKS.get() {
        Some(h) => h.check_open(table),
        None => Err(not_installed(table.special, &table.name)),
    }
}

/// Refuses `write` against `table` when its policy does. Plain tables pass
/// with one branch.
#[inline]
pub fn admit(table: &TableSchema, write: WriteKind<'_>) -> Result<(), String> {
    if !table.special.is_special() {
        return Ok(());
    }
    admit_special(table, write)
}

#[cold]
fn admit_special(table: &TableSchema, write: WriteKind<'_>) -> Result<(), String> {
    match HOOKS.get() {
        Some(h) => h.admit(table, write),
        None => Err(not_installed(table.special, &table.name)),
    }
}

/// [`admit`] as an `io::Error`, for the table layer.
#[inline]
pub fn admit_io(table: &TableSchema, write: WriteKind<'_>) -> std::io::Result<()> {
    admit(table, write).map_err(std::io::Error::other)
}

/// The first policy record for a new special table (see
/// [`LedgerHooks::create_policy`]); refused when no ledger support is
/// installed, so a GPL build never creates a table it could not guard.
pub fn create_policy(kind: SpecialTable, table: &str, clauses: &str) -> Result<Vec<u8>, String> {
    match HOOKS.get() {
        Some(h) => h.create_policy(kind, clauses),
        None => Err(not_installed(kind, table)),
    }
}

/// Refuses deleting one row of `table` while its retention or a hold
/// stands.
pub fn admit_row_delete(table: &TableSchema, inserted_at_micros: i64) -> Result<(), String> {
    if !table.special.is_special() {
        return Ok(());
    }
    match HOOKS.get() {
        Some(h) => h.admit_row_delete(table, inserted_at_micros),
        None => Err(not_installed(table.special, &table.name)),
    }
}

/// The sidecar a special table's policy records live in, beside its schema
/// (`<table>_schema_LEDGER.db`). Core only stores and loads the bytes; the
/// installed hooks define what the records say. Records are appended, never
/// rewritten, so raising a retention or placing a hold leaves the earlier
/// records in place.
pub mod policy_sidecar {
    use rclite::Arc;

    use crate::core::storage_providers::traits::StorageIO;

    pub const SUFFIX: &str = "LEDGER.db";

    /// Every byte of the table's policy sidecar; empty when it has none.
    pub async fn load<S: StorageIO>(schema_io: &Arc<S>) -> std::io::Result<Vec<u8>> {
        let io = schema_io
            .create_new(crate::core::torn_tail::sidecar_name(&**schema_io, SUFFIX))
            .await;
        let len = io.get_len().await;
        let mut bytes = vec![0u8; len as usize];
        if len > 0 {
            io.read_data_into_buffer(&mut 0, &mut bytes).await?;
        }
        Ok(bytes)
    }

    /// Adds `record` to the statement's sidecar writes, appended to the
    /// table's policy sidecar when they commit.
    pub fn stage<S: StorageIO>(schema_io: &S, writes: &mut crate::core::torn_tail::SidecarWrites, record: &[u8]) {
        writes.add(crate::core::torn_tail::sidecar_name(schema_io, SUFFIX), record);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn special(kind: SpecialTable) -> TableSchema {
        let mut s = TableSchema::new("t".into(), false);
        s.special = kind;
        s
    }

    #[test]
    fn ledger_statements_are_recognised_in_place() {
        for yes in ["VERIFY LEDGER", "verify ledger pay", "SHOW LEDGER;", "SHOW LEDGERS", "ALTER LEDGER TABLE t SET RETENTION 1 DAY",
            "ACKNOWLEDGE LEDGER ROLLBACK 'x'", "VAULT v GET SECRET 'a'", "  vault v LIST"] {
            assert!(is_ledger_statement(yes), "{yes}");
        }
        for no in ["SELECT 1", "SHOW TABLES", "ALTER TABLE t ADD COLUMN x INT", "VERIFY", "VAULTED", "ALTER LEDGER t", ""] {
            assert!(!is_ledger_statement(no), "{no}");
        }
    }

    #[test]
    fn plain_tables_pass_every_write() {
        let s = special(SpecialTable::None);
        for w in [WriteKind::Update, WriteKind::Delete, WriteKind::Truncate, WriteKind::Drop] {
            assert!(admit(&s, w).is_ok());
        }
        assert!(check_open(&s).is_ok());
    }

    #[test]
    fn a_special_table_without_hooks_refuses_everything() {
        // No test installs hooks in this process, so this is the GPL build's view.
        if installed() {
            return;
        }
        for kind in [SpecialTable::Ledger, SpecialTable::Vault] {
            let s = special(kind);
            assert!(check_open(&s).unwrap_err().starts_with("0A000"));
            for w in [WriteKind::Insert, WriteKind::Update, WriteKind::Delete, WriteKind::Maintenance] {
                assert!(admit(&s, w).is_err(), "{w} on {kind:?}");
            }
            assert!(admit_row_delete(&s, 0).is_err());
        }
    }

    #[test]
    fn the_header_byte_round_trips_and_refuses_unknown_kinds() {
        for (kind, imm) in [
            (SpecialTable::None, false),
            (SpecialTable::None, true),
            (SpecialTable::Ledger, false),
            (SpecialTable::Vault, false),
        ] {
            let b = kind.header_byte(imm);
            assert_eq!(SpecialTable::from_header_byte(b).unwrap(), (imm, kind));
        }
        assert!(SpecialTable::from_header_byte(2).is_err());
        assert!(SpecialTable::from_header_byte(b'X').is_err());
    }
}
