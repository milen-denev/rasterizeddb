//! Hooks for encrypted columns (docs/ledger-and-vault-plan.md §4.7). The
//! cipher, the key handling and the statement analysis are licensed and
//! live in `rasterizeddb_enterprise`; this module is where the engine calls
//! them, and what it does when nothing is installed.
//!
//! A column declared `ENCRYPTED WITH VAULT v KEY 'k'` holds AES-256-GCM
//! ciphertext as text (`rdbenc1:<column id>:<base64>`), under a data key of
//! its own wrapped by the vault's key. What the engine guarantees, through
//! these hooks:
//!
//! - **No answer computed over ciphertext.** Every statement that names a
//!   table with an encrypted column is shown to [`ColumnCrypto::guard`]
//!   (the client's text, before any rewrite) and to [`ColumnCrypto::plan`]
//!   (the text the executor runs). An encrypted column may appear only whole:
//!   as a select-list or `RETURNING` item, in an `INSERT` column list, or as
//!   `SET column = <literal>`. A filter, sort, group, join, index, function,
//!   `DISTINCT` or set operation over one is refused (0A000), never run.
//! - **Values are encrypted before they are written**: the plan replaces each
//!   literal bound for an encrypted column with its ciphertext.
//! - **Values are decrypted only on the way out**, at the outermost
//!   statement of a task, for a role holding `SELECT` on the column's vault
//!   (the grant `VAULT … DECRYPT` needs). Any other role reads the
//!   ciphertext, which copies and restores like any text.
//! - **Learned and native fast paths stay off** for such statements
//!   ([`touches`]): only the text path is guarded.
//!
//! Without the hooks a statement declaring an encrypted column is refused,
//! and a directory holding one carries `incompat::ENCRYPTED_VALUES_V1`, which
//! a build that did not install them refuses to open.

use std::sync::OnceLock;

use crate::core::database::Database;

/// What the licensed analysis decided for one statement.
#[derive(Debug, Default)]
pub struct Plan {
    /// The text to run instead (literals encrypted, `ENCRYPTED WITH …`
    /// removed from a `CREATE TABLE`); `None` runs the statement as it is.
    pub sql: Option<String>,
    /// Vaults whose key the statement uses to encrypt: the session's role
    /// needs `SELECT` on each.
    pub vaults: Vec<String>,
    /// The statement declares an encrypted column: the directory is fenced
    /// (`ENCRYPTED_VALUES_V1`) before it runs.
    pub declares: bool,
    /// Handed back to [`ColumnCrypto::finish`] once the statement ran.
    pub token: Option<u64>,
}

pub trait ColumnCrypto: Send + Sync + 'static {
    /// Whether `store` has any encrypted column: nothing else is asked of a
    /// store without one, so a database that never declared one pays a
    /// lookup per statement.
    fn active(&self, store: &str) -> bool;
    /// Whether `sql` names a table with an encrypted column (or declares
    /// one): such a statement takes the text path, guarded.
    fn touches(&self, store: &str, sql: &str) -> bool;
    /// The client's text, before any rewrite: `Err` (with its SQLSTATE) when
    /// an encrypted column appears where its ciphertext would be computed on.
    /// The database is there for a table's column order (`INSERT … VALUES`
    /// without a column list).
    fn guard(&self, database: &Database, sql: &str) -> Result<(), String>;
    /// The text the executor is about to run: refused, run as it is, or run
    /// as the plan says.
    fn plan(&self, database: &Database, sql: &str) -> Result<Option<Plan>, String>;
    /// The planned statement ran (`ok`) or failed: a declaration is kept or
    /// dropped, a dropped table's columns forgotten.
    fn finish(&self, store: &str, token: u64, ok: bool);
    /// The vault of a stored ciphertext cell, `None` for any other value.
    fn vault_of(&self, store: &str, cell: &[u8]) -> Option<String>;
    /// The plaintext of a stored ciphertext cell (the database to unwrap
    /// its column's key with, through the vault).
    fn decrypt(&self, database: &Database, cell: &[u8]) -> Result<Vec<u8>, String>;
    /// `SHOW LEDGER`'s lines about encrypted columns.
    fn show_lines(&self, store: &str) -> Vec<String>;
    /// `table` was dropped by the rollback of the block that created it
    /// (`Database::drop_table_for_rollback`, also after a crash): its
    /// columns are forgotten, as a `DROP TABLE` statement's are.
    fn forget_table(&self, _store: &str, _table: &str) {}
    /// Block DDL renamed `from` to `to`: a dropped table set aside under a
    /// hidden name for a new one of its name, or renamed back by a
    /// rollback (`block_ddl::set_aside`, `rename_back`). Its columns follow
    /// it, so the new table neither inherits nor clobbers them.
    fn rename_table(&self, _store: &str, _from: &str, _to: &str) {}
    /// Whether `sql` names a table with an encrypted column in any store
    /// this process has read: its literals are then the plaintext the
    /// client sent, and are blanked wherever the statement's text is kept
    /// (`redact::statement`: the log, `SHOW QUERIES`). Cheap when no store
    /// has one.
    fn names_any(&self, _sql: &str) -> bool {
        false
    }
}

static HOOKS: OnceLock<Box<dyn ColumnCrypto>> = OnceLock::new();

/// Installs the licensed cipher and makes `ENCRYPTED_VALUES_V1` known.
pub fn install(hooks: Box<dyn ColumnCrypto>) -> Result<(), String> {
    crate::core::database::incompat::register_licensed(crate::core::database::incompat::ENCRYPTED_VALUES_V1)?;
    HOOKS.set(hooks).map_err(|_| "the column cipher is already installed".to_string())
}

pub fn installed() -> bool {
    HOOKS.get().is_some()
}

fn hooks() -> Option<&'static dyn ColumnCrypto> {
    HOOKS.get().map(|h| h.as_ref())
}

/// Whether the text could declare an encrypted column: what a build
/// without the hooks refuses.
fn declares(sql: &str) -> bool {
    let up = sql.to_ascii_uppercase();
    up.contains("ENCRYPTED") && up.contains("VAULT")
}

/// See [`ColumnCrypto::touches`]; false without the hooks.
pub fn touches(database: &Database, sql: &str) -> bool {
    match hooks() {
        Some(h) => {
            let store = database.ledger_store();
            h.active(&store) && h.touches(&store, sql) || declares(sql)
        }
        None => false,
    }
}

/// See [`ColumnCrypto::guard`]. Without the hooks only a declaration is
/// refused: no encrypted column can exist.
pub fn guard(database: &Database, sql: &str) -> Result<(), String> {
    match hooks() {
        Some(h) => {
            let store = database.ledger_store();
            if h.active(&store) { h.guard(database, sql) } else { Ok(()) }
        }
        None if declares(sql) && sql.trim_start().get(..6).is_some_and(|w| w.eq_ignore_ascii_case("CREATE")) => {
            Err("0A000: encrypted columns need the enterprise build (rasterizeddb_core_enterprise)".to_string())
        }
        None => Ok(()),
    }
}

/// See [`ColumnCrypto::plan`].
pub fn plan(database: &Database, sql: &str) -> Result<Option<Plan>, String> {
    match hooks() {
        Some(h) => {
            let store = database.ledger_store();
            if h.active(&store) || declares(sql) { h.plan(database, sql) } else { Ok(None) }
        }
        None => guard(database, sql).map(|()| None),
    }
}

pub fn finish(database: &Database, token: u64, ok: bool) {
    if let Some(h) = hooks() {
        h.finish(&database.ledger_store(), token, ok);
    }
}

/// See [`ColumnCrypto::names_any`]; false without the hooks.
pub fn names_any(sql: &str) -> bool {
    hooks().is_some_and(|h| h.names_any(sql))
}

/// See [`ColumnCrypto::rename_table`].
pub fn rename_table(database: &Database, from: &str, to: &str) {
    if let Some(h) = hooks() {
        h.rename_table(&database.ledger_store(), from, to);
    }
}

/// See [`ColumnCrypto::forget_table`].
pub fn forget_table(database: &Database, table: &str) {
    if let Some(h) = hooks() {
        h.forget_table(&database.ledger_store(), table);
    }
}

pub fn active(database: &Database) -> bool {
    hooks().is_some_and(|h| h.active(&database.ledger_store()))
}

pub fn show_lines(store: &str) -> Vec<String> {
    hooks().map(|h| h.show_lines(store)).unwrap_or_default()
}

/// The prefix every stored ciphertext cell starts with.
pub const MARK: &[u8] = b"rdbenc1:";

/// Each text cell of a rows response (`[2] row*`) that holds ciphertext,
/// passed to `each`; what it returns replaces the cell. Anything that is not
/// a rows response, or does not parse as one, is returned as it was.
pub fn map_cipher_cells(response: Vec<u8>, mut each: impl FnMut(&[u8]) -> Option<Vec<u8>>) -> Vec<u8> {
    #[cfg(feature = "enable_long_row")]
    const LEN_HEADER: usize = 8;
    #[cfg(not(feature = "enable_long_row"))]
    const LEN_HEADER: usize = 4;
    let string = crate::core::db_type::DbType::STRING.to_byte();
    if response.first() != Some(&2) || !response.windows(MARK.len()).any(|w| w == MARK) {
        return response;
    }
    let mut out = Vec::with_capacity(response.len());
    out.push(2u8);
    let mut at = 1usize;
    let bytes = &response;
    let read = |at: usize, n: usize| bytes.get(at..at + n);
    while at < bytes.len() {
        let Some(h) = read(at, LEN_HEADER) else { return response };
        let mut len_bytes = [0u8; 8];
        len_bytes[..LEN_HEADER].copy_from_slice(h);
        let payload = u64::from_le_bytes(len_bytes) as usize;
        let Some(row) = read(at + LEN_HEADER, payload) else { return response };
        // The row again, with any changed cell.
        let mut cols: Vec<(u64, Vec<u8>, u8)> = Vec::new();
        let mut c = 0usize;
        while c < row.len() {
            let (Some(size), Some(id)) = (row.get(c..c + 8), row.get(c + 8..c + 16)) else { return response };
            let size = u64::from_le_bytes(size.try_into().unwrap()) as usize;
            let id = u64::from_le_bytes(id.try_into().unwrap());
            let (Some(data), Some(&tag)) = (row.get(c + 16..c + 16 + size), row.get(c + 16 + size)) else { return response };
            let data = if tag == string && data.starts_with(MARK) { each(data).unwrap_or_else(|| data.to_vec()) } else { data.to_vec() };
            cols.push((id, data, tag));
            c += 16 + size + 1;
        }
        let new_payload: usize = cols.iter().map(|(_, d, _)| 17 + d.len()).sum();
        #[cfg(feature = "enable_long_row")]
        out.extend_from_slice(&(new_payload as u64).to_le_bytes());
        #[cfg(not(feature = "enable_long_row"))]
        out.extend_from_slice(&(new_payload as u32).to_le_bytes());
        for (id, data, tag) in cols {
            out.extend_from_slice(&(data.len() as u64).to_le_bytes());
            out.extend_from_slice(&id.to_le_bytes());
            out.extend_from_slice(&data);
            out.push(tag);
        }
        at += LEN_HEADER + payload;
    }
    out
}

/// The vaults whose ciphertext a rows response holds.
pub fn vaults_in(database: &Database, response: &[u8]) -> Vec<String> {
    let Some(h) = hooks() else { return Vec::new() };
    let store = database.ledger_store();
    let mut found = Vec::new();
    let _ = map_cipher_cells(response.to_vec(), |cell| {
        if let Some(v) = h.vault_of(&store, cell) {
            if !found.contains(&v) {
                found.push(v);
            }
        }
        None
    });
    found
}

/// The response with each ciphertext cell of a vault in `allowed` replaced by
/// its plaintext. A cell that does not decrypt stays as it was (a value that
/// merely starts like ciphertext); one that names a known column but fails
/// its tag fails the statement, as damage.
pub fn decrypt_response(database: &Database, response: Vec<u8>, allowed: &[String]) -> Result<Vec<u8>, String> {
    let Some(h) = hooks() else { return Ok(response) };
    let store = database.ledger_store();
    let mut failure = None;
    let out = map_cipher_cells(response, |cell| {
        let vault = h.vault_of(&store, cell)?;
        if !allowed.contains(&vault) {
            return None;
        }
        match h.decrypt(database, cell) {
            Ok(plain) => Some(plain),
            Err(e) => {
                failure.get_or_insert(e);
                None
            }
        }
    });
    match failure {
        Some(e) => Err(e),
        None => Ok(out),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::db_type::DbType;
    use crate::core::row::row::{Column, Row};

    fn row(cells: &[(&[u8], DbType)]) -> Row {
        let mut r = Row::default();
        for (i, (b, t)) in cells.iter().enumerate() {
            let mut data = crate::memory_pool::MEMORY_POOL.acquire(b.len());
            if !b.is_empty() {
                data.into_slice_mut().copy_from_slice(b);
            }
            r.columns.push(Column { schema_id: crate::core::row::row::schema_id_of(i as u64), data, column_type: t.clone() });
        }
        r
    }

    #[test]
    fn only_text_cells_that_look_like_ciphertext_are_handed_over() {
        let mut resp = vec![2u8];
        row(&[(b"rdbenc1:abc", DbType::STRING), (b"rdbenc1:not-text", DbType::BYTEA), (b"plain", DbType::STRING)]).extend_serialized(&mut resp);
        row(&[(b"", DbType::NULL), (b"rdbenc1:x", DbType::STRING)]).extend_serialized(&mut resp);
        let mut seen = Vec::new();
        let out = map_cipher_cells(resp.clone(), |c| {
            seen.push(c.to_vec());
            Some(b"secret value".to_vec())
        });
        assert_eq!(seen, vec![b"rdbenc1:abc".to_vec(), b"rdbenc1:x".to_vec()]);
        let rows = crate::core::row::row::vec_into_rows(&out[1..]).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].columns[0].data.into_slice(), b"secret value");
        assert_eq!(rows[0].columns[1].data.into_slice(), b"rdbenc1:not-text");
        assert_eq!(rows[1].columns[1].data.into_slice(), b"secret value");
        // Nothing to change: the same bytes.
        assert_eq!(map_cipher_cells(resp.clone(), |_| None), resp);
        let error = vec![3u8, b'x'];
        assert_eq!(map_cipher_cells(error.clone(), |_| Some(vec![])), error);
    }
}
