//! What an index is: its name, method, uniqueness and columns, and the sidecar
//! that records the declarations a table has.
//!
//! One fixed-size record per declaration, appended to `<table>_schema_IDX.db`
//! and replayed last-wins by name with tombstones, the same shape as
//! [`crate::core::rql::path_index`]. Appending rather than rewriting means a
//! declaration that fails part-way leaves the previous state readable.
//!
//! Only the *declaration* is stored. The index itself lives in memory and is
//! rebuilt from the table when the table opens. After a crash it cannot
//! disagree with the data, because it is recomputed from the data the WAL
//! already made durable.

/// How an index stores its keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    /// Ordered: answers `=`, `IN`, `<`, `<=`, `>`, `>=`, `BETWEEN`, and a
    /// leading-column prefix of a composite key.
    BTree,
    /// Unordered: answers `=` and `IN` on every column of the key, in one
    /// probe.
    Hash,
}

impl Method {
    pub fn name(self) -> &'static str {
        match self {
            Method::BTree => "btree",
            Method::Hash => "hash",
        }
    }

    pub fn parse(text: &str) -> Option<Method> {
        match text.trim().to_ascii_lowercase().as_str() {
            "btree" | "b-tree" => Some(Method::BTree),
            "hash" => Some(Method::Hash),
            _ => None,
        }
    }

    fn code(self) -> u8 {
        match self {
            Method::BTree => 1,
            Method::Hash => 2,
        }
    }

    fn from_code(code: u8) -> Option<Method> {
        match code {
            1 => Some(Method::BTree),
            2 => Some(Method::Hash),
            _ => None,
        }
    }
}

/// Longest an index name may be, in bytes.
pub const NAME_LEN: usize = 128;
/// Most columns one index may cover.
pub const MAX_COLUMNS: usize = 16;
/// name + NUL, method, unique, column count, the columns, the deleted flag.
pub const DECL_SIZE: u64 = (NAME_LEN + 1 + 1 + 1 + 1 + MAX_COLUMNS * 4 + 1) as u64;

/// A declared index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexDecl {
    pub name: String,
    pub method: Method,
    pub unique: bool,
    /// The columns, by `write_order`, in key order. A column keeps its write
    /// order through a rename, so a renamed column keeps its index.
    pub columns: Vec<u32>,
    pub is_deleted: bool,
}

impl IndexDecl {
    pub fn into_vec(&self) -> Vec<u8> {
        let mut out = vec![0u8; DECL_SIZE as usize];
        let name = self.name.as_bytes();
        let n = name.len().min(NAME_LEN);
        out[..n].copy_from_slice(&name[..n]);
        let mut at = NAME_LEN + 1;
        out[at] = self.method.code();
        at += 1;
        out[at] = u8::from(self.unique);
        at += 1;
        out[at] = self.columns.len().min(MAX_COLUMNS) as u8;
        at += 1;
        for (i, wo) in self.columns.iter().take(MAX_COLUMNS).enumerate() {
            out[at + i * 4..at + i * 4 + 4].copy_from_slice(&wo.to_le_bytes());
        }
        at += MAX_COLUMNS * 4;
        out[at] = u8::from(self.is_deleted);
        out
    }

    pub fn from_vec(raw: &[u8]) -> Result<Self, String> {
        if raw.len() != DECL_SIZE as usize {
            return Err(format!("index record is {} bytes, expected {}", raw.len(), DECL_SIZE));
        }
        let end = raw[..NAME_LEN + 1].iter().position(|&b| b == 0).unwrap_or(NAME_LEN);
        let name = std::str::from_utf8(&raw[..end])
            .map_err(|_| "index name is not UTF-8".to_string())?
            .to_string();
        let mut at = NAME_LEN + 1;
        let method = Method::from_code(raw[at]).ok_or_else(|| format!("unknown index method code {}", raw[at]))?;
        at += 1;
        let unique = raw[at] != 0;
        at += 1;
        let count = raw[at] as usize;
        at += 1;
        if count == 0 || count > MAX_COLUMNS {
            return Err(format!("index {name} records {count} columns"));
        }
        let columns = (0..count)
            .map(|i| u32::from_le_bytes(raw[at + i * 4..at + i * 4 + 4].try_into().expect("4 bytes")))
            .collect();
        at += MAX_COLUMNS * 4;
        Ok(IndexDecl {
            name,
            method,
            unique,
            columns,
            is_deleted: raw[at] != 0,
        })
    }
}

pub const SIDECAR_SUFFIX: &str = "IDX.db";

fn sidecar_name<S: crate::core::storage_providers::traits::StorageIO>(schema_io: &rclite::Arc<S>) -> String {
    format!("{}_{}", schema_io.get_name().replace(".db", ""), SIDECAR_SUFFIX)
}

/// The indexes a table declares.
pub async fn load<S: crate::core::storage_providers::traits::StorageIO>(schema_io: rclite::Arc<S>) -> Vec<IndexDecl> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    let len = io.get_len().await;
    let mut out: Vec<IndexDecl> = Vec::new();
    let mut buffer = vec![0u8; DECL_SIZE as usize];
    for i in 0..(len / DECL_SIZE) {
        let mut position = i * DECL_SIZE;
        if io.read_data_into_buffer(&mut position, &mut buffer).await.is_err() {
            break;
        }
        match IndexDecl::from_vec(&buffer) {
            Ok(decl) => {
                // Last record for a name wins, so a drop and a re-create leave
                // one declaration rather than two disagreeing ones.
                out.retain(|d| !d.name.eq_ignore_ascii_case(&decl.name));
                if !decl.is_deleted {
                    out.push(decl);
                }
            }
            Err(e) => log::error!("Skipping unreadable index record: {}", e),
        }
    }
    out
}

/// Records a declaration, or a tombstone for one.
pub async fn append<S: crate::core::storage_providers::traits::StorageIO>(
    schema_io: rclite::Arc<S>,
    decl: &IndexDecl,
) -> std::io::Result<()> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    io.append_data(&decl.into_vec(), true).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_declaration_and_a_tombstone_round_trip() {
        for decl in [
            IndexDecl {
                name: "orders_customer_created".into(),
                method: Method::BTree,
                unique: false,
                columns: vec![3, 7],
                is_deleted: false,
            },
            IndexDecl {
                name: "sessions_token".into(),
                method: Method::Hash,
                unique: true,
                columns: vec![1],
                is_deleted: true,
            },
        ] {
            let raw = decl.into_vec();
            assert_eq!(raw.len(), DECL_SIZE as usize);
            assert_eq!(IndexDecl::from_vec(&raw).unwrap(), decl);
        }
    }
}
