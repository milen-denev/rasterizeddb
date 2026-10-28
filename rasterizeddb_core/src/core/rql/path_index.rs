//! Indexes declared over a path into a document.
//!
//! `CREATE INDEX ix ON t ((doc->>'status'))` records that a path is worth
//! indexing. It is deliberately **not** automatic: a document keyed by data has
//! unboundedly many paths, so indexing every path a document happens to contain
//! would rebuild, in the index, the same unbounded growth that made shredding
//! the documents unusable in the first place. An operator naming the one path a
//! query filters on is bounded by the queries, not by the data.
//!
//! # Only `->>`
//!
//! An indexed path must be written with `->>`, and that restriction is what
//! makes the index sound rather than merely simpler.
//!
//! JSON is untyped: one row's `doc->'n'` is a number, another's is a string.
//! The filter index fingerprints a value under a *pinned* column type, and a
//! value that does not match the pinned type has no fingerprint — it would be
//! written as `CODE_EMPTY`, which does not mean "unknown", it means "matches
//! nothing". A row whose value could not be typed would then be pruned from a
//! query it genuinely matches: a false negative, returning a short answer with
//! nothing reporting it.
//!
//! `->>` yields text for every value it can reach, so the pinned type is always
//! `STRING` and every row has a fingerprint. The hazard cannot arise rather
//! than being handled. `->` stays unindexed and its predicates scan, which is
//! correct and merely slower.
//!
//! # Storage
//!
//! One fixed-size record per declaration, appended to a sidecar beside the
//! schema, replayed last-wins with tombstones — the same shape as
//! [`super::json_map`] and the constraint sidecars. Appending rather than
//! rewriting means a declaration that fails part-way leaves the previous state
//! readable.

use crate::core::jsonb::PathExpr;

/// Longest an index name or a path expression may be, in bytes.
const FIELD_LEN: usize = 128;
/// name, then the path expression, then the deleted flag.
pub const PATH_INDEX_SIZE: u64 = (FIELD_LEN as u64 + 1) * 2 + 1;

/// A declared index over a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathIndex {
    /// The name the operator gave it, which `DROP INDEX` names.
    pub name: String,
    /// The path, canonically spelled — [`PathExpr::index_key`].
    pub path: String,
    pub is_deleted: bool,
}

impl PathIndex {
    pub fn into_vec(&self) -> Vec<u8> {
        let mut out = vec![0u8; PATH_INDEX_SIZE as usize];
        let write = |out: &mut [u8], at: usize, text: &str| {
            let bytes = text.as_bytes();
            let len = bytes.len().min(FIELD_LEN);
            out[at..at + len].copy_from_slice(&bytes[..len]);
        };
        write(&mut out, 0, &self.name);
        write(&mut out, FIELD_LEN + 1, &self.path);
        out[(FIELD_LEN + 1) * 2] = u8::from(self.is_deleted);
        out
    }

    pub fn from_vec(raw: &[u8]) -> Result<Self, String> {
        if raw.len() != PATH_INDEX_SIZE as usize {
            return Err(format!(
                "invalid path index record: expected {} bytes, got {}",
                PATH_INDEX_SIZE,
                raw.len()
            ));
        }
        let read = |at: usize| -> Result<String, String> {
            let end = raw[at..at + FIELD_LEN + 1]
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(FIELD_LEN);
            std::str::from_utf8(&raw[at..at + end])
                .map(|s| s.to_string())
                .map_err(|e| format!("a path index record is not UTF-8: {}", e))
        };
        let path = read(FIELD_LEN + 1)?;
        // A record written before `#>` steps were keyed as `#>'{1}'` could not
        // be read back, so its index was never built and no query matched it.
        let path = crate::core::jsonb::upgrade_legacy_index_key(&path).unwrap_or(path);
        Ok(PathIndex {
            name: read(0)?,
            path,
            is_deleted: raw[(FIELD_LEN + 1) * 2] != 0,
        })
    }
}

fn sidecar_name<S: crate::core::storage_providers::traits::StorageIO>(
    schema_io: &rclite::Arc<S>,
) -> String {
    format!("{}_{}", schema_io.get_name().replace(".db", ""), "PIX.db")
}

/// Reads a table's declared path indexes.
pub async fn load<S: crate::core::storage_providers::traits::StorageIO>(
    schema_io: rclite::Arc<S>,
) -> Vec<PathIndex> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    let len = io.get_len().await;

    let mut out: Vec<PathIndex> = Vec::new();
    if len < PATH_INDEX_SIZE {
        return out;
    }

    let mut buffer = vec![0u8; PATH_INDEX_SIZE as usize];
    for i in 0..(len / PATH_INDEX_SIZE) {
        let mut position = i * PATH_INDEX_SIZE;
        if io
            .read_data_into_buffer(&mut position, &mut buffer)
            .await
            .is_err()
        {
            break;
        }
        // A record of zeros is an append a power cut kept the length of and
        // dropped (`torn_tail::is_unwritten`), never a declaration.
        if crate::core::torn_tail::is_unwritten(&buffer) {
            continue;
        }
        match PathIndex::from_vec(&buffer) {
            Ok(entry) => {
                // Last record for a name wins, so a drop and a re-create leave
                // one entry rather than two disagreeing ones.
                out.retain(|e| !e.name.eq_ignore_ascii_case(&entry.name));
                if !entry.is_deleted {
                    out.push(entry);
                }
            }
            Err(e) => log::error!("Skipping unreadable path index record: {}", e),
        }
    }

    out
}

/// Records a declaration, or a tombstone for one.
pub async fn append<S: crate::core::storage_providers::traits::StorageIO>(
    schema_io: rclite::Arc<S>,
    entry: &PathIndex,
) -> std::io::Result<()> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    // Write-ahead, never a logged append (`torn_tail::append_records`).
    crate::core::torn_tail::append_records(&io, &entry.into_vec()).await
}

/// Whether a path may be indexed, and why not when it may not.
///
/// The `->>` rule is the load-bearing one — see the module note.
pub fn indexable(path: &PathExpr) -> Result<(), String> {
    if !path.as_text {
        return Err(
            "an index over a document path has to be written with ->> rather than -> . A path \
             read with -> yields a document, whose type differs from row to row, and a value the \
             index cannot type would be recorded as one that matches nothing — so rows that do \
             match would be left out of the answer. ->> reads every value as text, which every \
             row has."
                .to_string(),
        );
    }
    if path.steps.is_empty() {
        return Err("an index over a document path has to name a path into it".to_string());
    }
    if path.index_key().len() > FIELD_LEN {
        return Err(format!(
            "the path is longer than the {} bytes an index record holds",
            FIELD_LEN
        ));
    }
    Ok(())
}

/// The declaration matching `path`, if the table has one.
pub fn index_for<'a>(path: &PathExpr, declared: &'a [PathIndex]) -> Option<&'a PathIndex> {
    let key = path.index_key();
    declared.iter().find(|i| i.path == key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::jsonb::parse_path_expression;

    #[test]
    fn a_record_round_trips() {
        let entry = PathIndex {
            name: "ix_status".to_string(),
            path: "doc->'status'".to_string(),
            is_deleted: false,
        };
        let raw = entry.into_vec();
        assert_eq!(raw.len(), PATH_INDEX_SIZE as usize);
        assert_eq!(PathIndex::from_vec(&raw).unwrap(), entry);
    }

    #[test]
    fn a_tombstone_round_trips() {
        let entry = PathIndex {
            name: "ix".to_string(),
            path: "doc->'a'".to_string(),
            is_deleted: true,
        };
        assert!(PathIndex::from_vec(&entry.into_vec()).unwrap().is_deleted);
    }

    /// The rule that keeps the index from producing false negatives.
    #[test]
    fn only_a_text_path_may_be_indexed() {
        let text = parse_path_expression("doc->>'status'").unwrap();
        assert!(indexable(&text).is_ok());

        let document = parse_path_expression("doc->'status'").unwrap();
        let refused = indexable(&document).unwrap_err();
        assert!(refused.contains("->>"), "{}", refused);
    }

    /// `->` and `->>` address the same value, so a declaration written either
    /// way has to be found by a query written the other.
    #[test]
    fn a_declaration_is_found_by_the_path_it_indexes() {
        let declared = vec![PathIndex {
            name: "ix".to_string(),
            path: parse_path_expression("doc->>'status'").unwrap().index_key(),
            is_deleted: false,
        }];
        for spelling in ["doc->>'status'", "doc->'status'", "DOC ->> 'status'"] {
            let path = parse_path_expression(spelling).unwrap();
            assert!(
                index_for(&path, &declared).is_some(),
                "{} did not find the declaration",
                spelling
            );
        }
        let other = parse_path_expression("doc->>'other'").unwrap();
        assert!(index_for(&other, &declared).is_none());
    }

    #[test]
    fn a_path_too_long_to_record_is_refused() {
        let long = format!("doc->>'{}'", "k".repeat(200));
        let path = parse_path_expression(&long).unwrap();
        assert!(indexable(&path).is_err());
    }
}

// ---------------------------------------------------------------------------
// statements
// ---------------------------------------------------------------------------

/// What a `CREATE INDEX` over a document path says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePathIndex {
    pub name: String,
    pub table: String,
    pub path: PathExpr,
    pub if_not_exists: bool,
}

/// Recognises `CREATE INDEX ix ON t ((doc->>'k'))`.
///
/// `None` means "not an index over a path", which leaves every other form of
/// index DDL to the no-op it already was — the engine maintains its own index
/// over ordinary columns, so declaring one adds nothing. A path is the case
/// that genuinely cannot be inferred: there are unboundedly many paths a
/// document might contain and only the queries say which one matters.
///
/// `Some(Err(..))` is a path index that cannot be built, and it is deliberately
/// distinguished from `None`: an operator who wrote `->` rather than `->>`
/// should be told why it was refused, not have it silently accepted and do
/// nothing.
pub fn parse_create(sql: &str) -> Option<Result<CreatePathIndex, String>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    // `CREATE UNIQUE INDEX` is a constraint here, not an index; it has its own
    // rewrite and must not be captured.
    if !upper.starts_with("CREATE INDEX") {
        return None;
    }

    let mut rest = trimmed["CREATE INDEX".len()..].trim_start();
    let mut if_not_exists = false;
    if rest.to_ascii_uppercase().starts_with("IF NOT EXISTS") {
        if_not_exists = true;
        rest = rest["IF NOT EXISTS".len()..].trim_start();
    }

    // The name, then `ON`, then the table.
    let on_at = crate::core::rql::join::find_keyword(rest, "ON", 0)?;
    let name = rest[..on_at].trim().trim_matches('"').to_string();
    if name.is_empty() {
        return None;
    }
    let after_on = rest[on_at + 2..].trim_start();

    let open = after_on.find('(')?;
    let table = after_on[..open].trim().trim_matches('"').to_string();
    if table.is_empty() {
        return None;
    }
    let close = matching_paren(after_on, open)?;
    // Anything after the column list — `WHERE`, `USING`, `INCLUDE` — is a shape
    // this cannot honour, and honouring part of it would be worse than
    // declining the whole.
    if !after_on[close + 1..].trim().is_empty() {
        return None;
    }

    let inner = after_on[open + 1..close].trim();
    // A path may be written with PostgreSQL's required double parentheses or
    // without; `parse_path_expression` peels either.
    let path = crate::core::jsonb::parse_path_expression(inner)?;

    Some(match indexable(&path) {
        Ok(()) => Ok(CreatePathIndex {
            name,
            table,
            path,
            if_not_exists,
        }),
        Err(e) => Err(e),
    })
}

/// Recognises `DROP INDEX ix`, returning the name.
///
/// Nothing here says which table it belongs to, because PostgreSQL's index
/// names are database-wide; the caller finds the table holding it.
pub fn parse_drop(sql: &str) -> Option<(String, bool)> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("DROP INDEX") {
        return None;
    }
    let mut rest = trimmed["DROP INDEX".len()..].trim_start();
    let mut if_exists = false;
    if rest.to_ascii_uppercase().starts_with("IF EXISTS") {
        if_exists = true;
        rest = rest["IF EXISTS".len()..].trim_start();
    }
    // `CASCADE`/`RESTRICT` have nothing to act on here.
    let name = rest
        .split_whitespace()
        .next()?
        .trim_matches('"')
        .trim_end_matches(',')
        .to_string();
    if name.is_empty() {
        return None;
    }
    Some((name, if_exists))
}

fn matching_paren(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut quote: Option<u8> = None;
    for (index, byte) in bytes.iter().enumerate().skip(open) {
        match quote {
            Some(q) => {
                if *byte == q {
                    quote = None;
                }
            }
            None => match byte {
                b'\'' | b'"' => quote = Some(*byte),
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(index);
                    }
                }
                _ => {}
            },
        }
    }
    None
}

#[cfg(test)]
mod statement_tests {
    use super::*;

    #[test]
    fn the_postgresql_form_parses() {
        let parsed = parse_create("CREATE INDEX ix_status ON docs ((doc->>'status'))")
            .expect("recognised")
            .expect("indexable");
        assert_eq!(parsed.name, "ix_status");
        assert_eq!(parsed.table, "docs");
        assert_eq!(parsed.path.index_key(), "doc->'status'");
        assert!(!parsed.if_not_exists);
    }

    #[test]
    fn the_optional_spellings_parse() {
        for sql in [
            "CREATE INDEX ix ON docs ((doc->>'status'))",
            "CREATE INDEX ix ON docs (doc->>'status')",
            "create index ix on docs ((doc ->> 'status'))",
            "CREATE INDEX ix ON \"docs\" ((\"doc\"->>'status'));",
        ] {
            let parsed = parse_create(sql)
                .unwrap_or_else(|| panic!("{} should be recognised", sql))
                .unwrap_or_else(|e| panic!("{} should be indexable: {}", sql, e));
            assert_eq!(parsed.path.index_key(), "doc->'status'", "{}", sql);
            assert_eq!(parsed.table.to_lowercase(), "docs", "{}", sql);
        }

        let parsed = parse_create("CREATE INDEX IF NOT EXISTS ix ON docs ((doc->>'a'))")
            .unwrap()
            .unwrap();
        assert!(parsed.if_not_exists);
    }

    /// An index over an ordinary column stays the no-op it was: the engine
    /// already maintains one over every eligible column.
    #[test]
    fn what_is_not_a_path_index_is_left_alone() {
        for sql in [
            "CREATE INDEX ix ON docs (name)",
            "CREATE INDEX ix ON docs (a, b)",
            "CREATE UNIQUE INDEX ix ON docs ((doc->>'a'))",
            "CREATE INDEX ix ON docs ((doc->>'a')) WHERE id > 1",
            "CREATE INDEX ix ON docs USING gin (doc)",
            "SELECT 1",
        ] {
            assert!(
                parse_create(sql).is_none(),
                "{} should not be a path index",
                sql
            );
        }
    }

    /// Refused rather than ignored, and with the reason — see `indexable`.
    #[test]
    fn a_document_path_is_refused_with_a_reason() {
        let refused = parse_create("CREATE INDEX ix ON docs ((doc->'status'))")
            .expect("recognised as a path index")
            .expect_err("but not indexable");
        assert!(refused.contains("->>"), "{}", refused);
    }

    #[test]
    fn drop_parses() {
        assert_eq!(
            parse_drop("DROP INDEX ix_status"),
            Some(("ix_status".to_string(), false))
        );
        assert_eq!(
            parse_drop("DROP INDEX IF EXISTS ix"),
            Some(("ix".to_string(), true))
        );
        assert_eq!(
            parse_drop("drop index \"ix\";"),
            Some(("ix".to_string(), false))
        );
        assert!(parse_drop("DROP TABLE t").is_none());
    }
}
