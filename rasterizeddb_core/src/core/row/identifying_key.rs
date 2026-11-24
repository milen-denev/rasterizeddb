//! The columns that identify one row (sharding-plan.md B0.3).
//!
//! Moved out of the executor so storage-level paths (a shard's apply loop, a
//! tenant move) can address rows by primary key with the same bytes the
//! executor locks and deduplicates by. The SQL side (the write-back
//! predicate and its literals) stays in `rql::executor`.

/// The columns that identify one row, for the statements that need to name
/// rows they have already chosen (`docs/partitioning.md` §1b).
///
/// `FOR UPDATE` locks a row by them, a writer checks the same lock, and
/// `UPDATE … FROM` and `DELETE … USING` write each joined row back by them.
/// Only an inline `UNIQUE` used to count, so a table whose key is written
/// `PRIMARY KEY` or as a table-level `UNIQUE (id)` — the ordinary way an ORM
/// emits one — was refused as having no unique column at all, though it
/// plainly had one. And only one column counted, so a composite key, which is
/// the only kind a partitioned table has (every unique key holds every
/// partition-key column), refused all four statements.
pub(crate) struct IdentifyingKey {
    /// In the key's declared order, which is the order the framed tuple and
    /// the write-back predicate use.
    pub(crate) fields: Vec<crate::core::row::schema::SchemaField>,
    /// Each field's declared `NUMERIC` scale (`type_modifier::scale_of`),
    /// the scale every key cell this is given is at.
    pub(crate) scales: Vec<u32>,
}

impl IdentifyingKey {
    pub(crate) fn names(&self) -> impl Iterator<Item = &str> {
        self.fields.iter().map(|f| f.name.as_str())
    }

    /// The bytes one row is locked and deduplicated by, from its key cells in
    /// [`Self::fields`] order: one column's stored bytes, exactly what a
    /// one-column key always used, or the framed tuple of several (each
    /// component `encode_canonical`, which is length-prefixed, so `(1, 23)`
    /// and `(12, 3)` cannot meet). The same function on every side — the
    /// locker, the writer that checks, the deadlock detector's blockers — so
    /// they name one row one way.
    ///
    /// `Err` for a row whose key holds a NULL: it names no row (a unique key
    /// admits any number of NULLs), and the bytes a NULL travels as would
    /// name every NULL row at once.
    ///
    /// **Every cell must be at its column's declared scale**, never rescaled:
    /// the select pipeline's, and `run_subquery_scaled` with `normalize`
    /// false. A sixteen-byte decimal does not say its scale, and the paths
    /// differ: `NUMERIC(10,2)`'s 1.50 is 150 in the pipeline and was
    /// 1500000000 through a normalising subquery, so a lock keyed by one and
    /// checked by the other never met; normalising also rounds past nine
    /// places, so two `NUMERIC(20,12)` keys became one. A decimal is keyed by
    /// its value: the unscaled integer with trailing zeros stripped, and the
    /// scale left (a seventeen-byte cell's own scale where it carries one).
    pub(crate) fn row_key(&self, cells: &[&crate::core::row::row::Column]) -> Result<Vec<u8>, String> {
        if let Some((field, _)) =
            self.fields.iter().zip(cells).find(|(_, c)| c.column_type == crate::core::db_type::DbType::NULL)
        {
            return Err(format!(
                "a row whose key column '{}' is NULL cannot be identified by its key",
                field.name
            ));
        }
        let value = |i: usize, cell: &crate::core::row::row::Column| -> Vec<u8> {
            let bytes = cell.data.into_slice();
            if self.fields[i].db_type != crate::core::db_type::DbType::DECIMAL {
                return bytes.to_vec();
            }
            let Some((mut v, own)) = crate::core::value_codec::split_decimal_cell(bytes) else {
                return bytes.to_vec();
            };
            let mut scale = own.unwrap_or(self.scales[i]);
            while scale > 0 && v % 10 == 0 {
                v /= 10;
                scale -= 1;
            }
            if v == 0 {
                scale = 0;
            }
            let mut out = v.to_le_bytes().to_vec();
            out.push(scale as u8);
            out
        };
        if let [only] = cells {
            return Ok(value(0, only));
        }
        let mut out = Vec::new();
        for (i, (field, cell)) in self.fields.iter().zip(cells).enumerate() {
            let bytes = value(i, cell);
            let canonical = field.db_type != crate::core::db_type::DbType::DECIMAL
                && crate::core::index::key::encode_canonical(&field.db_type, &bytes, &mut out).is_some();
            if !canonical {
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(&bytes);
            }
        }
        Ok(out)
    }
}

/// The key that identifies `schema`'s rows, chosen as PostgreSQL names a row
/// by its key, or `None` when the table has none to name a row by.
///
/// 1. The primary key: one column (`schema.primary_key`), or a composite one,
///    which is the table-level key named `<table>_pkey` (`lexer_ct` records
///    only a one-column primary key as such).
/// 2. Else a `UNIQUE` key, inline or table-level, whose every column is
///    `NOT NULL`: the fewest columns, then the first declared.
/// 3. Else, as before composite keys counted, a one-column `UNIQUE` that may
///    hold NULL. Its NULL rows cannot be named, and [`IdentifyingKey::row_key`]
///    refuses them one by one rather than naming every NULL row at once.
///
/// A primary key's rows hold no NULL in PostgreSQL. Here a column of a
/// table-level `PRIMARY KEY (a, b)` written without `NOT NULL` is nullable,
/// so the same refusal covers such a row. A partial key is never chosen:
/// rows outside its predicate share its values, and writing back by it would
/// reach every one of them.
pub(crate) fn identifying_key(schema: &crate::core::row::schema::TableSchema) -> Option<IdentifyingKey> {
    let live = |name: &str| {
        schema
            .fields
            .iter()
            .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(name))
            .cloned()
    };
    let resolve = |columns: &[String]| columns.iter().map(|c| live(c)).collect::<Option<Vec<_>>>();
    let whole = || schema.unique_keys.iter().filter(|k| !k.is_deleted && k.predicate.is_none());

    let key = |fields: Vec<crate::core::row::schema::SchemaField>| {
        let scales = fields
            .iter()
            .map(|f| u32::from(crate::core::rql::type_modifier::scale_of(&f.name, &schema.type_modifiers)))
            .collect();
        IdentifyingKey { fields, scales }
    };
    if let Some(field) = schema.primary_key.as_deref().and_then(live) {
        return Some(key(vec![field]));
    }
    // A composite primary key, by its mark or its name
    // (`unique_key::primary_key_of`).
    if let Some(fields) = crate::core::rql::unique_key::primary_key_of(schema).and_then(|pk| resolve(&pk.columns)) {
        return Some(key(fields));
    }

    let candidates = || {
        schema
            .fields
            .iter()
            .filter(|f| f.is_unique && !f.is_deleted)
            .map(|f| vec![f.clone()])
            .chain(whole().filter_map(|k| resolve(&k.columns)))
    };
    let not_null = |fields: &Vec<crate::core::row::schema::SchemaField>| {
        fields.iter().all(|f| !crate::core::rql::nullable::is_nullable(&f.name, &schema.fields))
    };
    let mut best: Option<Vec<crate::core::row::schema::SchemaField>> = None;
    for fields in candidates().filter(not_null) {
        if best.as_ref().is_none_or(|b| fields.len() < b.len()) {
            best = Some(fields);
        }
    }
    best.or_else(|| candidates().find(|fields| fields.len() == 1)).map(key)
}
