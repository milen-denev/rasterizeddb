//! Foreign keys a `CREATE TABLE` declares, checked before anything is
//! written (`docs/partitioning.md` §10, phase 3 P4), for plain and
//! partitioned tables alike.

use super::Database;
use crate::core::rql::foreign_key::{ForeignKey, ReferencedSide, referenced_key_refusal};
use crate::core::rql::lexer_ct::CreateTable;

impl Database {
    /// Why the foreign keys `ct` declares, or the ones other tables already
    /// declare naming `ct.table_name`, cannot stand once `ct` is created
    /// (partitioned by `partition_key`, when it is), or `None`. In
    /// PostgreSQL's words, so the wire maps each to its SQLSTATE: a missing
    /// referencing or referenced column (42703), a partitioned parent's
    /// column that no unique key is exactly (42830), a name taken by another
    /// of the table's constraints (42710), and a name too long to store
    /// (0A000).
    ///
    /// A parent that does not exist is refused, as PostgreSQL refuses it
    /// (42P01, "relation … does not exist"). It was accepted, and every insert
    /// into the child then failed naming the missing table. Keys naming a
    /// table that does not exist can still be on disk (written before this
    /// check); a partitioned table later created under that name is held to
    /// them (below), so their checks route.
    ///
    /// Called under the CREATE's locks: ACCESS EXCLUSIVE on the name and
    /// SHARE on every parent (`lock_create_target`), so no parent's keys
    /// change while they are read here.
    pub(crate) fn create_foreign_key_refusal(&self, ct: &CreateTable, partition_key: Option<&[&str]>) -> Option<String> {
        let own_key = partition_key.map(|k| k.iter().map(|c| c.to_string()).collect::<Vec<_>>());
        let own_side = || ReferencedSide {
            table: &ct.table_name,
            fields: ct
                .columns
                .iter()
                .filter(|c| !crate::core::rql::nullable::is_hidden(&c.name))
                .map(|c| (c.name.as_str(), false, ct.unique_columns.iter().any(|u| u.eq_ignore_ascii_case(&c.name))))
                .collect(),
            primary_key: ct.primary_key.as_deref(),
            unique_keys: &ct.unique_keys,
            partition_key: own_key.clone(),
        };
        let other_names: Vec<&str> = ct
            .checks
            .iter()
            .map(|c| c.name.as_str())
            .chain(ct.unique_keys.iter().map(|k| k.name.as_str()))
            .collect();
        for (i, fk) in ct.foreign_keys.iter().enumerate() {
            if !ct
                .columns
                .iter()
                .any(|c| !crate::core::rql::nullable::is_hidden(&c.name) && c.name.eq_ignore_ascii_case(&fk.column))
            {
                return Some(format!("column \"{}\" referenced in foreign key constraint does not exist", fk.column));
            }
            if let Err(why) = fk.storable() {
                return Some(why);
            }
            let clash = other_names.iter().any(|n| n.eq_ignore_ascii_case(&fk.name))
                || ct.foreign_keys[..i].iter().any(|other| other.name.eq_ignore_ascii_case(&fk.name));
            if clash {
                return Some(format!("constraint \"{}\" for table \"{}\" already exists", fk.name, ct.table_name));
            }
            if let Some(why) = self.parent_refusal(fk, &ct.table_name, &own_side) {
                return Some(why);
            }
            // The two columns' types (42804), against the parent as it is
            // now or as this statement declares it.
            let child_type = ct.columns.iter().find(|c| c.name.eq_ignore_ascii_case(&fk.column)).map(|c| &c.data_type);
            let parent_type = if fk.parent_table.eq_ignore_ascii_case(&ct.table_name) {
                ct.columns.iter().find(|c| c.name.eq_ignore_ascii_case(&fk.parent_column)).map(|c| c.data_type.clone())
            } else {
                self.column_type(&fk.parent_table, &fk.parent_column)
            };
            if let (Some(child), Some(parent)) = (child_type, parent_type)
                && let Some(why) = crate::core::rql::foreign_key::type_refusal(fk, child, &parent)
            {
                return Some(why);
            }
        }
        // Keys another table declared on this name while no table had it: a
        // partitioned table must be a parent they can be checked against by
        // routing. (A plain one is accepted as the plain path accepts it.)
        if partition_key.is_none() {
            return None;
        }
        for (child, fk) in self.referencing_keys(&ct.table_name) {
            if fk.is_deleted {
                continue;
            }
            if let Some(why) = referenced_key_refusal(&fk, &own_side()) {
                return Some(format!("table \"{child}\" has foreign key \"{}\" naming \"{}\": {why}", fk.name, ct.table_name));
            }
            let parent_type = ct.columns.iter().find(|c| c.name.eq_ignore_ascii_case(&fk.parent_column)).map(|c| &c.data_type);
            if let (Some(child_type), Some(parent)) = (self.column_type(&child, &fk.column), parent_type)
                && let Some(why) = crate::core::rql::foreign_key::type_refusal(&fk, &child_type, parent)
            {
                return Some(why);
            }
        }
        None
    }

    /// Why `fk`, declared on `table`, cannot reference its parent as the
    /// parent stands now (`own` when it references `table` itself).
    fn parent_refusal<'a>(
        &self,
        fk: &ForeignKey,
        table: &str,
        own: &dyn Fn() -> ReferencedSide<'a>,
    ) -> Option<String> {
        if fk.parent_table.eq_ignore_ascii_case(table) {
            return referenced_key_refusal(fk, &own());
        }
        self.existing_parent_refusal(fk)
    }

    /// The column a `REFERENCES parent` with no column list names: the
    /// parent's primary key, as in PostgreSQL. `own_table` and `own_key` are
    /// the declaring table's name and primary key, for a key on itself
    /// (`CREATE TABLE`'s own declaration, which no schema holds yet).
    ///
    /// Refused as PostgreSQL refuses it: a missing parent (42P01), one with
    /// no primary key (42830), and here also a composite one, since this
    /// engine's foreign keys are one column. Called under the statement's
    /// lock on the parent, so the key read is the key enforced.
    pub(crate) fn default_referenced_column(
        &self,
        fk: &ForeignKey,
        own_table: &str,
        own_key: Option<&str>,
        own_unique_keys: &[crate::core::rql::unique_key::UniqueKey],
    ) -> Result<String, String> {
        // A one-column primary key is the header's; a composite one is only
        // a unique key, under the primary key's default name.
        let (key, composite) = if fk.parent_table.eq_ignore_ascii_case(own_table) {
            (own_key.map(str::to_string), composite_primary_key(own_table, own_unique_keys))
        } else {
            let view = self
                .schema_view(&fk.parent_table)
                .ok_or_else(|| format!("relation \"{}\" does not exist", fk.parent_table))?;
            let schema = view.schema();
            (schema.primary_key.clone(), composite_primary_key(&fk.parent_table, &schema.unique_keys))
        };
        if let Some(key) = key.map(|k| k.trim().trim_matches('"').to_string()).filter(|k| !k.is_empty()) {
            return Ok(key);
        }
        if composite {
            // PostgreSQL's words for one referencing column against a
            // composite key (42830).
            return Err("number of referencing and referenced columns for foreign key disagree".to_string());
        }
        Err(format!("there is no primary key for referenced table \"{}\"", fk.parent_table))
    }

    /// The declared type of `table`'s live column `column`, if both exist.
    pub(crate) fn column_type(&self, table: &str, column: &str) -> Option<crate::core::db_type::DbType> {
        let view = self.schema_view(table)?;
        view.schema()
            .fields
            .iter()
            .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(column))
            .map(|f| f.db_type.clone())
    }

    /// Why `fk` cannot reference its parent, a table other than the one
    /// declaring it: `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY`'s check too,
    /// which needs the parent to exist (42P01), since it validates the rows
    /// already there against it.
    pub(crate) fn existing_parent_refusal(&self, fk: &ForeignKey) -> Option<String> {
        let Some(view) = self.schema_view(&fk.parent_table) else {
            return Some(format!("relation \"{}\" does not exist", fk.parent_table));
        };
        let partition_key = if view.is_partitioned() {
            match self.partitioned_table(&fk.parent_table).map(|pt| pt.key_columns()) {
                Some(Ok(key)) => Some(key),
                Some(Err(e)) => return Some(e.to_string()),
                None => return Some(format!("relation \"{}\" does not exist", fk.parent_table)),
            }
        } else {
            None
        };
        referenced_key_refusal(fk, &ReferencedSide::of_schema(view.schema(), &fk.parent_table, partition_key))
    }
}

/// Whether `table`'s unique keys hold a live composite primary key (one
/// declared `PRIMARY KEY (a, b)` without a name of its own).
fn composite_primary_key(table: &str, keys: &[crate::core::rql::unique_key::UniqueKey]) -> bool {
    let name = crate::core::rql::unique_key::pkey_name(table);
    keys.iter().any(|k| !k.is_deleted && k.columns.len() > 1 && k.name.eq_ignore_ascii_case(&name))
}
