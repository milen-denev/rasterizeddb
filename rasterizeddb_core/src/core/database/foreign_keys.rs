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
    /// A parent that does not exist yet is accepted, as the plain path always
    /// accepted it (PostgreSQL refuses it, 42P01): each insert's check then
    /// names it. A partitioned table later created under that name is held
    /// to the keys naming it (below), so their checks route.
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
        if self.schema_view(&fk.parent_table).is_none() {
            return None;
        }
        self.existing_parent_refusal(fk)
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
