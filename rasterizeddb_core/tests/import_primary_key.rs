//! The import's primary keys, through the migrator's own plan.
//!
//! `migrate::plan` creates each table with its keys, drops them for the bulk
//! load and adds them back after (`drop_constraints`, `add_constraints`). The
//! table that comes out must read as one created with the key: `pg_index`'s
//! `indisprimary`, `SHOW COLUMNS`' `is_primary_key`, the key enforced, its
//! columns NOT NULL, a second primary key refused, across a restart. Each is
//! compared with a twin created directly by `CREATE TABLE … PRIMARY KEY`.
//! No PostgreSQL needed: the plan is built from a source schema as the
//! catalogue reader would return it.

#![cfg(feature = "pg_import")]

mod partition_wire;

use partition_wire::*;
use rasterizeddb_core::core::migrate::plan::{ImportOptions, TablePlan, plan};
use rasterizeddb_core::core::migrate::source::{SourceSchema, SourceTable, UniqueKey};
use rasterizeddb_core::core::migrate::types::SourceColumn;

fn column(name: &str, type_name: &str, category: char, not_null: bool) -> SourceColumn {
    SourceColumn {
        name: name.to_string(),
        attnum: 1,
        type_name: type_name.to_string(),
        type_kind: 'b',
        type_category: category,
        element_type_name: None,
        type_modifier: -1,
        not_null,
        default: None,
        formatted_type: type_name.to_string(),
        json_shreddable: None,
    }
}

fn table(name: &str, columns: Vec<SourceColumn>, primary_key: &[&str], unique_keys: Vec<UniqueKey>) -> SourceTable {
    SourceTable {
        schema: "public".to_string(),
        name: name.to_string(),
        oid: 1,
        columns,
        primary_key: primary_key.iter().map(|c| c.to_string()).collect(),
        unique_keys,
        foreign_keys: Vec::new(),
        estimated_rows: None,
        notes: Vec::new(),
    }
}

/// `users`: a one-column key and a UNIQUE beside it. `pairs`: a composite key.
fn plans() -> Vec<TablePlan> {
    let schema = SourceSchema {
        tables: vec![
            table(
                "users",
                vec![column("id", "int4", 'N', true), column("email", "text", 'S', false)],
                &["id"],
                vec![UniqueKey { name: "users_email_key".to_string(), columns: vec!["email".to_string()] }],
            ),
            table(
                "pairs",
                vec![column("a", "int4", 'N', true), column("b", "int4", 'N', true), column("v", "text", 'S', false)],
                &["a", "b"],
                Vec::new(),
            ),
        ],
        notes: Vec::new(),
    };
    plan(&schema, &ImportOptions::default()).expect("plan").tables
}

/// How a client sees the table's keys: `pg_index`, and `SHOW COLUMNS`' key
/// flags per column.
fn keys_seen(c: &mut Client, t: &str) -> (Vec<Vec<Option<String>>>, Vec<Vec<Option<String>>>) {
    let index = c
        .ok(&format!(
            "SELECT i.indisunique, i.indisprimary, i.indnatts FROM pg_index i JOIN pg_class r ON i.indrelid = r.oid \
             WHERE r.relname = '{t}' ORDER BY 1, 2, 3"
        ))
        .rows;
    let shown = c.ok(&format!("SHOW COLUMNS FROM {t}"));
    let at = |name: &str| shown.columns.iter().position(|c| c == name).unwrap_or_else(|| panic!("no {name}"));
    let (name, nullable, unique, primary) = (at("column_name"), at("nullable"), at("is_unique"), at("is_primary_key"));
    let columns = shown
        .rows
        .iter()
        .map(|r| vec![r[name].clone(), r[nullable].clone(), r[unique].clone(), r[primary].clone()])
        .collect();
    (index, columns)
}

fn primary_flags(c: &mut Client, t: &str) -> Vec<(String, String)> {
    let (_, columns) = keys_seen(c, t);
    columns.into_iter().map(|r| (r[0].clone().unwrap(), r[3].clone().unwrap())).collect()
}

#[test]
fn an_imported_primary_key_is_the_tables_primary_key() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = Server::start(dir.path().to_str().unwrap());
    let plans = plans();
    {
        let mut c = server.connect();
        for p in &plans {
            c.ok(&p.create);
            for d in &p.drop_constraints {
                c.ok(d);
            }
        }
        // The keys are off for the load, the primary key included: no
        // catalog calls a column the key while nothing keeps it unique.
        for t in ["users", "pairs"] {
            let (index, _) = keys_seen(&mut c, t);
            assert!(index.iter().all(|r| r[1].as_deref() == Some("f")), "{t} during the load: {index:?}");
            assert!(primary_flags(&mut c, t).iter().all(|(_, p)| p == "f"), "{t} during the load");
        }
        // The load.
        c.ok("INSERT INTO users (id, email) VALUES (1, 'a'), (2, 'b'), (3, NULL)");
        c.ok("INSERT INTO pairs (a, b, v) VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, NULL)");
        for p in &plans {
            for a in &p.add_constraints {
                c.ok(a);
            }
        }
        // The twins, created with their keys.
        c.ok("CREATE TABLE users_ref (id INTEGER NOT NULL, email TEXT, PRIMARY KEY (id), CONSTRAINT users_ref_email_key UNIQUE (email))");
        c.ok("CREATE TABLE pairs_ref (a INTEGER NOT NULL, b INTEGER NOT NULL, v TEXT, PRIMARY KEY (a, b))");
    }
    for round in ["after the import", "after a restart"] {
        let mut c = server.connect();
        assert_eq!(
            primary_flags(&mut c, "users"),
            vec![("id".to_string(), "t".to_string()), ("email".to_string(), "f".to_string())],
            "{round}: users"
        );
        let (index, _) = keys_seen(&mut c, "users");
        assert!(index.iter().any(|r| r[1].as_deref() == Some("t")), "{round}: no indisprimary on users: {index:?}");
        for (t, twin) in [("users", "users_ref"), ("pairs", "pairs_ref")] {
            assert_eq!(keys_seen(&mut c, t), keys_seen(&mut c, twin), "{round}: {t} reads unlike {twin}");
        }
        // Enforced as a key, NOT NULL included.
        for (sql, code) in [
            ("INSERT INTO users (id, email) VALUES (1, 'z')", "23505"),
            ("INSERT INTO users (id, email) VALUES (NULL, 'z')", "23502"),
            ("INSERT INTO pairs (a, b, v) VALUES (1, 2, 'z')", "23505"),
            ("ALTER TABLE users ADD PRIMARY KEY (email)", "42P16"),
            ("ALTER TABLE pairs ADD CONSTRAINT other_pk PRIMARY KEY (b, a)", "42P16"),
        ] {
            let e = c.simple(sql).expect_err(sql);
            assert_eq!(e.code, code, "{round}: {sql}: {e}");
        }
        drop(c);
        if round == "after the import" {
            server = server.restart();
        }
    }
}

