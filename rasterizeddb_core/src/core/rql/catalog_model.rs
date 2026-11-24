//! The PostgreSQL catalogue a driver or `psql` reads, as one model per
//! statement.
//!
//! A driver learns the server's types from `pg_type` when it connects
//! (Npgsql refused to connect: "relation pg_type does not exist"), and a
//! tool describes a table from `pg_attribute`, `pg_attrdef`, `pg_constraint`
//! and `pg_index` joined to `pg_class` (`psql`'s `\d` failed on each). The
//! relations here are built from one [`Model`] of the database, so a
//! column's `attnum` in `pg_attribute` is the one `pg_index.indkey` and
//! `pg_constraint.conkey` name, and a relation's oid is the one every view
//! gives it.
//!
//! The functions that read the catalogue by oid (`pg_get_indexdef`,
//! `pg_get_constraintdef`, `pg_get_userbyid`, `pg_get_function_result`, …)
//! read the model the statement built ([`lookup`]); `format_type` needs none.
//!
//! Built-in types carry PostgreSQL 17's oids, so a type a `RowDescription`
//! advertises (`DbType::pg_oid`) is found by the oid the driver was given.

use std::collections::HashMap;
use std::sync::{Arc as StdArc, Mutex};

use rclite::Arc;

use crate::core::database::Database;
use crate::core::database::relation::Relation;
use crate::core::db_type::DbType;
use crate::core::row::schema::{SchemaField, TableSchema};

/// `pg_catalog`'s oid.
pub const NS_CATALOG: i64 = 11;
/// `public`'s oid.
pub const NS_PUBLIC: i64 = 2200;
/// `information_schema`'s oid (PostgreSQL 17's initdb gives it one past
/// 13000; nothing reads it but a join to `pg_namespace`).
pub const NS_INFORMATION_SCHEMA: i64 = 13178;
/// The bootstrap superuser's oid, which owns what no role was recorded as
/// owning.
pub const BOOTSTRAP_ROLE: i64 = 10;
/// `pg_database_owner`, the role standing for whoever owns the database.
pub const DATABASE_OWNER_ROLE: i64 = 6171;

/// A value of a catalogue row.
#[derive(Debug, Clone)]
pub(crate) enum V {
    I(i64),
    T(String),
    B(bool),
    /// An array of integers (`int2vector`, `int2[]`).
    A(Vec<i64>),
    /// An array of text (`text[]`): `pg_class.reloptions`.
    TA(Vec<String>),
    N,
}

fn t(s: impl Into<String>) -> V {
    V::T(s.into())
}

impl V {
    fn literal(&self) -> String {
        match self {
            V::I(n) => n.to_string(),
            V::T(s) => crate::auth::quote(s),
            V::B(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            V::A(items) => format!(
                "'{{{}}}'",
                items
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            V::TA(items) => crate::auth::quote(&format!(
                "{{{}}}",
                items
                    .iter()
                    .map(|i| format!("\"{}\"", i.replace('\\', "\\\\").replace('"', "\\\"")))
                    .collect::<Vec<_>>()
                    .join(",")
            )),
            V::N => "NULL".to_string(),
        }
    }
}

/// The SQL type a catalogue column is created with.
fn sql_type(ty: &DbType) -> &'static str {
    match ty {
        DbType::I64 => "BIGINT",
        DbType::I32 => "INTEGER",
        DbType::I16 => "SMALLINT",
        DbType::BOOL => "BOOLEAN",
        DbType::F64 => "DOUBLE PRECISION",
        DbType::ARRAYOF(_) => "SMALLINT[]",
        DbType::ARRAY => "TEXT[]",
        DbType::DATETIME => "TIMESTAMPTZ",
        _ => "TEXT",
    }
}

/// `CREATE TABLE` and the rows' `INSERT`s for one catalogue relation.
pub(crate) fn table_sql(
    temp: &str,
    columns: &[(&str, DbType)],
    rows: &[Vec<V>],
) -> (String, Vec<String>) {
    let create = format!(
        "CREATE TABLE {} ({})",
        temp,
        columns
            .iter()
            .map(|(n, ty)| format!("{} {}", n, sql_type(ty)))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let names = columns
        .iter()
        .map(|(n, _)| *n)
        .collect::<Vec<_>>()
        .join(", ");
    let inserts = rows
        .iter()
        .map(|row| {
            format!(
                "INSERT INTO {} ({}) VALUES ({})",
                temp,
                names,
                row.iter().map(V::literal).collect::<Vec<_>>().join(", ")
            )
        })
        .collect();
    (create, inserts)
}

// ------------------------------------------------------------------ types

/// One built-in type: oid, name, length, `typtype`, category, its array's
/// oid (0 for none), collation.
struct BuiltinType(i64, &'static str, i64, char, char, i64, i64);

const BUILTIN_TYPES: &[BuiltinType] = &[
    BuiltinType(16, "bool", 1, 'b', 'B', 1000, 0),
    BuiltinType(17, "bytea", -1, 'b', 'U', 1001, 0),
    BuiltinType(18, "char", 1, 'b', 'Z', 1002, 0),
    BuiltinType(19, "name", 64, 'b', 'S', 1003, 950),
    BuiltinType(20, "int8", 8, 'b', 'N', 1016, 0),
    BuiltinType(21, "int2", 2, 'b', 'N', 1005, 0),
    BuiltinType(22, "int2vector", -1, 'b', 'A', 1006, 0),
    BuiltinType(23, "int4", 4, 'b', 'N', 1007, 0),
    BuiltinType(24, "regproc", 4, 'b', 'N', 1008, 0),
    BuiltinType(25, "text", -1, 'b', 'S', 1009, 100),
    BuiltinType(26, "oid", 4, 'b', 'N', 1028, 0),
    BuiltinType(27, "tid", 6, 'b', 'U', 1010, 0),
    BuiltinType(28, "xid", 4, 'b', 'U', 1011, 0),
    BuiltinType(29, "cid", 4, 'b', 'U', 1012, 0),
    BuiltinType(30, "oidvector", -1, 'b', 'A', 1013, 0),
    BuiltinType(114, "json", -1, 'b', 'U', 199, 0),
    BuiltinType(142, "xml", -1, 'b', 'U', 143, 0),
    BuiltinType(600, "point", 16, 'b', 'G', 1017, 0),
    BuiltinType(601, "lseg", 32, 'b', 'G', 1018, 0),
    BuiltinType(602, "path", -1, 'b', 'G', 1019, 0),
    BuiltinType(603, "box", 32, 'b', 'G', 1020, 0),
    BuiltinType(604, "polygon", -1, 'b', 'G', 1027, 0),
    BuiltinType(628, "line", 24, 'b', 'G', 629, 0),
    BuiltinType(650, "cidr", -1, 'b', 'I', 651, 0),
    BuiltinType(700, "float4", 4, 'b', 'N', 1021, 0),
    BuiltinType(701, "float8", 8, 'b', 'N', 1022, 0),
    BuiltinType(705, "unknown", -2, 'p', 'X', 0, 0),
    BuiltinType(718, "circle", 24, 'b', 'G', 719, 0),
    BuiltinType(774, "macaddr8", 8, 'b', 'U', 775, 0),
    BuiltinType(790, "money", 8, 'b', 'N', 791, 0),
    BuiltinType(829, "macaddr", 6, 'b', 'U', 1040, 0),
    BuiltinType(869, "inet", -1, 'b', 'I', 1041, 0),
    BuiltinType(1033, "aclitem", 16, 'b', 'U', 1034, 0),
    BuiltinType(1042, "bpchar", -1, 'b', 'S', 1014, 100),
    BuiltinType(1043, "varchar", -1, 'b', 'S', 1015, 100),
    BuiltinType(1082, "date", 4, 'b', 'D', 1182, 0),
    BuiltinType(1083, "time", 8, 'b', 'D', 1183, 0),
    BuiltinType(1114, "timestamp", 8, 'b', 'D', 1115, 0),
    BuiltinType(1184, "timestamptz", 8, 'b', 'D', 1185, 0),
    BuiltinType(1186, "interval", 16, 'b', 'T', 1187, 0),
    BuiltinType(1266, "timetz", 12, 'b', 'D', 1270, 0),
    BuiltinType(1560, "bit", -1, 'b', 'V', 1561, 0),
    BuiltinType(1562, "varbit", -1, 'b', 'V', 1563, 0),
    BuiltinType(1700, "numeric", -1, 'b', 'N', 1231, 0),
    BuiltinType(1790, "refcursor", -1, 'b', 'U', 2201, 0),
    BuiltinType(2202, "regprocedure", 4, 'b', 'N', 2207, 0),
    BuiltinType(2205, "regclass", 4, 'b', 'N', 2210, 0),
    BuiltinType(2206, "regtype", 4, 'b', 'N', 2211, 0),
    BuiltinType(2249, "record", -1, 'p', 'P', 2287, 0),
    BuiltinType(2275, "cstring", -2, 'p', 'P', 1263, 0),
    BuiltinType(2276, "any", 4, 'p', 'P', 0, 0),
    BuiltinType(2277, "anyarray", -1, 'p', 'P', 0, 0),
    BuiltinType(2278, "void", 4, 'p', 'P', 0, 0),
    BuiltinType(2279, "trigger", 4, 'p', 'P', 0, 0),
    BuiltinType(2283, "anyelement", 4, 'p', 'P', 0, 0),
    BuiltinType(2950, "uuid", 16, 'b', 'U', 2951, 0),
    BuiltinType(3220, "pg_lsn", 8, 'b', 'U', 3221, 0),
    BuiltinType(3614, "tsvector", -1, 'b', 'U', 3643, 0),
    BuiltinType(3615, "tsquery", -1, 'b', 'U', 3645, 0),
    BuiltinType(3802, "jsonb", -1, 'b', 'U', 3807, 0),
    BuiltinType(4072, "jsonpath", -1, 'b', 'U', 4073, 0),
    BuiltinType(4089, "regnamespace", 4, 'b', 'N', 4090, 0),
    BuiltinType(4096, "regrole", 4, 'b', 'N', 4097, 0),
    BuiltinType(5069, "xid8", 8, 'b', 'U', 271, 0),
    // Ranges and multiranges: `pg_range` pairs them with their subtypes.
    BuiltinType(3904, "int4range", -1, 'r', 'R', 3905, 0),
    BuiltinType(3906, "numrange", -1, 'r', 'R', 3907, 0),
    BuiltinType(3908, "tsrange", -1, 'r', 'R', 3909, 0),
    BuiltinType(3910, "tstzrange", -1, 'r', 'R', 3911, 0),
    BuiltinType(3912, "daterange", -1, 'r', 'R', 3913, 0),
    BuiltinType(3926, "int8range", -1, 'r', 'R', 3927, 0),
    BuiltinType(4451, "int4multirange", -1, 'm', 'R', 6150, 0),
    BuiltinType(4532, "nummultirange", -1, 'm', 'R', 6151, 0),
    BuiltinType(4533, "tsmultirange", -1, 'm', 'R', 6152, 0),
    BuiltinType(4534, "tstzmultirange", -1, 'm', 'R', 6153, 0),
    BuiltinType(4535, "datemultirange", -1, 'm', 'R', 6155, 0),
    BuiltinType(4536, "int8multirange", -1, 'm', 'R', 6157, 0),
];

/// `(range, subtype, multirange)`.
const RANGES: &[(i64, i64, i64)] = &[
    (3904, 23, 4451),
    (3906, 1700, 4532),
    (3908, 1114, 4533),
    (3910, 1184, 4534),
    (3912, 1082, 4535),
    (3926, 20, 4536),
];

/// The receive functions `pg_type.typreceive` names, as `pg_proc` lists
/// them. A driver tells an array from a base type by its receive function
/// being `array_recv` (Npgsql does).
const RECEIVE_FUNCTIONS: &[(i64, &str)] = &[
    (2400, "array_recv"),
    (2402, "record_recv"),
    (3836, "range_recv"),
    (4231, "multirange_recv"),
];

struct TypeRow {
    oid: i64,
    name: String,
    len: i64,
    typtype: char,
    category: char,
    elem: i64,
    array: i64,
    receive: i64,
    collation: i64,
    namespace: i64,
}

fn type_rows() -> Vec<TypeRow> {
    let mut out = Vec::new();
    for b in BUILTIN_TYPES {
        let receive = match b.3 {
            'r' => 3836,
            'm' => 4231,
            _ if b.0 == 2249 => 2402,
            _ => 0,
        };
        out.push(TypeRow {
            oid: b.0,
            name: b.1.to_string(),
            len: b.2,
            typtype: b.3,
            category: b.4,
            elem: 0,
            array: b.5,
            receive,
            collation: b.6,
            namespace: NS_CATALOG,
        });
        if b.5 != 0 {
            out.push(TypeRow {
                oid: b.5,
                name: format!("_{}", b.1),
                len: -1,
                typtype: 'b',
                category: 'A',
                elem: b.0,
                array: 0,
                receive: 2400,
                collation: b.6,
                namespace: NS_CATALOG,
            });
        }
    }
    out
}

/// [`type_rows`] and an enum type's two (itself and its array) for each of
/// `enums`, in `public`.
fn type_rows_with(enums: &[crate::core::rql::user_types::EnumType]) -> Vec<TypeRow> {
    let mut out = type_rows();
    for e in enums {
        let oid = e.oid();
        out.push(TypeRow {
            oid,
            name: e.name.clone(),
            len: 4,
            typtype: 'e',
            category: 'E',
            elem: 0,
            array: oid + 1,
            receive: 3532,
            collation: 0,
            namespace: NS_PUBLIC,
        });
        out.push(TypeRow {
            oid: oid + 1,
            name: format!("_{}", e.name),
            len: -1,
            typtype: 'b',
            category: 'A',
            elem: oid,
            array: 0,
            receive: 2400,
            collation: 0,
            namespace: NS_PUBLIC,
        });
    }
    out
}

/// A type's name by its oid, as `format_type` writes it, without a modifier.
fn type_display(oid: i64) -> Option<String> {
    let rows = BUILTIN_TYPES;
    if let Some(b) = rows.iter().find(|b| b.0 == oid) {
        return Some(
            match b.1 {
                "bool" => "boolean",
                "int8" => "bigint",
                "int2" => "smallint",
                "int4" => "integer",
                "float4" => "real",
                "float8" => "double precision",
                "varchar" => "character varying",
                "bpchar" => "character",
                "timestamp" => "timestamp without time zone",
                "timestamptz" => "timestamp with time zone",
                "time" => "time without time zone",
                "timetz" => "time with time zone",
                "varbit" => "bit varying",
                "char" => "\"char\"",
                other => other,
            }
            .to_string(),
        );
    }
    rows.iter()
        .find(|b| b.5 == oid)
        .and_then(|b| type_display(b.0))
        .map(|e| format!("{e}[]"))
}

/// `format_type(oid, typmod)`: the type's SQL name with its modifier, as
/// PostgreSQL writes it (`character varying(20)`, `numeric(10,2)`,
/// `timestamp(3) without time zone`, `integer[]`). `None` for an oid that
/// names no type, which PostgreSQL shows as `???`.
pub fn format_type(oid: i64, typmod: Option<i64>) -> Option<String> {
    let base = type_display(oid)?;
    let typmod = typmod.filter(|m| *m >= 0);
    let Some(m) = typmod else {
        return Some(base);
    };
    Some(match oid {
        1043 | 1042 | 1560 | 1562 if m >= 4 || matches!(oid, 1560 | 1562) => {
            let n = if matches!(oid, 1560 | 1562) { m } else { m - 4 };
            format!("{base}({n})")
        }
        1700 if m >= 4 => {
            let m = m - 4;
            format!("numeric({},{})", (m >> 16) & 0xffff, m & 0xffff)
        }
        1114 => format!("timestamp({m}) without time zone"),
        1184 => format!("timestamp({m}) with time zone"),
        1083 => format!("time({m}) without time zone"),
        1266 => format!("time({m}) with time zone"),
        1186 => format!("interval({m})"),
        _ => base,
    })
}

// ------------------------------------------------------------------ model

/// A column as the catalogue describes it.
#[derive(Debug, Clone)]
pub(crate) struct Column {
    pub attnum: i64,
    pub name: String,
    pub type_oid: i64,
    pub typmod: i64,
    pub len: i64,
    pub not_null: bool,
    pub default: Option<String>,
    /// `a` (always), `d` (by default) or empty.
    pub identity: &'static str,
    /// `s` for a stored generated column, or empty.
    pub generated: &'static str,
    pub collation: i64,
    /// `pg_attribute.attcompression`: the method a variable-width column
    /// stores its values with (`zstd`, `zstd(9)`; `core::compression`), or
    /// empty for none, as PostgreSQL leaves it for the default.
    pub compression: String,
}

/// An index, as `pg_index` and `pg_class` list it.
#[derive(Debug, Clone)]
pub(crate) struct Index {
    pub oid: i64,
    pub name: String,
    pub unique: bool,
    pub primary: bool,
    pub ready: bool,
    pub keys: Vec<i64>,
    pub method: &'static str,
    pub definition: String,
}

/// A constraint, as `pg_constraint` lists it.
#[derive(Debug, Clone)]
pub(crate) struct Constraint {
    pub oid: i64,
    pub name: String,
    pub contype: &'static str,
    pub index: i64,
    pub keys: Vec<i64>,
    pub foreign_table: i64,
    pub foreign_keys: Vec<i64>,
    pub on_update: &'static str,
    pub on_delete: &'static str,
    /// A foreign key's `confmatchtype`: `s` (MATCH SIMPLE) or `f` (MATCH
    /// FULL); a blank for any other constraint.
    pub match_type: &'static str,
    /// A foreign key's referenced columns, in key order, resolved to the
    /// parent's attnums into `foreign_keys` once the model is complete.
    pub foreign_columns: Vec<String>,
    pub definition: String,
}

/// A relation: a table (`r`), view (`v`), materialized view (`m`) or
/// sequence (`S`).
#[derive(Debug, Clone)]
pub(crate) struct Rel {
    pub oid: i64,
    pub name: String,
    pub kind: &'static str,
    pub owner: i64,
    pub columns: Vec<Column>,
    pub indexes: Vec<Index>,
    pub constraints: Vec<Constraint>,
    pub bytes: i64,
    pub tuples: i64,
    /// A partitioned table's key columns, outermost first, by `attnum`
    /// (`pg_partitioned_table.partattrs`); empty for any other relation.
    pub partition_key: Vec<i64>,
    /// `pg_class.reloptions`: `row_compression=zstd(9)` for a table that
    /// compresses its row tails (`core::compression`); empty otherwise, which
    /// is listed as NULL, as PostgreSQL lists a table with no options.
    pub options: Vec<String>,
    /// `pg_class.relacl` as aclitem text (`alice=arw/owner`); empty, listed
    /// as NULL, while the relation has its owner's default rights only.
    pub acl: Vec<String>,
    /// Row-level security on, and forced on its owner (R3).
    pub rls: (bool, bool),
}

/// A role, as `pg_roles` lists it.
#[derive(Debug, Clone)]
pub(crate) struct RoleRow {
    pub oid: i64,
    pub name: String,
    pub superuser: bool,
    pub login: bool,
    pub create_role: bool,
    pub create_db: bool,
    /// `-1`: no limit.
    pub conn_limit: i64,
    pub valid_until_us: Option<i64>,
    /// `ALTER ROLE … SET` values, `param=value`.
    pub config: Vec<String>,
    pub bypass_rls: bool,
}

/// One privilege one role holds on one object.
#[derive(Debug, Clone)]
pub(crate) struct GrantRow {
    pub grantor: String,
    pub grantee: String,
    pub object: String,
    /// `TABLE`, `SEQUENCE`, `FUNCTION` or `PROCEDURE`.
    pub kind: &'static str,
    pub privilege: String,
    pub grantable: bool,
}

/// The grants on the relations `rels` lists and on routines, with each
/// owner's own privileges first (held with their grant option, as an
/// owner's are).
fn grants(acl: &[crate::core::rql::privileges::AclRow], rels: &[Rel]) -> Vec<GrantRow> {
    use crate::core::rql::privileges::{ROUTINE_PRIVILEGES, SEQUENCE_PRIVILEGES, TABLE_PRIVILEGES};
    let kind_of = |object: &str| -> Option<(&'static str, &'static [&'static str], String)> {
        if let Some(name) = object.strip_prefix("@function:") {
            return Some(("FUNCTION", ROUTINE_PRIVILEGES, name.to_string()));
        }
        if let Some(name) = object.strip_prefix("@procedure:") {
            return Some(("PROCEDURE", ROUTINE_PRIVILEGES, name.to_string()));
        }
        let rel = rels.iter().find(|r| r.name == object)?;
        Some(if rel.kind == "S" { ("SEQUENCE", SEQUENCE_PRIVILEGES, rel.name.clone()) } else { ("TABLE", TABLE_PRIVILEGES, rel.name.clone()) })
    };
    let mut out = Vec::new();
    for owner in acl.iter().filter(|a| a.privilege == "OWNER") {
        let Some((kind, all, object)) = kind_of(&owner.relation) else { continue };
        for p in all {
            out.push(GrantRow {
                grantor: owner.grantee.clone(),
                grantee: owner.grantee.clone(),
                object: object.clone(),
                kind,
                privilege: p.to_string(),
                grantable: true,
            });
        }
    }
    for a in acl.iter().filter(|a| a.privilege != "OWNER" && acl_letter(&a.privilege).is_some()) {
        let Some((kind, all, object)) = kind_of(&a.relation) else { continue };
        if !all.contains(&a.privilege.as_str()) {
            continue;
        }
        out.push(GrantRow {
            grantor: a.grantor.clone(),
            grantee: if a.grantee == "public" { "PUBLIC".to_string() } else { a.grantee.clone() },
            object,
            kind,
            privilege: a.privilege.clone(),
            grantable: a.grantable,
        });
    }
    out
}

/// Each role's default privileges per kind (`@default:role[:kind]`), as
/// aclitems, PUBLIC's EXECUTE taken away where a marker says so.
fn default_acls(acl: &[crate::core::rql::privileges::AclRow], roles: &[RoleRow]) -> Vec<(i64, &'static str, Vec<String>)> {
    use crate::core::rql::privileges::{DefaultKind, NOEXECUTE};
    let mut out = Vec::new();
    for role in roles {
        for (kind, letter) in [(DefaultKind::Tables, "r"), (DefaultKind::Sequences, "S"), (DefaultKind::Functions, "f")] {
            let key = kind.relation(&role.name);
            let rows: Vec<&crate::core::rql::privileges::AclRow> = acl.iter().filter(|a| a.relation == key).collect();
            if rows.is_empty() {
                continue;
            }
            let mut items: Vec<String> = Vec::new();
            let mut by_grantee: Vec<(&str, String)> = Vec::new();
            for r in rows.iter().filter(|r| r.privilege != NOEXECUTE) {
                let Some(letter) = acl_letter(&r.privilege) else { continue };
                match by_grantee.iter_mut().find(|(g, _)| *g == r.grantee) {
                    Some((_, l)) => l.push(letter),
                    None => by_grantee.push((r.grantee.as_str(), letter.to_string())),
                }
            }
            // A function's defaults name PUBLIC's EXECUTE only by taking it
            // away: listed, as PostgreSQL lists them, with the owner's own.
            if kind == DefaultKind::Functions && rows.iter().any(|r| r.privilege == NOEXECUTE) {
                items.push(format!("{}=X/{}", acl_name(&role.name), acl_name(&role.name)));
            }
            for (grantee, letters) in by_grantee {
                let grantee = if grantee == "public" { String::new() } else { acl_name(grantee) };
                items.push(format!("{grantee}={letters}/{}", acl_name(&role.name)));
            }
            out.push((role.oid, letter, items));
        }
    }
    out
}

/// Where `pg_auth_members` row oids start: above the fixed catalogue oids,
/// below the hashed oids of user objects' usual range.
const AUTH_MEMBERS_OID_BASE: i64 = 60_000;
/// Where `pg_policy` row oids start.
const POLICY_OID_BASE: i64 = 64_000;
/// Where `pg_default_acl` row oids start.
const DEFAULT_ACL_OID_BASE: i64 = 62_000;

/// One role membership (`GRANT role TO member`), with its options (R2).
#[derive(Debug, Clone)]
pub(crate) struct MemberRow {
    pub roleid: i64,
    pub member: i64,
    pub grantor: i64,
    pub admin: bool,
    pub inherit: bool,
    pub set: bool,
}

/// A user's function or procedure, as `pg_proc` lists it.
#[derive(Debug, Clone)]
pub(crate) struct ProcRow {
    pub oid: i64,
    pub name: String,
    pub kind: &'static str,
    pub result: String,
    pub result_oid: i64,
    pub arguments: String,
    pub arg_oids: Vec<i64>,
    pub returns_set: bool,
    pub language: String,
    pub body: String,
}

/// The PRIMARY KEY or UNIQUE constraint of `fk`'s referenced table whose
/// columns are exactly the ones `fk` references, in any order: what
/// `information_schema` calls its unique constraint.
fn referenced_key<'m>(model: &'m Model, fk: &Constraint) -> Option<&'m Constraint> {
    let parent = model.rels.iter().find(|r| r.oid == fk.foreign_table)?;
    parent.constraints.iter().find(|k| {
        matches!(k.contype, "p" | "u")
            && !fk.foreign_keys.is_empty()
            && k.keys.len() == fk.foreign_keys.len()
            && fk.foreign_keys.iter().all(|a| k.keys.contains(a))
    })
}

/// What one statement's catalogue relations are built from.
#[derive(Debug, Default)]
pub(crate) struct Model {
    /// The database's name: the `…_catalog` columns of `information_schema`.
    pub database: String,
    pub rels: Vec<Rel>,
    pub roles: Vec<RoleRow>,
    /// Role memberships, as `pg_auth_members` lists them.
    pub memberships: Vec<MemberRow>,
    /// Every privilege held on a table, view, sequence or routine, the
    /// owners' own included: the `information_schema` privilege views.
    pub grants: Vec<GrantRow>,
    /// Row-level security policies, as `pg_policy` lists them.
    pub policies: Vec<crate::core::rql::policies::Policy>,
    /// Default privileges, as `pg_default_acl` lists them: (owner oid,
    /// object type letter, aclitems).
    pub default_acls: Vec<(i64, &'static str, Vec<String>)>,
    pub procs: Vec<ProcRow>,
    /// The enum types (`user_types`).
    pub enums: Vec<crate::core::rql::user_types::EnumType>,
}

impl Model {
    fn index(&self, oid: i64) -> Option<(&Rel, &Index)> {
        self.rels
            .iter()
            .find_map(|r| r.indexes.iter().find(|i| i.oid == oid).map(|i| (r, i)))
    }

    fn constraint(&self, oid: i64) -> Option<&Constraint> {
        self.rels
            .iter()
            .find_map(|r| r.constraints.iter().find(|c| c.oid == oid))
    }

    /// The relation an oid names, a table's or an index's.
    pub fn relation_name(&self, oid: i64) -> Option<String> {
        self.rels
            .iter()
            .find(|r| r.oid == oid)
            .map(|r| r.name.clone())
            .or_else(|| self.index(oid).map(|(_, i)| i.name.clone()))
    }

    fn role_name(&self, oid: i64) -> Option<String> {
        self.roles
            .iter()
            .find(|r| r.oid == oid)
            .map(|r| r.name.clone())
    }
}

/// The oid a name has, as every catalogue view gives it.
pub fn oid_of(name: &str) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in name.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    ((hash % 2_000_000_000) + 16_384) as i64
}

/// The oid of an index of `table`.
pub fn index_oid(table: &str, index: &str) -> i64 {
    oid_of(&format!("{table}.{index}"))
}

/// A column's type oid, modifier and storage length.
fn column_type(field: &SchemaField, schema: &TableSchema) -> (i64, i64, i64) {
    use crate::core::rql::type_modifier::TextTypmod;
    if crate::core::rql::json_map::shadow_for(&field.name, &schema.json_columns).is_some() {
        return (3802, -1, -1);
    }
    let ty = field.literal_type();
    let typmod = field.text_typmod();
    let (oid, modifier) = match (&ty, typmod) {
        (DbType::STRING | DbType::BPCHAR(_), Some(TextTypmod::Varchar(n))) => {
            (1043, if n > 0 { n as i64 + 4 } else { -1 })
        }
        (DbType::STRING | DbType::BPCHAR(_), Some(TextTypmod::Bpchar(n))) => {
            (1042, if n > 0 { n as i64 + 4 } else { -1 })
        }
        (DbType::BPCHAR(n), _) => (1042, if *n > 0 { *n as i64 + 4 } else { -1 }),
        (DbType::ARRAY, Some(TextTypmod::Element(code))) => (
            crate::core::rql::type_modifier::described_element(code)
                .map_or(1009, |c| DbType::ARRAYOF(c).pg_oid() as i64),
            -1,
        ),
        (DbType::DECIMAL, _) => {
            let declared = schema
                .type_modifiers
                .iter()
                .find(|m| !m.is_deleted && m.column == field.name);
            (
                1700,
                declared.map_or(-1, |m| {
                    ((i64::from(m.precision) << 16) | i64::from(m.scale)) + 4
                }),
            )
        }
        (
            DbType::DATETIME | DbType::TIMESTAMP | DbType::TIME | DbType::TIMETZ | DbType::INTERVAL,
            Some(TextTypmod::Precision(p)),
        ) => (ty.pg_oid() as i64, p as i64),
        _ => (ty.pg_oid() as i64, -1),
    };
    let len = BUILTIN_TYPES
        .iter()
        .find(|b| b.0 == oid)
        .map_or(-1, |b| b.2);
    (oid, modifier, len)
}

/// A column's type oid, as `pg_attribute.atttypid` gives it.
pub(crate) fn column_type_oid(field: &SchemaField, schema: &TableSchema) -> i64 {
    column_type(field, schema).0
}

fn collation_of(type_oid: i64) -> i64 {
    if matches!(type_oid, 25 | 1042 | 1043) {
        100
    } else {
        0
    }
}

/// A table's live columns in order, numbered from one.
/// What [`column_origins`] reads of a column: its number, name and declared
/// type, as [`table_columns`] gives them.
#[derive(Debug, Clone, PartialEq)]
struct OriginColumn {
    attnum: i64,
    name: String,
    type_oid: i64,
    typmod: i64,
}

/// [`table_columns`]' numbering, names and types alone. Every
/// `RowDescription` of a SELECT reads them, and the rest of
/// `table_columns` (defaults, identity, generated expressions, a search
/// of every field for each one's NULL flag) was most of what describing a
/// point query cost.
fn origin_columns(schema: &TableSchema) -> Vec<OriginColumn> {
    use crate::core::rql::nullable;
    let mut fields: Vec<&SchemaField> = schema
        .fields
        .iter()
        .filter(|f| !f.is_deleted && !nullable::is_hidden(&f.name))
        .collect();
    fields.sort_by_key(|f| f.write_order);
    fields
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let (type_oid, typmod, _) = column_type(field, schema);
            OriginColumn { attnum: i as i64 + 1, name: field.name.clone(), type_oid, typmod }
        })
        .collect()
}

fn table_columns(schema: &TableSchema) -> Vec<Column> {
    use crate::core::rql::nullable;
    let mut fields: Vec<&SchemaField> = schema
        .fields
        .iter()
        .filter(|f| !f.is_deleted && !nullable::is_hidden(&f.name))
        .collect();
    fields.sort_by_key(|f| f.write_order);
    fields
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let (type_oid, typmod, len) = column_type(field, schema);
            let nullable = schema.fields.iter().any(|f| {
                !f.is_deleted
                    && f.name
                        .eq_ignore_ascii_case(&nullable::flag_column_name(&field.name))
            });
            let identity = match crate::core::rql::column_default::identity_of(
                &field.name,
                &schema.defaults,
            ) {
                Some(crate::core::rql::column_default::IdentityKind::Always) => "a",
                Some(_) => "d",
                None => "",
            };
            let generated =
                crate::core::rql::generated::expression_for(&field.name, &schema.generated);
            let default = if !identity.is_empty() {
                None
            } else {
                generated.map(str::to_string).or_else(|| {
                    crate::core::rql::column_default::expression_for(&field.name, &schema.defaults)
                        .map(str::to_string)
                })
            };
            Column {
                attnum: i as i64 + 1,
                name: field.name.clone(),
                type_oid,
                typmod,
                len,
                not_null: !nullable,
                default,
                identity,
                generated: if generated.is_some() { "s" } else { "" },
                collation: collation_of(type_oid),
                compression: crate::core::compression::method_name(crate::core::compression::level_of(
                    &schema.compression,
                    field.write_order,
                ))
                .unwrap_or_default(),
            }
        })
        .collect()
}

/// Where a result column comes from: the relation's oid, the column's
/// number, its type and modifier, as a `RowDescription` names them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ColumnOrigin {
    pub table_oid: i32,
    pub attnum: i16,
    pub type_oid: i32,
    pub typmod: i32,
}

/// Each result column of `sql`, a SELECT whose items are columns of the
/// tables it names (`*`, `t.*`, `col`, `t.col`, aliased or not), as the
/// table and column it reads; `None` for an item that computes. `None`
/// overall for a statement whose items cannot be read so (a UNION, a derived
/// table, a view or a relation this database has not).
pub(crate) fn column_origins(database: &Database, sql: &str) -> Option<Vec<Option<ColumnOrigin>>> {
    use crate::core::rql::join::find_keyword;
    let sql = sql.trim().trim_end_matches(';').trim_end();
    if !sql.get(..6).is_some_and(|w| w.eq_ignore_ascii_case("SELECT")) || find_keyword(sql, "UNION", 0).is_some() {
        return None;
    }
    let from = find_keyword(sql, "FROM", 0)?;
    let list = sql[6..from].trim_start();
    let list = if list.get(..8).is_some_and(|w| w.eq_ignore_ascii_case("DISTINCT")) { &list[8..] } else { list };
    let rest = &sql[from + 4..];
    let end = ["WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "FOR", "WINDOW", "INTERSECT", "EXCEPT"]
        .iter()
        .filter_map(|k| find_keyword(rest, k, 0))
        .min()
        .unwrap_or(rest.len());
    let from_text = rest[..end].trim();
    if from_text.contains('(') {
        return None;
    }
    let relations = crate::core::rql::join::parse_from_clause(from_text).ok()?;
    // Each relation's columns, numbered as `pg_attribute` numbers them.
    let mut tables: Vec<(String, i64, Vec<OriginColumn>)> = Vec::with_capacity(relations.len());
    for r in &relations {
        let relation = database.relation(&r.table)?;
        let schema = relation.schema();
        tables.push((r.label.clone(), oid_of(&r.table), origin_columns(&schema)));
    }
    let origin = |oid: i64, c: &OriginColumn| ColumnOrigin {
        table_oid: oid as i32,
        attnum: c.attnum as i16,
        type_oid: c.type_oid as i32,
        typmod: c.typmod as i32,
    };
    let unquote = |s: &str| {
        let s = s.trim();
        if s.starts_with('"') { s.trim_matches('"').to_string() } else { s.to_ascii_lowercase() }
    };
    let mut out = Vec::new();
    for item in crate::core::sql_scan::split_top_level(list, b',') {
        let (expr, _) = crate::core::rql::lexer_query::split_alias(item.trim());
        let expr = expr.trim();
        if expr == "*" {
            for (_, oid, columns) in &tables {
                out.extend(columns.iter().map(|c| Some(origin(*oid, c))));
            }
            continue;
        }
        let is_name = |s: &str| {
            let s = s.trim();
            !s.is_empty()
                && (s.starts_with('"') && s.ends_with('"') && s.len() >= 2
                    || s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') && !s.as_bytes()[0].is_ascii_digit())
        };
        let (qualifier, column) = match expr.split_once('.') {
            Some((q, c)) if is_name(q) && (c.trim() == "*" || is_name(c)) => (Some(unquote(q)), c.trim()),
            _ if is_name(expr) => (None, expr),
            _ => {
                out.push(None);
                continue;
            }
        };
        if column == "*" {
            let (_, oid, columns) = tables.iter().find(|(label, _, _)| Some(label.to_ascii_lowercase()) == qualifier.as_ref().map(|q| q.to_ascii_lowercase()))?;
            out.extend(columns.iter().map(|c| Some(origin(*oid, c))));
            continue;
        }
        let column = unquote(column);
        let found: Vec<ColumnOrigin> = tables
            .iter()
            .filter(|(label, _, _)| qualifier.as_ref().is_none_or(|q| label.eq_ignore_ascii_case(q)))
            .filter_map(|(_, oid, columns)| columns.iter().find(|c| c.name == column).map(|c| origin(*oid, c)))
            .collect();
        out.push(if found.len() == 1 { Some(found[0]) } else { None });
    }
    Some(out)
}

fn quote_ident(name: &str) -> String {
    crate::core::rql::views::quote_ident(name)
}

/// The names of the columns `pairs` point at, `(table oid, attnum)` as a
/// `RowDescription` from [`column_origins`] names them, grouped by table in
/// the order the tables are first named: (table oid, table name, its
/// (attnum, column name)s). A pair naming no column of a table here is
/// left out, as an inner join with `pg_attribute` leaves it.
pub(crate) fn origin_names(database: &Database, pairs: &[(i64, i64)]) -> Vec<(i64, String, Vec<(i64, String)>)> {
    let mut out: Vec<(i64, String, Vec<(i64, String)>)> = Vec::new();
    let mut tables: Vec<(i64, String, Vec<Column>)> = Vec::new();
    for (oid, attnum) in pairs {
        if !tables.iter().any(|(o, _, _)| o == oid) {
            if let Some(name) = database.table_names().into_iter().find(|n| oid_of(n) == *oid)
                && let Some(relation) = database.relation(&name)
            {
                tables.push((*oid, name, table_columns(&relation.schema())));
            }
        }
        let Some((_, name, columns)) = tables.iter().find(|(o, _, _)| o == oid) else { continue };
        let Some(column) = columns.iter().find(|c| c.attnum == *attnum) else { continue };
        match out.iter_mut().find(|(o, _, _)| o == oid) {
            Some((_, _, named)) => {
                if !named.iter().any(|(n, _)| n == attnum) {
                    named.push((*attnum, column.name.clone()));
                }
            }
            None => out.push((*oid, name.clone(), vec![(*attnum, column.name.clone())])),
        }
    }
    out
}

fn action_code(
    a: &crate::core::rql::foreign_key::ReferentialAction,
) -> (&'static str, &'static str) {
    use crate::core::rql::foreign_key::ReferentialAction::*;
    match a {
        NoAction => ("a", ""),
        Restrict => ("r", "RESTRICT"),
        Cascade => ("c", "CASCADE"),
        SetNull => ("n", "SET NULL"),
        SetDefault => ("d", "SET DEFAULT"),
    }
}

/// A table's indexes and constraints, from the declarations `pg_indexes`
/// lists (`shared` is `catalog_views::shared_declarations`).
fn table_indexes(
    name: &str,
    schema: &TableSchema,
    columns: &[Column],
    shared: Vec<(String, bool, Vec<u32>, Option<String>)>,
    ready: bool,
) -> (Vec<Index>, Vec<Constraint>) {
    use crate::core::rql::unique_key::pkey_name;
    let attnum_of_order = |wo: u32| {
        schema
            .fields
            .iter()
            .find(|f| !f.is_deleted && f.write_order as u32 == wo)
            .and_then(|f| columns.iter().find(|c| c.name == f.name))
            .map(|c| (c.attnum, c.name.clone()))
    };
    let attnum_of = |column: &str| {
        columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(column))
            .map(|c| c.attnum)
    };
    let pkey = crate::core::rql::unique_key::primary_key_of(schema).map_or_else(|| pkey_name(&schema.name), |key| key.name);
    let mut indexes = Vec::new();
    let mut constraints = Vec::new();
    for (index_name, unique, keys, predicate) in shared {
        let Some(resolved) = keys
            .iter()
            .map(|k| attnum_of_order(*k))
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        if indexes
            .iter()
            .any(|i: &Index| i.name.eq_ignore_ascii_case(&index_name))
        {
            continue;
        }
        let primary = unique && index_name == pkey;
        let method = schema
            .secondary_indexes
            .iter()
            .find(|d| !d.is_deleted && d.name.eq_ignore_ascii_case(&index_name))
            .map_or("btree", |d| d.method.name());
        let column_list = resolved
            .iter()
            .map(|(_, n)| quote_ident(n))
            .collect::<Vec<_>>()
            .join(", ");
        let oid = index_oid(name, &index_name);
        indexes.push(Index {
            oid,
            name: index_name.clone(),
            unique,
            primary,
            ready,
            keys: resolved.iter().map(|(a, _)| *a).collect(),
            method,
            definition: format!(
                "CREATE {}INDEX {} ON public.{} USING {} ({}){}",
                if unique { "UNIQUE " } else { "" },
                quote_ident(&index_name),
                quote_ident(name),
                method,
                column_list,
                predicate
                    .as_ref()
                    .map(|p| format!(" WHERE ({p})"))
                    .unwrap_or_default()
            ),
        });
        // A unique index under a predicate is an index, not a constraint, and
        // so is one `CREATE UNIQUE INDEX` made: PostgreSQL gives either no
        // `pg_constraint` row, and psql's `\d` lists it as `UNIQUE`, not
        // `UNIQUE CONSTRAINT`.
        let made_as_index = schema
            .unique_keys
            .iter()
            .any(|k| !k.is_deleted && k.from_index && k.name.eq_ignore_ascii_case(&index_name));
        if unique && predicate.is_none() && !made_as_index {
            constraints.push(Constraint {
                oid: oid_of(&format!("{name}.{index_name}.con")),
                name: index_name.clone(),
                contype: if primary { "p" } else { "u" },
                index: oid,
                keys: resolved.iter().map(|(a, _)| *a).collect(),
                foreign_table: 0,
                foreign_keys: Vec::new(),
                on_update: "a",
                on_delete: "a",
                match_type: " ",
                foreign_columns: Vec::new(),
                definition: format!(
                    "{} ({})",
                    if primary { "PRIMARY KEY" } else { "UNIQUE" },
                    column_list
                ),
            });
        }
    }
    // GIN indexes over one document column.
    for decl in schema
        .secondary_indexes
        .iter()
        .filter(|d| !d.is_deleted && d.method.opclass().is_some())
    {
        let Some(resolved) = decl
            .columns
            .iter()
            .map(|k| attnum_of_order(*k))
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        let class = match decl.method.opclass() {
            Some(crate::core::jsonb::GinOpclass::PathOps) => " jsonb_path_ops",
            _ => "",
        };
        indexes.push(Index {
            oid: index_oid(name, &decl.name),
            name: decl.name.clone(),
            unique: false,
            primary: false,
            ready,
            keys: resolved.iter().map(|(a, _)| *a).collect(),
            method: "gin",
            definition: format!(
                "CREATE INDEX {} ON public.{} USING gin ({}{})",
                quote_ident(&decl.name),
                quote_ident(name),
                resolved
                    .iter()
                    .map(|(_, n)| quote_ident(n))
                    .collect::<Vec<_>>()
                    .join(", "),
                class
            ),
        });
    }
    for fk in schema.foreign_keys.iter().filter(|f| !f.is_deleted) {
        let (on_update, update_text) = action_code(&fk.on_update);
        let (on_delete, delete_text) = action_code(&fk.on_delete);
        let list = |columns: &[String]| columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        // PostgreSQL's `pg_get_constraintdef`: `FOREIGN KEY (a, b)
        // REFERENCES p(x, y) [MATCH FULL] [ON UPDATE …] [ON DELETE … [(cols)]]`.
        let mut definition = format!(
            "FOREIGN KEY ({}) REFERENCES {}({})",
            list(&fk.columns),
            quote_ident(&fk.parent_table),
            list(&fk.parent_columns)
        );
        if fk.match_full {
            definition.push_str(" MATCH FULL");
        }
        if !update_text.is_empty() {
            definition.push_str(&format!(" ON UPDATE {update_text}"));
        }
        if !delete_text.is_empty() {
            definition.push_str(&format!(" ON DELETE {delete_text}"));
            if !fk.on_delete_columns.is_empty() {
                definition.push_str(&format!(" ({})", list(&fk.on_delete_columns)));
            }
        }
        constraints.push(Constraint {
            oid: oid_of(&format!("{name}.{}.con", fk.name)),
            name: fk.name.clone(),
            contype: "f",
            index: 0,
            // In key order, as `conkey` lists them; a column that is not
            // there leaves the whole list out rather than a part of it.
            keys: fk.columns.iter().map(|c| attnum_of(c)).collect::<Option<Vec<i64>>>().unwrap_or_default(),
            foreign_table: oid_of(&fk.parent_table),
            // The parent's attnums are resolved when the model is complete.
            foreign_keys: Vec::new(),
            on_update,
            on_delete,
            match_type: if fk.match_full { "f" } else { "s" },
            foreign_columns: fk.parent_columns.clone(),
            definition,
        });
    }
    for check in schema.checks.iter().filter(|c| !c.is_deleted) {
        constraints.push(Constraint {
            oid: oid_of(&format!("{name}.{}.con", check.name)),
            name: check.name.clone(),
            contype: "c",
            index: 0,
            keys: Vec::new(),
            foreign_table: 0,
            foreign_keys: Vec::new(),
            on_update: "a",
            on_delete: "a",
            match_type: " ",
            foreign_columns: Vec::new(),
            definition: format!("CHECK (({}))", check.expression.trim()),
        });
    }
    (indexes, constraints)
}

/// The input a model is built from: what `catalog_views::build` has already
/// gathered for the statement.
pub(crate) struct Gathered {
    pub tables: Vec<String>,
    pub views: Vec<crate::core::rql::views::ViewDef>,
    pub sequences: Vec<String>,
}

/// Builds the model of `database` as `tx` sees it.
pub(crate) async fn build_model(
    database: &Arc<Database>,
    tx: Option<crate::core::mvcc::transaction::TxId>,
    gathered: Gathered,
    shared: &(dyn Fn(&TableSchema) -> Vec<(String, bool, Vec<u32>, Option<String>)> + Sync),
    ready_of: &(dyn Fn(&Relation) -> bool + Sync),
    sizes: &HashMap<String, (i64, i64)>,
) -> Model {
    let acl = if database.has_table(crate::core::rql::privileges::ACL_TABLE) {
        crate::core::rql::privileges::load(database, tx)
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let roles = roles(database, &acl).await;
    let owner = |relation: &str| {
        crate::core::rql::privileges::owner_of(&acl, relation)
            .and_then(|o| roles.iter().find(|r| r.name == o).map(|r| r.oid))
            .unwrap_or(BOOTSTRAP_ROLE)
    };
    let mut rels: Vec<Rel> = Vec::new();
    let materialized: Vec<&str> = gathered
        .views
        .iter()
        .filter(|v| v.kind == crate::core::rql::views::ViewKind::Materialized)
        .map(|v| v.name.as_str())
        .collect();
    // Not the role catalogue: a system catalogue, as `pg_authid` is to
    // PostgreSQL, which no client statement may read whole
    // (`privileges::catalog_refusal`). Listed here, `pg_dump` dumped it with
    // a `COPY` that the guard refuses, and every dump failed.
    for name in gathered
        .tables
        .iter()
        .map(String::as_str)
        .filter(|name| *name != crate::auth::ROLE_TABLE)
        .chain(materialized.iter().copied())
    {
        let Some(relation) = database.relation(name) else {
            continue;
        };
        let schema = relation.schema();
        let columns = table_columns(&schema);
        let (indexes, constraints) = table_indexes(
            name,
            &schema,
            &columns,
            shared(&schema),
            ready_of(&relation),
        );
        let (bytes, tuples) = sizes.get(name).copied().unwrap_or((0, 0));
        // A partitioned table is PostgreSQL's partitioned parent, relkind
        // 'p' with its `pg_partitioned_table` row. Its partitions are not
        // relations a client can name, so nothing lists them as children
        // (`pg_inherits` stays empty).
        let partition_key: Vec<i64> = match &relation {
            Relation::Partitioned(pt) => pt
                .key_columns()
                .unwrap_or_default()
                .iter()
                .filter_map(|k| columns.iter().find(|c| c.name.eq_ignore_ascii_case(k)).map(|c| c.attnum))
                .collect(),
            Relation::Plain(_) => Vec::new(),
        };
        rels.push(Rel {
            acl: Vec::new(),
            rls: (false, false),
            oid: oid_of(name),
            name: name.to_string(),
            kind: if materialized.contains(&name) {
                "m"
            } else if matches!(relation, Relation::Partitioned(_)) {
                "p"
            } else {
                "r"
            },
            partition_key,
            options: crate::core::compression::method_name(schema.compression.row.as_ref().map(|c| c.level))
                .map(|m| vec![format!("row_compression={m}")])
                .unwrap_or_default(),
            owner: owner(name),
            columns,
            indexes,
            constraints,
            bytes,
            tuples,
        });
    }
    // A foreign key's referenced column, by the parent's numbering.
    let parents: HashMap<i64, Vec<(String, i64)>> = rels
        .iter()
        .map(|r| {
            (
                r.oid,
                r.columns
                    .iter()
                    .map(|c| (c.name.clone(), c.attnum))
                    .collect(),
            )
        })
        .collect();
    for rel in rels.iter_mut() {
        for con in rel.constraints.iter_mut().filter(|c| c.contype == "f") {
            let Some(columns) = parents.get(&con.foreign_table) else {
                continue;
            };
            // In key order, `confkey` index for index with `conkey`.
            con.foreign_keys = con
                .foreign_columns
                .iter()
                .map(|wanted| columns.iter().find(|(n, _)| n.eq_ignore_ascii_case(wanted)).map(|(_, a)| *a))
                .collect::<Option<Vec<i64>>>()
                .unwrap_or_default();
        }
    }
    for view in gathered
        .views
        .iter()
        .filter(|v| v.kind == crate::core::rql::views::ViewKind::View)
    {
        let select = format!("SELECT * FROM {}", quote_ident(&view.name));
        let fields = match Box::pin(crate::pgwire::describe_query(database, &select)).await {
            Ok(Some((fields, true))) => fields,
            _ => Vec::new(),
        };
        let columns = fields
            .iter()
            .enumerate()
            .map(|(i, (name, ty))| Column {
                attnum: i as i64 + 1,
                name: name.clone(),
                type_oid: ty.pg_oid() as i64,
                typmod: ty.pg_typmod() as i64,
                len: BUILTIN_TYPES
                    .iter()
                    .find(|b| b.0 == ty.pg_oid() as i64)
                    .map_or(-1, |b| b.2),
                not_null: false,
                default: None,
                identity: "",
                generated: "",
                collation: collation_of(ty.pg_oid() as i64),
                // A view stores nothing.
                compression: String::new(),
            })
            .collect();
        rels.push(Rel {
            acl: Vec::new(),
            rls: (false, false),
            oid: oid_of(&view.name),
            name: view.name.clone(),
            kind: "v",
            partition_key: Vec::new(),
            options: Vec::new(),
            owner: owner(&view.name),
            columns,
            indexes: Vec::new(),
            constraints: Vec::new(),
            bytes: 0,
            tuples: 0,
        });
    }
    for name in &gathered.sequences {
        rels.push(Rel {
            oid: oid_of(name),
            name: name.clone(),
            kind: "S",
            partition_key: Vec::new(),
            options: Vec::new(),
            acl: Vec::new(),
            rls: (false, false),
            owner: owner(name),
            columns: Vec::new(),
            indexes: Vec::new(),
            constraints: Vec::new(),
            bytes: 0,
            tuples: 1,
        });
    }
    let procs = procs(database, tx).await;
    // Boxed: inline, the model future grew past what the Send check of a
    // caller that spawns it can follow.
    let policies = Box::pin(crate::core::rql::policies::load(database, None, tx)).await.unwrap_or_default();
    let enums = crate::core::rql::user_types::load(database, None, tx).await.unwrap_or_default();
    // Grouped once, so each relation reads its own rows and not the whole
    // ACL (`aclitems` needs only those: the owner's row has the same key).
    let mut by_relation: HashMap<&str, Vec<crate::core::rql::privileges::AclRow>> = HashMap::new();
    for a in &acl {
        by_relation.entry(a.relation.as_str()).or_default().push(a.clone());
    }
    for rel in &mut rels {
        if let Some(rows) = by_relation.get(rel.name.as_str()) {
            rel.acl = aclitems(rows, &rel.name);
            rel.rls = crate::core::rql::policies::rls_state(rows, &rel.name);
        }
    }
    let memberships = memberships(&acl, &roles);
    let grants = grants(&acl, &rels);
    let default_acls = default_acls(&acl, &roles);
    Model { database: database.name.clone(), rels, roles, memberships, grants, default_acls, policies, procs, enums }
}

/// PostgreSQL's letter for a privilege in an aclitem.
fn acl_letter(privilege: &str) -> Option<char> {
    Some(match privilege {
        "SELECT" => 'r',
        "INSERT" => 'a',
        "UPDATE" => 'w',
        "DELETE" => 'd',
        "TRUNCATE" => 'D',
        "REFERENCES" => 'x',
        "TRIGGER" => 't',
        "MAINTAIN" => 'm',
        "EXECUTE" => 'X',
        "USAGE" => 'U',
        "CREATE" => 'C',
        "CONNECT" => 'c',
        "TEMPORARY" | "TEMP" => 'T',
        _ => return None,
    })
}

/// The aclitems of the object kept under `key`, as PostgreSQL prints them
/// (`grantee=privs/grantor`, `*` after a privilege held with its grant
/// option, an empty grantee for PUBLIC), the owner's own item first. Empty
/// while no one but the owner holds anything, which is NULL there too.
pub(crate) fn aclitems(acl: &[crate::core::rql::privileges::AclRow], key: &str) -> Vec<String> {
    let grants: Vec<&crate::core::rql::privileges::AclRow> =
        acl.iter().filter(|a| a.relation == key && acl_letter(&a.privilege).is_some()).collect();
    if grants.is_empty() {
        return Vec::new();
    }
    let owner = crate::core::rql::privileges::owner_of(acl, key).unwrap_or("postgres");
    let mut out = vec![format!("{}=arwdDxtm/{}", acl_name(owner), acl_name(owner))];
    let mut groups: Vec<((&str, &str), String)> = Vec::new();
    for g in grants {
        let who = (g.grantee.as_str(), g.grantor.as_str());
        let letters = match groups.iter_mut().find(|(k, _)| *k == who) {
            Some((_, letters)) => letters,
            None => {
                groups.push((who, String::new()));
                &mut groups.last_mut().expect("pushed just above").1
            }
        };
        letters.push(acl_letter(&g.privilege).expect("filtered above"));
        if g.grantable {
            letters.push('*');
        }
    }
    for ((grantee, grantor), letters) in groups {
        let grantee = if grantee == "public" { String::new() } else { acl_name(grantee) };
        out.push(format!("{grantee}={letters}/{}", acl_name(grantor)));
    }
    out
}

/// A role name inside an aclitem: double-quoted unless plain.
fn acl_name(name: &str) -> String {
    if name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') && !name.is_empty() {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// The memberships the ACL holds, between roles that exist.
fn memberships(acl: &[crate::core::rql::privileges::AclRow], roles: &[RoleRow]) -> Vec<MemberRow> {
    use crate::core::rql::privileges::{NOINHERIT, NOSET};
    let oid = |name: &str| roles.iter().find(|r| r.name == name).map(|r| r.oid);
    acl.iter()
        .filter(|a| a.privilege == "MEMBER")
        .filter_map(|a| {
            let role = a.relation.strip_prefix("@role:")?;
            let off = |companion: &str| {
                acl.iter().any(|x| x.privilege == companion && x.relation == a.relation && x.grantee == a.grantee)
            };
            Some(MemberRow {
                roleid: oid(role)?,
                member: oid(&a.grantee)?,
                grantor: oid(&a.grantor).unwrap_or(BOOTSTRAP_ROLE),
                admin: a.grantable,
                inherit: !off(NOINHERIT),
                set: !off(NOSET),
            })
        })
        .collect()
}

/// The roles `rdb_roles` holds, with the bootstrap superuser's oid given to
/// the first superuser; one `postgres` superuser when there is no catalogue
/// (a server without authentication).
async fn roles(database: &Arc<Database>, acl: &[crate::core::rql::privileges::AclRow]) -> Vec<RoleRow> {
    use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};
    let mut out: Vec<RoleRow> = Vec::new();
    if database.has_table(crate::auth::ROLE_TABLE) {
        let sql = format!(
            "SELECT name, can_login, is_superuser FROM {}",
            crate::auth::ROLE_TABLE
        );
        if let Some(purpose) = recognize_query_purpose(&sql) {
            let response = executor::execute_as(
                crate::core::mvcc::owner::OwnerKind::Auth,
                purpose,
                database.clone(),
            )
            .await;
            if response.first() == Some(&2)
                && let Ok(rows) = crate::core::row::row::vec_into_rows(&response[1..])
            {
                for row in rows {
                    let text = |i: usize| {
                        row.columns
                            .get(i)
                            .map(|c| String::from_utf8_lossy(c.data.into_slice()).to_string())
                    };
                    let flag = |i: usize| {
                        row.columns
                            .get(i)
                            .is_some_and(|c| c.data.into_slice().first() == Some(&1))
                    };
                    let Some(name) = text(0) else { continue };
                    out.push(RoleRow {
                        oid: oid_of(&name),
                        name,
                        login: flag(1),
                        superuser: flag(2),
                        create_role: false,
                        create_db: false,
                        conn_limit: -1,
                        valid_until_us: None,
                        config: Vec::new(),
                        bypass_rls: false,
                    });
                }
            }
        }
    }
    match out.iter_mut().find(|r| r.superuser) {
        Some(first) => first.oid = BOOTSTRAP_ROLE,
        None => out.insert(
            0,
            RoleRow {
                oid: BOOTSTRAP_ROLE,
                name: "postgres".to_string(),
                superuser: true,
                login: true,
                create_role: true,
                create_db: true,
                conn_limit: -1,
                valid_until_us: None,
                config: Vec::new(),
                bypass_rls: true,
            },
        ),
    }
    // The attributes kept beside the role catalogue (`auth::RoleAttrs`).
    for a in acl.iter().filter(|a| a.relation == crate::auth::ROLE_ATTR_RELATION) {
        let Some(r) = out.iter_mut().find(|r| r.name == a.grantee) else { continue };
        match a.privilege.as_str() {
            "CREATEDB" => r.create_db = true,
            "CREATEROLE" => r.create_role = true,
            "BYPASSRLS" => r.bypass_rls = true,
            "CONNECTION LIMIT" => r.conn_limit = a.grantor.parse().unwrap_or(-1),
            "VALID UNTIL" => r.valid_until_us = a.grantor.parse().ok(),
            p => {
                if let Some(param) = p.strip_prefix("SET:") {
                    r.config.push(format!("{param}={}", a.grantor));
                }
            }
        }
    }
    out
}

/// A type name as a routine declares it, as an oid.
pub(crate) fn type_oid_by_name(name: &str) -> i64 {
    let lower = name.trim().to_ascii_lowercase();
    let (base, array) = match lower.strip_suffix("[]") {
        Some(b) => (b.trim().to_string(), true),
        None => (lower, false),
    };
    // Without its modifier, wherever it is written: `numeric(10,2)`,
    // `timestamp(3) with time zone`.
    let base = match (base.find('('), base.find(')')) {
        (Some(open), Some(close)) if open < close => {
            format!("{} {}", base[..open].trim_end(), base[close + 1..].trim_start()).trim().to_string()
        }
        _ => base,
    };
    let canonical = match base.as_str() {
        "integer" | "int" | "int4" => "int4",
        "bigint" | "int8" => "int8",
        "smallint" | "int2" => "int2",
        "boolean" | "bool" => "bool",
        "real" | "float4" => "float4",
        "double precision" | "float8" | "float" => "float8",
        "character varying" | "varchar" => "varchar",
        "character" | "char" | "bpchar" => "bpchar",
        "decimal" | "numeric" => "numeric",
        "timestamp" | "timestamp without time zone" => "timestamp",
        "timestamptz" | "timestamp with time zone" => "timestamptz",
        "time" | "time without time zone" => "time",
        "timetz" | "time with time zone" => "timetz",
        other => {
            return BUILTIN_TYPES
                .iter()
                .find(|b| b.1 == other)
                .map_or(0, |b| if array { b.5 } else { b.0 });
        }
    };
    BUILTIN_TYPES
        .iter()
        .find(|b| b.1 == canonical)
        .map_or(0, |b| if array { b.5 } else { b.0 })
}

async fn procs(
    database: &Arc<Database>,
    tx: Option<crate::core::mvcc::transaction::TxId>,
) -> Vec<ProcRow> {
    let routines = crate::core::rql::functions::load(database, None, tx)
        .await
        .unwrap_or_default();
    routines
        .into_iter()
        .map(|r| {
            let arguments = r
                .arg_names
                .iter()
                .zip(r.arg_types.iter())
                .map(|(n, ty)| {
                    if n.is_empty() {
                        ty.clone()
                    } else {
                        format!("{n} {ty}")
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            let identity = r.arg_types.join(",");
            let procedure = matches!(r.kind, crate::core::rql::functions::RoutineKind::Procedure);
            ProcRow {
                oid: oid_of(&format!("{}({identity})", r.name)),
                name: r.name.clone(),
                kind: if procedure { "p" } else { "f" },
                result: if r.returns_set {
                    format!("SETOF {}", r.return_type)
                } else {
                    r.return_type.clone()
                },
                result_oid: if procedure {
                    0
                } else {
                    type_oid_by_name(&r.return_type)
                },
                arguments,
                arg_oids: r.arg_types.iter().map(|t| type_oid_by_name(t)).collect(),
                returns_set: r.returns_set,
                language: r.language.clone(),
                body: r.body.clone(),
            }
        })
        .collect()
}

// ------------------------------------------------------------------ lookup

/// The model each database's last catalogue statement built, for the
/// functions that read the catalogue by oid while it runs.
static MODELS: Mutex<Option<HashMap<u64, StdArc<Model>>>> = Mutex::new(None);

pub(crate) fn publish(database: u64, model: StdArc<Model>) {
    let mut guard = MODELS.lock().unwrap_or_else(|e| e.into_inner());
    guard
        .get_or_insert_with(HashMap::new)
        .insert(database, model);
}

/// The model of the statement's database (`sequence::session::database`),
/// or of the only database that has one.
fn lookup() -> Option<StdArc<Model>> {
    let guard = MODELS.lock().unwrap_or_else(|e| e.into_inner());
    let models = guard.as_ref()?;
    match crate::core::rql::sequence::session::database() {
        Some(id) => models.get(&id).cloned(),
        None if models.len() == 1 => models.values().next().cloned(),
        None => None,
    }
}

/// `pg_get_userbyid(oid)`: the role's name, or PostgreSQL's
/// `unknown (OID=n)`.
pub fn user_by_id(oid: i64) -> String {
    lookup()
        .and_then(|m| m.role_name(oid))
        .unwrap_or_else(|| match oid {
            BOOTSTRAP_ROLE => "postgres".to_string(),
            DATABASE_OWNER_ROLE => "pg_database_owner".to_string(),
            _ => format!("unknown (OID={oid})"),
        })
}

/// `pg_get_indexdef(oid[, column, pretty])`: the index's `CREATE INDEX`, or
/// with a column number, that key column's name. `None` (NULL) for an oid
/// that names no index.
pub fn index_definition(oid: i64, column: i64) -> Option<String> {
    let model = lookup()?;
    let (rel, index) = model.index(oid)?;
    if column > 0 {
        let attnum = *index.keys.get(column as usize - 1)?;
        return rel
            .columns
            .iter()
            .find(|c| c.attnum == attnum)
            .map(|c| quote_ident(&c.name));
    }
    Some(index.definition.clone())
}

/// `pg_get_constraintdef(oid)`.
pub fn constraint_definition(oid: i64) -> Option<String> {
    lookup()?.constraint(oid).map(|c| c.definition.clone())
}

/// `pg_get_function_result(oid)`, `pg_get_function_arguments(oid)` and
/// `pg_get_function_identity_arguments(oid)`.
pub fn function_text(oid: i64, what: &str) -> Option<String> {
    let model = lookup()?;
    let p = model.procs.iter().find(|p| p.oid == oid)?;
    Some(match what {
        "result" => {
            if p.kind == "p" {
                return None;
            }
            p.result.clone()
        }
        _ => p.arguments.clone(),
    })
}

/// `pg_get_partkeydef(oid)`: a partitioned table's key as PostgreSQL
/// prints it, `HASH (tenant_id, id)`; `None` (NULL) for any other oid.
pub fn partition_key_definition(oid: i64) -> Option<String> {
    let model = lookup()?;
    let rel = model.rels.iter().find(|r| r.oid == oid && r.kind == "p")?;
    let names: Vec<String> = rel
        .partition_key
        .iter()
        .filter_map(|n| rel.columns.iter().find(|c| c.attnum == *n).map(|c| key_ident(&c.name)))
        .collect();
    Some(format!("HASH ({})", names.join(", ")))
}

/// A key column's name as `pg_get_partkeydef` prints it: bare when it is a
/// plain lower-case identifier, quoted otherwise. Reserved words are not
/// told apart; a key column named `user` prints bare.
fn key_ident(name: &str) -> String {
    let plain = name.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
        && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '$');
    if plain { name.to_string() } else { quote_ident(name) }
}

/// A relation's name by its oid (`oid::regclass` as text), or the number
/// for an oid that names none, as PostgreSQL prints it.
pub fn regclass_name(oid: i64) -> String {
    lookup()
        .and_then(|m| m.relation_name(oid))
        .unwrap_or_else(|| oid.to_string())
}

// ------------------------------------------------------------------ relations

pub(crate) const NEW_VIEWS: &[&str] = &[
    "pg_partitioned_table",
    "pg_type",
    "pg_proc",
    "pg_range",
    "pg_enum",
    "pg_attribute",
    "pg_attrdef",
    "pg_constraint",
    "pg_am",
    "pg_roles",
    "pg_authid",
    "pg_auth_members",
    "pg_default_acl",
    "pg_user",
    "pg_collation",
    "pg_description",
    "pg_tablespace",
    "pg_policy",
    "pg_statistic_ext",
    "pg_publication",
    "pg_publication_rel",
    "pg_publication_namespace",
    "pg_trigger",
    "pg_rewrite",
    "pg_depend",
    "pg_extension",
    // `information_schema`'s constraint views, built from `pg_constraint`'s
    // rows: what an ORM's schema reader joins to find the keys.
    "table_constraints",
    "table_privileges",
    "role_table_grants",
    "usage_privileges",
    "routine_privileges",
    "key_column_usage",
    "referential_constraints",
];

/// The columns of the relations this module builds, and of those
/// `catalog_views` builds from the model.
pub(crate) fn columns_of(view: &str) -> Option<&'static [(&'static str, DbType)]> {
    use DbType::{ARRAY, BOOL, DATETIME, I16, I32, I64, STRING};
    // `int2[]`: `DbType::I16` is element code 2.
    const INT2_ARRAY: DbType = DbType::ARRAYOF(2);
    Some(match view {
        "pg_type" => &[
            ("oid", I64),
            ("typname", STRING),
            ("typnamespace", I64),
            ("typowner", I64),
            ("typlen", I16),
            ("typbyval", BOOL),
            ("typtype", STRING),
            ("typcategory", STRING),
            ("typispreferred", BOOL),
            ("typisdefined", BOOL),
            ("typdelim", STRING),
            ("typrelid", I64),
            ("typelem", I64),
            ("typarray", I64),
            ("typinput", STRING),
            ("typoutput", STRING),
            ("typreceive", I64),
            ("typsend", I64),
            ("typalign", STRING),
            ("typstorage", STRING),
            ("typnotnull", BOOL),
            ("typbasetype", I64),
            ("typtypmod", I32),
            ("typndims", I32),
            ("typcollation", I64),
            ("typdefault", STRING),
        ],
        "pg_proc" => &[
            ("oid", I64),
            ("proname", STRING),
            ("pronamespace", I64),
            ("proowner", I64),
            ("prolang", I64),
            ("prokind", STRING),
            ("prorettype", I64),
            ("proretset", BOOL),
            ("pronargs", I16),
            ("proargtypes", STRING),
            ("provolatile", STRING),
            ("prosrc", STRING),
            ("prosecdef", BOOL),
            ("proisstrict", BOOL),
        ],
        "pg_range" => &[
            ("rngtypid", I64),
            ("rngsubtype", I64),
            ("rngmultitypid", I64),
            ("rngcollation", I64),
            ("rngsubopc", I64),
            ("rngcanonical", STRING),
            ("rngsubdiff", STRING),
        ],
        "pg_enum" => &[
            ("oid", I64),
            ("enumtypid", I64),
            ("enumsortorder", DbType::F64),
            ("enumlabel", STRING),
        ],
        "pg_attribute" => &[
            ("attrelid", I64),
            ("attname", STRING),
            ("atttypid", I64),
            ("attlen", I16),
            ("attnum", I16),
            ("atttypmod", I32),
            ("attndims", I16),
            ("attnotnull", BOOL),
            ("atthasdef", BOOL),
            ("atthasmissing", BOOL),
            ("attidentity", STRING),
            ("attgenerated", STRING),
            ("attisdropped", BOOL),
            ("attislocal", BOOL),
            ("attinhcount", I16),
            ("attcollation", I64),
            ("attstattarget", I16),
            // What `psql`'s `\d+` selects: `p` (plain) for a fixed-width
            // type, `x` (extended) for a variable one, as PostgreSQL's
            // defaults. `attcompression` names the method a column
            // compresses with (`zstd(9)`), empty for none.
            ("attstorage", STRING),
            ("attcompression", STRING),
        ],
        "pg_attrdef" => &[
            ("oid", I64),
            ("adrelid", I64),
            ("adnum", I16),
            ("adbin", STRING),
        ],
        "table_privileges" | "role_table_grants" => &[
            ("grantor", STRING),
            ("grantee", STRING),
            ("table_catalog", STRING),
            ("table_schema", STRING),
            ("table_name", STRING),
            ("privilege_type", STRING),
            ("is_grantable", STRING),
            ("with_hierarchy", STRING),
        ],
        "usage_privileges" => &[
            ("grantor", STRING),
            ("grantee", STRING),
            ("object_catalog", STRING),
            ("object_schema", STRING),
            ("object_name", STRING),
            ("object_type", STRING),
            ("privilege_type", STRING),
            ("is_grantable", STRING),
        ],
        "routine_privileges" => &[
            ("grantor", STRING),
            ("grantee", STRING),
            ("specific_catalog", STRING),
            ("specific_schema", STRING),
            ("specific_name", STRING),
            ("routine_catalog", STRING),
            ("routine_schema", STRING),
            ("routine_name", STRING),
            ("privilege_type", STRING),
            ("is_grantable", STRING),
        ],
        "table_constraints" => &[
            ("constraint_catalog", STRING),
            ("constraint_schema", STRING),
            ("constraint_name", STRING),
            ("table_catalog", STRING),
            ("table_schema", STRING),
            ("table_name", STRING),
            ("constraint_type", STRING),
            ("is_deferrable", STRING),
            ("initially_deferred", STRING),
            ("enforced", STRING),
            ("nulls_distinct", STRING),
        ],
        "key_column_usage" => &[
            ("constraint_catalog", STRING),
            ("constraint_schema", STRING),
            ("constraint_name", STRING),
            ("table_catalog", STRING),
            ("table_schema", STRING),
            ("table_name", STRING),
            ("column_name", STRING),
            ("ordinal_position", I32),
            ("position_in_unique_constraint", I32),
        ],
        "referential_constraints" => &[
            ("constraint_catalog", STRING),
            ("constraint_schema", STRING),
            ("constraint_name", STRING),
            ("unique_constraint_catalog", STRING),
            ("unique_constraint_schema", STRING),
            ("unique_constraint_name", STRING),
            ("match_option", STRING),
            ("update_rule", STRING),
            ("delete_rule", STRING),
        ],
        "pg_constraint" => &[
            ("oid", I64),
            ("conname", STRING),
            ("connamespace", I64),
            ("contype", STRING),
            ("condeferrable", BOOL),
            ("condeferred", BOOL),
            ("convalidated", BOOL),
            ("conrelid", I64),
            ("contypid", I64),
            ("conindid", I64),
            ("conparentid", I64),
            ("confrelid", I64),
            ("confupdtype", STRING),
            ("confdeltype", STRING),
            ("confmatchtype", STRING),
            ("conislocal", BOOL),
            ("coninhcount", I16),
            ("connoinherit", BOOL),
            ("conkey", INT2_ARRAY),
            ("confkey", INT2_ARRAY),
            ("conbin", STRING),
        ],
        "pg_am" => &[
            ("oid", I64),
            ("amname", STRING),
            ("amhandler", STRING),
            ("amtype", STRING),
        ],
        "pg_roles" => &[
            ("rolname", STRING),
            ("rolsuper", BOOL),
            ("rolinherit", BOOL),
            ("rolcreaterole", BOOL),
            ("rolcreatedb", BOOL),
            ("rolcanlogin", BOOL),
            ("rolreplication", BOOL),
            ("rolconnlimit", I32),
            ("rolpassword", STRING),
            ("rolvaliduntil", DATETIME),
            ("rolbypassrls", BOOL),
            ("rolconfig", ARRAY),
            ("oid", I64),
        ],
        // A superuser's alone (`privilege_statements::check`). `rolpassword`
        // is NULL through SQL whoever asks: the verifiers stay in the role
        // catalogue no statement reads.
        "pg_authid" => &[
            ("oid", I64),
            ("rolname", STRING),
            ("rolsuper", BOOL),
            ("rolinherit", BOOL),
            ("rolcreaterole", BOOL),
            ("rolcreatedb", BOOL),
            ("rolcanlogin", BOOL),
            ("rolreplication", BOOL),
            ("rolbypassrls", BOOL),
            ("rolconnlimit", I32),
            ("rolpassword", STRING),
            ("rolvaliduntil", DATETIME),
        ],
        "pg_default_acl" => &[
            ("oid", I64),
            ("defaclrole", I64),
            ("defaclnamespace", I64),
            ("defaclobjtype", STRING),
            ("defaclacl", ARRAY),
        ],
        "pg_auth_members" => &[
            ("oid", I64),
            ("roleid", I64),
            ("member", I64),
            ("grantor", I64),
            ("admin_option", BOOL),
            ("inherit_option", BOOL),
            ("set_option", BOOL),
        ],
        "pg_user" => &[
            ("usename", STRING),
            ("usesysid", I64),
            ("usecreatedb", BOOL),
            ("usesuper", BOOL),
            ("userepl", BOOL),
            ("usebypassrls", BOOL),
            ("passwd", STRING),
            ("valuntil", DATETIME),
            ("useconfig", ARRAY),
        ],
        "pg_collation" => &[
            ("oid", I64),
            ("collname", STRING),
            ("collnamespace", I64),
            ("collowner", I64),
            ("collprovider", STRING),
            ("collisdeterministic", BOOL),
            ("collencoding", I32),
            ("collcollate", STRING),
            ("collctype", STRING),
        ],
        "pg_description" => &[
            ("objoid", I64),
            ("classoid", I64),
            ("objsubid", I32),
            ("description", STRING),
        ],
        "pg_tablespace" => &[
            ("oid", I64),
            ("spcname", STRING),
            ("spcowner", I64),
            ("spcacl", ARRAY),
        ],
        "pg_policy" => &[
            ("oid", I64),
            ("polname", STRING),
            ("polrelid", I64),
            ("polcmd", STRING),
            ("polpermissive", BOOL),
            ("polroles", STRING),
            ("polqual", STRING),
            ("polwithcheck", STRING),
        ],
        "pg_statistic_ext" => &[
            ("oid", I64),
            ("stxrelid", I64),
            ("stxname", STRING),
            ("stxnamespace", I64),
            ("stxowner", I64),
            ("stxkind", ARRAY),
            ("stxstattarget", I32),
        ],
        "pg_publication" => &[
            ("oid", I64),
            ("pubname", STRING),
            ("pubowner", I64),
            ("puballtables", BOOL),
            ("pubinsert", BOOL),
            ("pubupdate", BOOL),
            ("pubdelete", BOOL),
            ("pubtruncate", BOOL),
            ("pubviaroot", BOOL),
        ],
        "pg_publication_rel" => &[
            ("oid", I64),
            ("prpubid", I64),
            ("prrelid", I64),
            ("prqual", STRING),
            ("prattrs", STRING),
        ],
        "pg_publication_namespace" => &[("oid", I64), ("pnpubid", I64), ("pnnspid", I64)],
        "pg_trigger" => &[
            ("oid", I64),
            ("tgrelid", I64),
            ("tgparentid", I64),
            ("tgname", STRING),
            ("tgfoid", I64),
            ("tgtype", I16),
            ("tgenabled", STRING),
            ("tgisinternal", BOOL),
            ("tgconstraint", I64),
        ],
        "pg_rewrite" => &[
            ("oid", I64),
            ("rulename", STRING),
            ("ev_class", I64),
            ("ev_type", STRING),
        ],
        "pg_depend" => &[
            ("classid", I64),
            ("objid", I64),
            ("objsubid", I32),
            ("refclassid", I64),
            ("refobjid", I64),
            ("refobjsubid", I32),
            ("deptype", STRING),
        ],
        "pg_extension" => &[
            ("oid", I64),
            ("extname", STRING),
            ("extowner", I64),
            ("extnamespace", I64),
            ("extrelocatable", BOOL),
            ("extversion", STRING),
        ],
        "pg_class" => &[
            ("oid", I64),
            ("relname", STRING),
            ("relnamespace", I64),
            ("relkind", STRING),
            ("relsize", I64),
            ("reltuples", I64),
            ("reltype", I64),
            ("reloftype", I64),
            ("relowner", I64),
            ("relam", I64),
            ("relpages", I32),
            ("relallvisible", I32),
            ("reltoastrelid", I64),
            ("reltablespace", I64),
            ("relhasindex", BOOL),
            ("relisshared", BOOL),
            ("relpersistence", STRING),
            ("relnatts", I16),
            ("relchecks", I16),
            ("relhasrules", BOOL),
            ("relhastriggers", BOOL),
            ("relhassubclass", BOOL),
            ("relrowsecurity", BOOL),
            ("relforcerowsecurity", BOOL),
            ("relispopulated", BOOL),
            ("relreplident", STRING),
            ("relispartition", BOOL),
            ("relacl", ARRAY),
            ("reloptions", ARRAY),
            ("relpartbound", STRING),
        ],
        "pg_index" => &[
            ("indexrelid", I64),
            ("indrelid", I64),
            ("indnatts", I64),
            ("indisunique", BOOL),
            ("indisprimary", BOOL),
            ("indisvalid", BOOL),
            ("indisready", BOOL),
            ("indislive", BOOL),
            ("indpred", STRING),
            ("indexprs", STRING),
            ("indnkeyatts", I16),
            ("indnullsnotdistinct", BOOL),
            ("indisexclusion", BOOL),
            ("indimmediate", BOOL),
            ("indisclustered", BOOL),
            ("indcheckxmin", BOOL),
            ("indisreplident", BOOL),
            ("indkey", INT2_ARRAY),
            // One operator class and one option word per key column, zero
            // each: a key compares by the engine's own rule for its type,
            // ascending, NULLs last. What a catalogue query needs of them is
            // that two indexes over the same columns answer the same.
            ("indclass", INT2_ARRAY),
            ("indoption", INT2_ARRAY),
            ("indcollation", INT2_ARRAY),
        ],
        // `partclass` and `partcollation` (oidvectors) as arrays, one zero
        // each: the key hashes by the engine's own rule, not an operator
        // class. No default partition, no key expressions.
        "pg_partitioned_table" => &[
            ("partrelid", I64),
            ("partstrat", STRING),
            ("partnatts", I16),
            ("partdefid", I64),
            ("partattrs", INT2_ARRAY),
            ("partclass", INT2_ARRAY),
            ("partcollation", INT2_ARRAY),
            ("partexprs", STRING),
        ],
        "pg_namespace" => &[
            ("oid", I64),
            ("nspname", STRING),
            ("nspowner", I64),
            ("nspacl", ARRAY),
        ],
        _ => return None,
    })
}

fn am_oid(method: &str) -> i64 {
    match method {
        "hash" => 405,
        "gin" => 2742,
        "gist" => 783,
        "brin" => 3580,
        _ => 403,
    }
}

/// The rows of a relation this module builds, `None` for one it does not.
pub(crate) fn rows_of(view: &str, model: &Model) -> Option<Vec<Vec<V>>> {
    Some(match view {
        "pg_type" => type_rows_with(&model.enums)
            .into_iter()
            .map(|r| {
                vec![
                    V::I(r.oid),
                    t(&r.name),
                    V::I(r.namespace),
                    V::I(BOOTSTRAP_ROLE),
                    V::I(r.len),
                    V::B(r.len > 0 && r.len <= 8),
                    t(r.typtype.to_string()),
                    t(r.category.to_string()),
                    V::B(matches!(r.oid, 16 | 25 | 701 | 1184 | 1186 | 869)),
                    V::B(true),
                    t(if r.oid == 603 { ";" } else { "," }),
                    V::I(0),
                    V::I(r.elem),
                    V::I(r.array),
                    t(format!("{}in", r.name.trim_start_matches('_'))),
                    t(format!("{}out", r.name.trim_start_matches('_'))),
                    V::I(r.receive),
                    V::I(0),
                    t(if r.len == 8 {
                        "d"
                    } else if r.len == 2 {
                        "s"
                    } else if r.len == 1 {
                        "c"
                    } else {
                        "i"
                    }),
                    t(if r.len == -1 { "x" } else { "p" }),
                    V::B(false),
                    V::I(0),
                    V::I(-1),
                    V::I(0),
                    V::I(r.collation),
                    V::N,
                ]
            })
            .collect(),
        "pg_proc" => {
            let mut rows: Vec<Vec<V>> = RECEIVE_FUNCTIONS
                .iter()
                .map(|(oid, name)| {
                    vec![
                        V::I(*oid),
                        t(*name),
                        V::I(NS_CATALOG),
                        V::I(BOOTSTRAP_ROLE),
                        V::I(12),
                        t("f"),
                        V::I(0),
                        V::B(false),
                        V::I(3),
                        t("2281 26 23"),
                        t("s"),
                        t(*name),
                        V::B(false),
                        V::B(true),
                    ]
                })
                .collect();
            for p in &model.procs {
                rows.push(vec![
                    V::I(p.oid),
                    t(&p.name),
                    V::I(NS_PUBLIC),
                    V::I(BOOTSTRAP_ROLE),
                    V::I(if p.language.eq_ignore_ascii_case("plpgsql") {
                        13_000
                    } else {
                        14
                    }),
                    t(p.kind),
                    V::I(p.result_oid),
                    V::B(p.returns_set),
                    V::I(p.arg_oids.len() as i64),
                    t(p.arg_oids
                        .iter()
                        .map(i64::to_string)
                        .collect::<Vec<_>>()
                        .join(" ")),
                    t("v"),
                    t(&p.body),
                    V::B(false),
                    V::B(false),
                ]);
            }
            rows
        }
        "pg_range" => RANGES
            .iter()
            .map(|(r, sub, multi)| {
                vec![
                    V::I(*r),
                    V::I(*sub),
                    V::I(*multi),
                    V::I(0),
                    V::I(0),
                    t("-"),
                    t("-"),
                ]
            })
            .collect(),
        "pg_attribute" => {
            let mut rows = Vec::new();
            for rel in &model.rels {
                for c in &rel.columns {
                    rows.push(vec![
                        V::I(rel.oid),
                        t(&c.name),
                        V::I(c.type_oid),
                        V::I(c.len),
                        V::I(c.attnum),
                        V::I(c.typmod),
                        V::I(i64::from(BUILTIN_TYPES.iter().any(|b| b.5 == c.type_oid))),
                        V::B(c.not_null),
                        V::B(c.default.is_some()),
                        V::B(false),
                        t(c.identity),
                        t(c.generated),
                        V::B(false),
                        V::B(true),
                        V::I(0),
                        V::I(c.collation),
                        V::I(-1),
                        t(if c.len > 0 { "p" } else { "x" }),
                        t(&c.compression),
                    ]);
                }
            }
            rows
        }
        "pg_attrdef" => model
            .rels
            .iter()
            .flat_map(|rel| {
                rel.columns.iter().filter_map(move |c| {
                    c.default.as_ref().map(|d| {
                        vec![
                            V::I(oid_of(&format!("{}.{}.def", rel.name, c.name))),
                            V::I(rel.oid),
                            V::I(c.attnum),
                            t(d),
                        ]
                    })
                })
            })
            .collect(),
        "table_privileges" | "role_table_grants" => model
            .grants
            .iter()
            .filter(|g| g.kind == "TABLE")
            .map(|g| {
                vec![
                    t(&g.grantor),
                    t(&g.grantee),
                    t(&model.database),
                    t("public"),
                    t(&g.object),
                    t(&g.privilege),
                    t(if g.grantable { "YES" } else { "NO" }),
                    t(if g.privilege == "SELECT" { "YES" } else { "NO" }),
                ]
            })
            .collect(),
        // USAGE on sequences, as PostgreSQL lists it there; their SELECT
        // and UPDATE are in no information_schema view.
        "usage_privileges" => model
            .grants
            .iter()
            .filter(|g| g.kind == "SEQUENCE" && g.privilege == "USAGE")
            .map(|g| {
                vec![
                    t(&g.grantor),
                    t(&g.grantee),
                    t(&model.database),
                    t("public"),
                    t(&g.object),
                    t("SEQUENCE"),
                    t("USAGE"),
                    t(if g.grantable { "YES" } else { "NO" }),
                ]
            })
            .collect(),
        "routine_privileges" => model
            .grants
            .iter()
            .filter(|g| matches!(g.kind, "FUNCTION" | "PROCEDURE"))
            .map(|g| {
                vec![
                    t(&g.grantor),
                    t(&g.grantee),
                    t(&model.database),
                    t("public"),
                    t(&g.object),
                    t(&model.database),
                    t("public"),
                    t(&g.object),
                    t("EXECUTE"),
                    t(if g.grantable { "YES" } else { "NO" }),
                ]
            })
            .collect(),
        "table_constraints" => {
            let mut rows = Vec::new();
            for rel in model.rels.iter().filter(|r| matches!(r.kind, "r" | "p")) {
                for c in &rel.constraints {
                    let kind = match c.contype {
                        "p" => "PRIMARY KEY",
                        "u" => "UNIQUE",
                        "f" => "FOREIGN KEY",
                        "c" => "CHECK",
                        _ => continue,
                    };
                    rows.push(vec![
                        t(&model.database),
                        t("public"),
                        t(&c.name),
                        t(&model.database),
                        t("public"),
                        t(&rel.name),
                        t(kind),
                        t("NO"),
                        t("NO"),
                        t("YES"),
                        if matches!(c.contype, "p" | "u") { t("YES") } else { V::N },
                    ]);
                }
            }
            rows
        }
        // Each column of a PRIMARY KEY, UNIQUE or FOREIGN KEY constraint,
        // in key order; a foreign key's column with the position of the
        // column it references within the referenced key.
        "key_column_usage" => {
            let mut rows = Vec::new();
            for rel in model.rels.iter().filter(|r| matches!(r.kind, "r" | "p")) {
                for c in rel.constraints.iter().filter(|c| matches!(c.contype, "p" | "u" | "f")) {
                    let referenced = (c.contype == "f").then(|| referenced_key(model, c)).flatten();
                    for (i, attnum) in c.keys.iter().enumerate() {
                        let Some(column) = rel.columns.iter().find(|col| col.attnum == *attnum) else {
                            continue;
                        };
                        let position = match (&referenced, c.foreign_keys.get(i)) {
                            (Some(key), Some(parent)) => {
                                key.keys.iter().position(|k| k == parent).map_or(V::N, |p| V::I(p as i64 + 1))
                            }
                            _ => V::N,
                        };
                        rows.push(vec![
                            t(&model.database),
                            t("public"),
                            t(&c.name),
                            t(&model.database),
                            t("public"),
                            t(&rel.name),
                            t(&column.name),
                            V::I(i as i64 + 1),
                            position,
                        ]);
                    }
                }
            }
            rows
        }
        "referential_constraints" => {
            let rule = |code: &str| match code {
                "r" => "RESTRICT",
                "c" => "CASCADE",
                "n" => "SET NULL",
                "d" => "SET DEFAULT",
                _ => "NO ACTION",
            };
            let mut rows = Vec::new();
            for rel in model.rels.iter().filter(|r| matches!(r.kind, "r" | "p")) {
                for c in rel.constraints.iter().filter(|c| c.contype == "f") {
                    let key = referenced_key(model, c);
                    let unique = |v: &str| if key.is_some() { t(v) } else { V::N };
                    rows.push(vec![
                        t(&model.database),
                        t("public"),
                        t(&c.name),
                        unique(&model.database),
                        unique("public"),
                        key.map_or(V::N, |k| t(&k.name)),
                        t(if c.match_type == "f" { "FULL" } else { "NONE" }),
                        t(rule(c.on_update)),
                        t(rule(c.on_delete)),
                    ]);
                }
            }
            rows
        }
        "pg_constraint" => model
            .rels
            .iter()
            .flat_map(|rel| {
                rel.constraints.iter().map(move |c| {
                    vec![
                        V::I(c.oid),
                        t(&c.name),
                        V::I(NS_PUBLIC),
                        t(c.contype),
                        V::B(false),
                        V::B(false),
                        V::B(true),
                        V::I(rel.oid),
                        V::I(0),
                        V::I(c.index),
                        V::I(0),
                        V::I(c.foreign_table),
                        t(if c.contype == "f" { c.on_update } else { " " }),
                        t(if c.contype == "f" { c.on_delete } else { " " }),
                        t(if c.contype == "f" { c.match_type } else { " " }),
                        V::B(true),
                        V::I(0),
                        V::B(false),
                        if c.keys.is_empty() {
                            V::N
                        } else {
                            V::A(c.keys.clone())
                        },
                        if c.foreign_keys.is_empty() {
                            V::N
                        } else {
                            V::A(c.foreign_keys.clone())
                        },
                        if c.contype == "c" {
                            t(&c.definition)
                        } else {
                            V::N
                        },
                    ]
                })
            })
            .collect(),
        "pg_am" => [
            (2, "heap", "heap_tableam_handler", "t"),
            (403, "btree", "bthandler", "i"),
            (405, "hash", "hashhandler", "i"),
            (783, "gist", "gisthandler", "i"),
            (2742, "gin", "ginhandler", "i"),
            (4000, "spgist", "spghandler", "i"),
            (3580, "brin", "brinhandler", "i"),
        ]
        .iter()
        .map(|(oid, name, handler, kind)| vec![V::I(*oid), t(*name), t(*handler), t(*kind)])
        .collect(),
        "pg_roles" => model
            .roles
            .iter()
            .map(|r| {
                vec![
                    t(&r.name),
                    V::B(r.superuser),
                    V::B(true),
                    V::B(r.superuser || r.create_role),
                    V::B(r.superuser || r.create_db),
                    V::B(r.login),
                    V::B(r.superuser),
                    V::I(r.conn_limit),
                    t("********"),
                    r.valid_until_us.map_or(V::N, |us| t(crate::core::value_codec::format_timestamp_us(us))),
                    V::B(r.superuser || r.bypass_rls),
                    if r.config.is_empty() { V::N } else { V::TA(r.config.clone()) },
                    V::I(r.oid),
                ]
            })
            .collect(),
        "pg_authid" => model
            .roles
            .iter()
            .map(|r| {
                vec![
                    V::I(r.oid),
                    t(&r.name),
                    V::B(r.superuser),
                    V::B(true),
                    V::B(r.superuser || r.create_role),
                    V::B(r.superuser || r.create_db),
                    V::B(r.login),
                    V::B(r.superuser),
                    V::B(r.superuser || r.bypass_rls),
                    V::I(r.conn_limit),
                    V::N,
                    r.valid_until_us.map_or(V::N, |us| t(crate::core::value_codec::format_timestamp_us(us))),
                ]
            })
            .collect(),
        "pg_default_acl" => model
            .default_acls
            .iter()
            .enumerate()
            .map(|(i, (role, kind, items))| {
                vec![V::I(DEFAULT_ACL_OID_BASE + i as i64), V::I(*role), V::I(0), t(*kind), V::TA(items.clone())]
            })
            .collect(),
        "pg_auth_members" => model
            .memberships
            .iter()
            .enumerate()
            .map(|(i, m)| {
                vec![
                    V::I(AUTH_MEMBERS_OID_BASE + i as i64),
                    V::I(m.roleid),
                    V::I(m.member),
                    V::I(m.grantor),
                    V::B(m.admin),
                    V::B(m.inherit),
                    V::B(m.set),
                ]
            })
            .collect(),
        "pg_user" => model
            .roles
            .iter()
            .filter(|r| r.login)
            .map(|r| {
                vec![
                    t(&r.name),
                    V::I(r.oid),
                    V::B(r.superuser || r.create_db),
                    V::B(r.superuser),
                    V::B(r.superuser),
                    V::B(r.superuser),
                    t("********"),
                    r.valid_until_us.map_or(V::N, |us| t(crate::core::value_codec::format_timestamp_us(us))),
                    V::N,
                ]
            })
            .collect(),
        "pg_collation" => [(100, "default", "d"), (950, "C", "c"), (951, "POSIX", "c")]
            .iter()
            .map(|(oid, name, provider)| {
                let locale = if *oid == 100 { V::N } else { t(*name) };
                vec![
                    V::I(*oid),
                    t(*name),
                    V::I(NS_CATALOG),
                    V::I(BOOTSTRAP_ROLE),
                    t(*provider),
                    V::B(true),
                    V::I(-1),
                    locale.clone(),
                    locale,
                ]
            })
            .collect(),
        "pg_tablespace" => vec![
            vec![V::I(1663), t("pg_default"), V::I(BOOTSTRAP_ROLE), V::N],
            vec![V::I(1664), t("pg_global"), V::I(BOOTSTRAP_ROLE), V::N],
        ],
        "pg_namespace" => vec![
            vec![
                V::I(NS_CATALOG),
                t("pg_catalog"),
                V::I(BOOTSTRAP_ROLE),
                V::N,
            ],
            // Owned by `pg_database_owner`, as PostgreSQL 15 on creates it.
            vec![
                V::I(NS_PUBLIC),
                t("public"),
                V::I(DATABASE_OWNER_ROLE),
                V::N,
            ],
            vec![
                V::I(NS_INFORMATION_SCHEMA),
                t("information_schema"),
                V::I(BOOTSTRAP_ROLE),
                V::N,
            ],
        ],
        "pg_class" => {
            let mut rows = Vec::new();
            for rel in &model.rels {
                rows.push(vec![
                    V::I(rel.oid),
                    t(&rel.name),
                    V::I(NS_PUBLIC),
                    t(rel.kind),
                    V::I(rel.bytes),
                    V::I(rel.tuples),
                    V::I(0),
                    V::I(0),
                    V::I(rel.owner),
                    V::I(if matches!(rel.kind, "r" | "m") { 2 } else { 0 }),
                    V::I(rel.bytes / 8192),
                    V::I(0),
                    V::I(0),
                    V::I(0),
                    V::B(!rel.indexes.is_empty()),
                    V::B(false),
                    t("p"),
                    V::I(rel.columns.len() as i64),
                    V::I(rel.constraints.iter().filter(|c| c.contype == "c").count() as i64),
                    V::B(false),
                    V::B(false),
                    V::B(false),
                    V::B(rel.rls.0),
                    V::B(rel.rls.1),
                    V::B(true),
                    t(if matches!(rel.kind, "r" | "p") { "d" } else { "n" }),
                    V::B(false),
                    if rel.acl.is_empty() { V::N } else { V::TA(rel.acl.clone()) },
                    if rel.options.is_empty() { V::N } else { V::TA(rel.options.clone()) },
                    V::N,
                ]);
                for index in &rel.indexes {
                    rows.push(vec![
                        V::I(index.oid),
                        t(&index.name),
                        V::I(NS_PUBLIC),
                        t("i"),
                        V::I(0),
                        V::I(rel.tuples),
                        V::I(0),
                        V::I(0),
                        V::I(rel.owner),
                        V::I(am_oid(index.method)),
                        V::I(0),
                        V::I(0),
                        V::I(0),
                        V::I(0),
                        V::B(false),
                        V::B(false),
                        t("p"),
                        V::I(index.keys.len() as i64),
                        V::I(0),
                        V::B(false),
                        V::B(false),
                        V::B(false),
                        V::B(false),
                        V::B(false),
                        V::B(true),
                        t("n"),
                        V::B(false),
                        V::N,
                        V::N,
                        V::N,
                    ]);
                }
            }
            rows
        }
        "pg_index" => model
            .rels
            .iter()
            .flat_map(|rel| {
                rel.indexes.iter().map(move |i| {
                    vec![
                        V::I(i.oid),
                        V::I(rel.oid),
                        V::I(i.keys.len() as i64),
                        V::B(i.unique),
                        V::B(i.primary),
                        V::B(true),
                        V::B(i.ready),
                        V::B(true),
                        // No index here is partial or over an expression
                        // (a partial unique index is a constraint): NULL, as
                        // PostgreSQL has it, where the empty string made
                        // `indpred IS NOT NULL` true of every index.
                        V::N,
                        V::N,
                        V::I(i.keys.len() as i64),
                        V::B(false),
                        V::B(false),
                        V::B(true),
                        V::B(false),
                        V::B(false),
                        V::B(false),
                        V::A(i.keys.clone()),
                        V::A(vec![0; i.keys.len()]),
                        V::A(vec![0; i.keys.len()]),
                        V::A(vec![0; i.keys.len()]),
                    ]
                })
            })
            .collect(),
        "pg_partitioned_table" => model
            .rels
            .iter()
            .filter(|rel| rel.kind == "p")
            .map(|rel| {
                vec![
                    V::I(rel.oid),
                    t("h"),
                    V::I(rel.partition_key.len() as i64),
                    V::I(0),
                    V::A(rel.partition_key.clone()),
                    V::A(vec![0; rel.partition_key.len()]),
                    V::A(vec![0; rel.partition_key.len()]),
                    V::N,
                ]
            })
            .collect(),
        "pg_enum" => model
            .enums
            .iter()
            .flat_map(|e| {
                let base = 2_000_000_000 + ((e.oid() - 900_000_000) / 2 % 30_000_000) * 64;
                e.labels.iter().enumerate().map(move |(i, label)| {
                    vec![V::I(base + i as i64), V::I(e.oid()), V::I((i + 1) as i64), t(label)]
                })
            })
            .collect(),
        // Nothing of these exists here: no comments, row-security policies,
        // extended statistics, publications, rewrite rules, dependencies or
        // extensions. Present so a query that joins them gets its answer
        // rather than an error.
        // `polroles` as PostgreSQL prints the array of role oids, here
        // the names (`{public}` for PUBLIC); `polcmd` its letter.
        "pg_policy" => model
            .policies
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let relid = model.rels.iter().find(|r| r.name == p.table).map_or(0, |r| r.oid);
                let cmd = match p.command.as_str() {
                    "SELECT" => "r",
                    "INSERT" => "a",
                    "UPDATE" => "w",
                    "DELETE" => "d",
                    _ => "*",
                };
                vec![
                    V::I(POLICY_OID_BASE + i as i64),
                    t(&p.name),
                    V::I(relid),
                    t(cmd),
                    V::B(p.permissive),
                    t(format!("{{{}}}", p.roles.join(","))),
                    p.using.as_ref().map_or(V::N, t),
                    p.check.as_ref().map_or(V::N, t),
                ]
            })
            .collect(),
        "pg_description"
        | "pg_statistic_ext"
        | "pg_publication"
        | "pg_publication_rel"
        | "pg_publication_namespace"
        | "pg_trigger"
        | "pg_rewrite"
        | "pg_depend"
        | "pg_extension" => Vec::new(),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `origin_columns` is `table_columns` cut to what a RowDescription
    /// reads: the same numbers, names, types and modifiers, column for
    /// column, over NULL flags, a dropped column, typmods, defaults, an
    /// identity and a generated column.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn origin_columns_are_table_columns_cut_down() {
        use crate::core::rql::lexer_s1::recognize_query_purpose;
        let dir = std::env::temp_dir().join(format!("origin_columns_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let db = rclite::Arc::new(crate::core::database::Database::new_with_backend(&dir.to_string_lossy(), true).await);
        for sql in [
            "CREATE TABLE oc (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name VARCHAR(20), \
             price NUMERIC(10, 2) NOT NULL DEFAULT 0, at TIMESTAMP(3), gone INT, note TEXT, \
             doubled BIGINT GENERATED ALWAYS AS (id * 2) STORED, flag BOOLEAN)",
            "ALTER TABLE oc DROP COLUMN gone",
            "ALTER TABLE oc ADD COLUMN later CHAR(4)",
        ] {
            let reply = crate::core::rql::executor::execute(recognize_query_purpose(sql).unwrap(), db.clone()).await;
            assert!(reply.first() != Some(&3), "{sql}: {}", String::from_utf8_lossy(&reply));
        }
        let relation = db.relation("oc").unwrap();
        let schema = relation.schema();
        let full: Vec<OriginColumn> = table_columns(&schema)
            .into_iter()
            .map(|c| OriginColumn { attnum: c.attnum, name: c.name, type_oid: c.type_oid, typmod: c.typmod })
            .collect();
        assert_eq!(origin_columns(&schema), full);
        assert_eq!(full.len(), 8, "{full:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn format_type_writes_modifiers_as_postgresql_does() {
        assert_eq!(
            format_type(1043, Some(24)).as_deref(),
            Some("character varying(20)")
        );
        assert_eq!(
            format_type(1043, Some(-1)).as_deref(),
            Some("character varying")
        );
        assert_eq!(
            format_type(1700, Some(((10 << 16) | 2) + 4)).as_deref(),
            Some("numeric(10,2)")
        );
        assert_eq!(
            format_type(1114, Some(3)).as_deref(),
            Some("timestamp(3) without time zone")
        );
        assert_eq!(format_type(1007, None).as_deref(), Some("integer[]"));
        assert_eq!(format_type(20, Some(-1)).as_deref(), Some("bigint"));
        assert_eq!(format_type(3802, None).as_deref(), Some("jsonb"));
        assert_eq!(format_type(424242, None), None);
    }

    #[test]
    fn every_column_list_matches_its_rows() {
        let model = Model::default();
        for view in NEW_VIEWS
            .iter()
            .chain(["pg_class", "pg_index", "pg_namespace"].iter())
        {
            let columns = columns_of(view).unwrap_or_else(|| panic!("{view} has no columns"));
            for row in rows_of(view, &model).unwrap_or_else(|| panic!("{view} has no rows")) {
                assert_eq!(row.len(), columns.len(), "{view}");
            }
        }
    }

    #[test]
    fn arrays_name_their_elements_and_array_recv() {
        let rows = type_rows();
        let int4_array = rows.iter().find(|r| r.oid == 1007).unwrap();
        assert_eq!(
            (
                int4_array.name.as_str(),
                int4_array.elem,
                int4_array.receive
            ),
            ("_int4", 23, 2400)
        );
        assert_eq!(rows.iter().find(|r| r.oid == 23).unwrap().array, 1007);
        // Every oid is listed once.
        let mut oids: Vec<i64> = rows.iter().map(|r| r.oid).collect();
        oids.sort();
        oids.dedup();
        assert_eq!(oids.len(), rows.len());
    }
}
