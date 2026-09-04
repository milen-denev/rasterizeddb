//! What each PostgreSQL type becomes here, and how each value is carried over.
//!
//! Two rules shape this module.
//!
//! **Nothing is lost.** A type this engine has no equivalent for becomes
//! `TEXT` holding PostgreSQL's own rendering of the value, which is exact and
//! reversible, rather than being dropped or approximated. Every such decision
//! is recorded and reported at the end of the import.
//!
//! **Nothing is guessed.** Every value is validated with
//! [`value_codec::encode_literal`] — the same encoder the storage layer
//! uses — before a statement is built from it. This is not belt-and-braces:
//! the insert path *coerces* rather than refuses, so a `numeric` with ten
//! decimal places, a timestamp of `infinity` and an unparseable `inet` are each
//! stored as a zero, with the statement reporting success. An import that
//! turned a balance of 0.1234567891 into 0 and said `INSERT 1` would be the
//! worst thing this feature could do, so the check happens here, before the
//! value is ever handed over.

use crate::core::db_type::DbType;
use crate::core::value_codec;

/// A column of the source database, as the catalogue describes it once domains
/// have been resolved to what they are built on.
#[derive(Debug, Clone)]
pub struct SourceColumn {
    pub name: String,
    /// `pg_attribute.attnum`. Kept because a constraint names its columns by
    /// number, and a number is not a position — a dropped column leaves a gap
    /// that would silently shift every key one along.
    pub attnum: i16,
    /// `pg_type.typname` of the resolved type: `int4`, `numeric`, `_text`.
    pub type_name: String,
    /// `pg_type.typtype`: `b`ase, `e`num, `c`omposite, `r`ange, `m`ultirange,
    /// `p`seudo. Domains are resolved before they get here.
    pub type_kind: char,
    /// `pg_type.typcategory`. `A` is the authority on whether something is an
    /// array; the name beginning with an underscore is only a convention.
    pub type_category: char,
    /// The element's type name, for an array.
    pub element_type_name: Option<String>,
    /// `atttypmod`, which carries `numeric`'s precision and scale.
    pub type_modifier: i32,
    pub not_null: bool,
    /// The default expression as PostgreSQL renders it, if any.
    pub default: Option<String>,
    /// The type as PostgreSQL would print it in `\d`, for reports.
    pub formatted_type: String,
    /// For a `json`/`jsonb` column: whether every document it holds is an
    /// object or an array — the shapes this engine's native JSON column can
    /// take. A bare scalar (`42`, `"text"`, `true`, `null`) is the one it
    /// cannot, having neither fields to become columns nor elements to become
    /// rows. `None` when the question was not asked.
    pub json_shreddable: Option<bool>,
}

/// How a value's text is turned into a literal for this engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Conversion {
    /// A quoted string. The bytes are carried across unchanged.
    Text,
    /// A bare integer, range-checked against the target width.
    Integer,
    /// A quoted float. `NaN` and the infinities survive; this engine parses
    /// them as PostgreSQL prints them.
    Float,
    /// Exact fixed-point. Refused when it will not fit in nine decimal places.
    Decimal,
    /// `t` / `f`, as `COPY` writes them.
    Boolean,
    Uuid,
    Inet,
    Timestamp,
    /// `\x…` hex, which is what `bytea_output = hex` produces.
    Bytea,
    /// A PostgreSQL array literal, `{…}`, carried across as written.
    Array,
}

/// What a source column maps to.
#[derive(Debug, Clone)]
pub struct Mapping {
    pub db_type: DbType,
    /// The type as it will be written in `CREATE TABLE`.
    pub declared: String,
    pub conversion: Conversion,
    /// An expression to wrap the column in when it is selected, for the two
    /// types whose default rendering is not something that can be read back.
    pub select_cast: Option<&'static str>,
    /// Why this mapping is not the obvious one, if it is not. Shown in the
    /// import's report.
    pub note: Option<String>,
    /// The declared scale of a `numeric(p, s)`, once the planner has read it.
    ///
    /// Values are checked against *this* rather than the engine's default
    /// scale: a `numeric(38,16)` column keeps sixteen places, so a literal with
    /// sixteen is representable in it even though it is not in a plain
    /// `NUMERIC`.
    pub decimal_scale: Option<u32>,
}

impl Mapping {
    fn plain(db_type: DbType, declared: &str, conversion: Conversion) -> Self {
        Mapping {
            db_type,
            declared: declared.to_string(),
            conversion,
            select_cast: None,
            note: None,
            decimal_scale: None,
        }
    }

    fn noted(db_type: DbType, declared: &str, conversion: Conversion, note: String) -> Self {
        Mapping {
            db_type,
            declared: declared.to_string(),
            conversion,
            select_cast: None,
            note: Some(note),
            decimal_scale: None,
        }
    }
}

/// How JSON and JSONB columns are carried over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JsonMapping {
    /// Decided per column from the data: this engine's `JSONB` where every
    /// document in the column is an object, `TEXT` where any is not.
    ///
    /// The default, because it is what an application expects. A driver checks
    /// the advertised type against the Rust type it was asked for, so a `jsonb`
    /// column carried over as `TEXT` is refused outright —
    /// "Rust type Option<serde_json::Value> (as SQL type JSONB) is not
    /// compatible with SQL type TEXT" — and the application cannot read a
    /// column it wrote. Choosing per column keeps that from costing correctness
    /// on the columns that cannot be native.
    #[default]
    Auto,
    /// `TEXT` holding the document exactly as PostgreSQL rendered it.
    ///
    /// The only mapping that cannot change what a document says.
    Text,
    /// This engine's native `JSONB` column, which decomposes an object into a
    /// table of its own.
    ///
    /// Only objects survive that: a document whose top level is an array, a
    /// number or a string has nowhere to go, and the import refuses such a row
    /// rather than storing something else.
    Native,
}

impl JsonMapping {
    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(JsonMapping::Auto),
            "text" => Some(JsonMapping::Text),
            "native" | "jsonb" | "json" => Some(JsonMapping::Native),
            _ => None,
        }
    }
}

/// Decides what a column becomes.
///
/// Never fails: a type nothing here recognises becomes `TEXT` carrying
/// PostgreSQL's rendering, with a note saying so. Refusing would strand a
/// migration on one column of an extension type that the application may not
/// even read.
pub fn map_column(column: &SourceColumn, json: JsonMapping) -> Mapping {
    // The category is what PostgreSQL itself uses to decide this, and it is
    // right for arrays of user-defined types too, whose names carry no
    // underscore rule worth trusting.
    if column.type_category == 'A' {
        let element = column.element_type_name.as_deref().unwrap_or("unknown");
        // Every array is `TEXT[]`, whatever it holds. This engine stores an
        // array's elements as text and tags the value `text[]` regardless of
        // what the column was declared as — a `DOUBLE PRECISION[]` column
        // refuses its own values — so declaring the element type would be
        // declaring something untrue.
        return Mapping {
            db_type: DbType::ARRAY,
            declared: "TEXT[]".to_string(),
            conversion: Conversion::Array,
            select_cast: None,
            note: (element != "text").then(|| {
                format!(
                    "{}[] became TEXT[]: arrays are stored element-wise as text here",
                    element
                )
            }),
            decimal_scale: None,
        };
    }

    if column.type_kind == 'e' {
        return Mapping::noted(
            DbType::STRING,
            "TEXT",
            Conversion::Text,
            format!("enum {} became TEXT", column.formatted_type),
        );
    }

    if matches!(column.type_kind, 'c' | 'r' | 'm') {
        let what = match column.type_kind {
            'c' => "composite",
            'r' => "range",
            _ => "multirange",
        };
        return Mapping::noted(
            DbType::STRING,
            "TEXT",
            Conversion::Text,
            format!(
                "{} {} became TEXT holding its PostgreSQL rendering",
                what, column.formatted_type
            ),
        );
    }

    match column.type_name.as_str() {
        "bool" => Mapping::plain(DbType::BOOL, "BOOLEAN", Conversion::Boolean),

        "int2" => Mapping::plain(DbType::I16, "SMALLINT", Conversion::Integer),
        "int4" => Mapping::plain(DbType::I32, "INTEGER", Conversion::Integer),
        "int8" => Mapping::plain(DbType::I64, "BIGINT", Conversion::Integer),
        // The identifier types are unsigned 32-bit, so they need the wider
        // signed column to hold their top half.
        "oid" | "xid" | "cid" => Mapping::noted(
            DbType::I64,
            "BIGINT",
            Conversion::Integer,
            format!("{} became BIGINT", column.type_name),
        ),

        "float4" => Mapping::plain(DbType::F32, "REAL", Conversion::Float),
        "float8" => Mapping::plain(DbType::F64, "DOUBLE PRECISION", Conversion::Float),

        "numeric" => Mapping::plain(DbType::DECIMAL, "NUMERIC", Conversion::Decimal),
        // `money` prints with a currency symbol and thousands separators that
        // depend on `lc_monetary`, so it is asked for as `numeric` instead.
        "money" => Mapping {
            db_type: DbType::DECIMAL,
            declared: "NUMERIC".to_string(),
            conversion: Conversion::Decimal,
            select_cast: Some("::numeric"),
            note: Some(
                "money became NUMERIC, read as an exact amount without its currency symbol"
                    .to_string(),
            ),
            decimal_scale: None,
        },

        "uuid" => Mapping::plain(DbType::UUID, "UUID", Conversion::Uuid),
        "inet" => Mapping::plain(DbType::INET, "INET", Conversion::Inet),
        "cidr" => Mapping::noted(
            DbType::INET,
            "INET",
            Conversion::Inet,
            "cidr became INET, which stores the same address and netmask".to_string(),
        ),

        "bytea" => Mapping::plain(DbType::BYTEA, "BYTEA", Conversion::Bytea),

        "timestamp" | "timestamptz" => {
            Mapping::plain(DbType::DATETIME, "TIMESTAMP", Conversion::Timestamp)
        }
        "date" => Mapping::noted(
            DbType::DATETIME,
            "TIMESTAMP",
            Conversion::Timestamp,
            "date became TIMESTAMP at midnight; this engine has no date-only type".to_string(),
        ),

        "text" | "varchar" | "bpchar" | "name" | "citext" | "xml" => {
            Mapping::plain(DbType::STRING, "TEXT", Conversion::Text)
        }
        // `"char"` is PostgreSQL's internal one-byte type, and `bpchar` is
        // `character(n)`. Neither is this engine's `CHAR`, which holds one
        // Unicode scalar in four bytes, so both go to `TEXT` — a `character(3)`
        // stored as `CHAR` would keep three bytes of a four-byte cell and read
        // back with the tail of whatever the encoder left.
        "char" => Mapping::noted(
            DbType::STRING,
            "TEXT",
            Conversion::Text,
            "\"char\" became TEXT".to_string(),
        ),

        "json" | "jsonb" => match json {
            // Native where the data allows it, text where it does not. The
            // probe runs while the schema is read; `None` means it could not
            // be asked, and text is the answer that cannot be wrong.
            // A stored document is bytes, so there is nothing a `json` column
            // can hold that a `JSONB` one cannot — bare scalars included. The
            // shreddability probe only decides anything when the column is
            // going to be shredded, which is no longer the default.
            JsonMapping::Auto if !crate::json_shadow_tables() => Mapping::noted(
                DbType::STRING,
                "JSONB",
                Conversion::Text,
                format!("{} became a native JSONB column", column.type_name),
            ),
            JsonMapping::Auto => match column.json_shreddable {
                Some(true) => Mapping::noted(
                    DbType::STRING,
                    "JSONB",
                    Conversion::Text,
                    format!(
                        "{} became a native JSONB column: every document in it is an \
                         object or an array",
                        column.type_name
                    ),
                ),
                _ => Mapping::noted(
                    DbType::STRING,
                    "TEXT",
                    Conversion::Text,
                    format!(
                        "{} became TEXT: some documents in it are bare scalars, and the \
                         shredded mapping this server is configured for takes only objects \
                         and arrays. Without --json-shadow-tables it would be JSONB. \
                         An application reading it as JSON will need to parse the text.",
                        column.type_name
                    ),
                ),
            },
            JsonMapping::Text => Mapping::noted(
                DbType::STRING,
                "TEXT",
                Conversion::Text,
                format!(
                    "{} became TEXT holding the document verbatim; \
                     --pg-import-json native maps it to a JSONB column instead",
                    column.type_name
                ),
            ),
            JsonMapping::Native => Mapping::noted(
                DbType::STRING,
                "JSONB",
                Conversion::Text,
                format!("{} became a native JSONB column", column.type_name),
            ),
        },

        // Types with a faithful text rendering and no counterpart here. Listed
        // rather than left to the fallback so that the report says what they
        // were instead of calling them unrecognised.
        "time" | "timetz" | "interval" | "macaddr" | "macaddr8" | "bit" | "varbit" | "tsvector"
        | "tsquery" | "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle"
        | "pg_lsn" | "txid_snapshot" => Mapping::noted(
            DbType::STRING,
            "TEXT",
            Conversion::Text,
            format!(
                "{} became TEXT holding its PostgreSQL rendering",
                column.formatted_type
            ),
        ),

        other => Mapping::noted(
            DbType::STRING,
            "TEXT",
            Conversion::Text,
            format!(
                "{} is a type this import does not recognise; it became TEXT holding \
                 PostgreSQL's rendering of the value",
                other
            ),
        ),
    }
}

/// Turns one value's text into a SQL literal for this engine, or explains why
/// it cannot be carried over.
///
/// `value` is what PostgreSQL rendered, already unescaped from `COPY`'s
/// framing. `None` is a SQL null.
pub fn literal(mapping: &Mapping, value: Option<&str>) -> Result<String, String> {
    let Some(value) = value else {
        return Ok("NULL".to_string());
    };

    match mapping.conversion {
        Conversion::Text => Ok(quote(value)),

        Conversion::Boolean => match value {
            "t" | "true" | "T" | "TRUE" => Ok("TRUE".to_string()),
            "f" | "false" | "F" | "FALSE" => Ok("FALSE".to_string()),
            other => Err(format!(
                "{:?} is not a boolean; PostgreSQL writes 't' or 'f'",
                other
            )),
        },

        Conversion::Integer => {
            let parsed: i128 = value
                .parse()
                .map_err(|_| format!("{:?} is not an integer", value))?;
            let (low, high) = integer_range(&mapping.db_type);
            if parsed < low || parsed > high {
                return Err(format!(
                    "{} does not fit in {}, which holds {}..={}",
                    value, mapping.declared, low, high
                ));
            }
            // Written back from the parsed value, so a leading `+`, leading
            // zeroes or surrounding space cannot reach the statement.
            Ok(parsed.to_string())
        }

        Conversion::Float => {
            // `NaN` and `Infinity` are values PostgreSQL really stores, and
            // this engine parses both, so they pass. Anything else has to be a
            // number.
            match value {
                "NaN" | "Infinity" | "-Infinity" => Ok(quote(value)),
                _ => {
                    let parsed: f64 = value
                        .parse()
                        .map_err(|_| format!("{:?} is not a number", value))?;
                    if mapping.db_type == DbType::F32 && parsed.is_finite() {
                        // Narrowing to `real` is what PostgreSQL already did on
                        // the way in, so this only catches a value that came
                        // from somewhere else.
                        let narrowed = parsed as f32;
                        if !narrowed.is_finite() {
                            return Err(format!("{} does not fit in REAL", value));
                        }
                    }
                    Ok(quote(value))
                }
            }
        }

        Conversion::Decimal => {
            if matches!(value, "NaN" | "Infinity" | "-Infinity") {
                return Err(format!(
                    "NUMERIC '{}' has no representation here: this engine's DECIMAL is an exact \
                     fixed-point number and has no not-a-number or infinity",
                    value
                ));
            }
            let representable = match mapping.decimal_scale {
                Some(scale) => value_codec::parse_decimal_scaled(value, scale).is_some(),
                None => value_codec::encode_literal(&mapping.db_type, value).is_some(),
            };
            if representable {
                Ok(quote(value))
            } else {
                Err(format!(
                    "NUMERIC {} cannot be carried over exactly: this column keeps {} decimal \
                     places and a magnitude below about 1.7e29",
                    value,
                    mapping.decimal_scale.unwrap_or(value_codec::DECIMAL_SCALE)
                ))
            }
        }

        Conversion::Uuid => {
            checked(mapping, value).map_err(|_| format!("{:?} is not a UUID", value))
        }

        Conversion::Inet => checked(mapping, value)
            .map_err(|_| format!("{:?} is not an IP address or network", value)),

        Conversion::Timestamp => checked(mapping, value).map_err(|_| {
            format!(
                "{:?} is not a timestamp this engine can store; `infinity`, `-infinity` and dates \
                 before 1 AD have no representation here",
                value
            )
        }),

        Conversion::Bytea => {
            if !value.starts_with("\\x") {
                return Err(format!(
                    "a bytea value arrived as {:?} rather than hex; the connection sets \
                     bytea_output = hex, so this should not happen",
                    truncate(value)
                ));
            }
            checked(mapping, value).map_err(|_| "a bytea value is not valid hex".to_string())
        }

        Conversion::Array => {
            if !(value.starts_with('{') && value.ends_with('}')) {
                return Err(format!(
                    "an array value arrived as {:?}, which is not an array literal",
                    truncate(value)
                ));
            }
            checked(mapping, value)
                .map_err(|_| format!("{:?} is not an array literal", truncate(value)))
        }
    }
}

/// Quotes a literal and checks the storage layer will read it back as the same
/// value.
///
/// The check is the point. `encode_literal` is the encoder the row writer uses,
/// and it returns nothing for a literal it cannot parse — but the insert path
/// treats that as a zero rather than as an error. Catching it here is what
/// stops a value that PostgreSQL held from becoming a zero that this engine
/// reports as stored.
fn checked(mapping: &Mapping, value: &str) -> Result<String, ()> {
    match value_codec::encode_literal(&mapping.db_type, value) {
        Some(_) => Ok(quote(value)),
        None => Err(()),
    }
}

fn integer_range(db_type: &DbType) -> (i128, i128) {
    match db_type {
        DbType::I8 => (i8::MIN as i128, i8::MAX as i128),
        DbType::I16 => (i16::MIN as i128, i16::MAX as i128),
        DbType::I32 => (i32::MIN as i128, i32::MAX as i128),
        DbType::I64 => (i64::MIN as i128, i64::MAX as i128),
        DbType::U8 => (0, u8::MAX as i128),
        DbType::U16 => (0, u16::MAX as i128),
        DbType::U32 => (0, u32::MAX as i128),
        DbType::U64 => (0, u64::MAX as i128),
        _ => (i128::MIN, i128::MAX),
    }
}

/// Wraps text in single quotes, doubling any it contains.
///
/// That is the whole of SQL string escaping here, and it is enough: this
/// engine's parser reads a doubled quote as one and treats every other byte —
/// backslashes, parentheses, newlines, semicolons, comment markers — as data.
/// Each of those was tried against the executor before this was written.
pub fn quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');
    for c in value.chars() {
        if c == '\'' {
            out.push('\'');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

/// Quotes an identifier for a statement this engine will parse.
///
/// Identifiers are *not* quoted with `"…"` on the way out. This engine strips
/// surrounding double quotes from a relation name but not from a column name in
/// every position, and an unquoted lower-case name is what its parsers are
/// exercised against. A name that needs quoting to be legal is refused when the
/// table is planned instead, which says so once rather than failing later on a
/// statement nobody can read.
pub fn identifier(name: &str) -> String {
    name.to_string()
}

/// Whether a name can be used unquoted, which is the only way this engine
/// reliably reads one.
pub fn is_plain_identifier(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn truncate(value: &str) -> String {
    if value.len() <= 60 {
        return value.to_string();
    }
    let mut cut = 60;
    while cut > 0 && !value.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}…", &value[..cut])
}

/// The declared scale of a `numeric(p, s)`, from its `atttypmod`.
///
/// Returns `None` for an unconstrained `numeric`, whose values can have any
/// scale at all and so can only be checked one at a time.
pub fn numeric_scale(type_modifier: i32) -> Option<i32> {
    if type_modifier < 4 {
        return None;
    }
    // `atttypmod` is `((precision << 16) | scale) + 4`.
    Some((type_modifier - 4) & 0xffff)
}

/// The declared precision of a `numeric(p, s)`.
pub fn numeric_precision(type_modifier: i32) -> Option<i32> {
    if type_modifier < 4 {
        return None;
    }
    Some(((type_modifier - 4) >> 16) & 0xffff)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(type_name: &str) -> SourceColumn {
        SourceColumn {
            name: "c".to_string(),
            attnum: 1,
            type_name: type_name.to_string(),
            type_kind: 'b',
            type_category: 'X',
            element_type_name: None,
            type_modifier: -1,
            not_null: false,
            default: None,
            formatted_type: type_name.to_string(),
            json_shreddable: None,
        }
    }

    fn mapping_of(type_name: &str) -> Mapping {
        map_column(&column(type_name), JsonMapping::Text)
    }

    #[test]
    fn the_ordinary_types_map_to_their_counterparts() {
        for (pg, declared, db_type) in [
            ("bool", "BOOLEAN", DbType::BOOL),
            ("int2", "SMALLINT", DbType::I16),
            ("int4", "INTEGER", DbType::I32),
            ("int8", "BIGINT", DbType::I64),
            ("float4", "REAL", DbType::F32),
            ("float8", "DOUBLE PRECISION", DbType::F64),
            ("numeric", "NUMERIC", DbType::DECIMAL),
            ("text", "TEXT", DbType::STRING),
            ("varchar", "TEXT", DbType::STRING),
            ("uuid", "UUID", DbType::UUID),
            ("inet", "INET", DbType::INET),
            ("bytea", "BYTEA", DbType::BYTEA),
            ("timestamptz", "TIMESTAMP", DbType::DATETIME),
        ] {
            let m = mapping_of(pg);
            assert_eq!(m.declared, declared, "{}", pg);
            assert_eq!(m.db_type, db_type, "{}", pg);
        }
    }

    #[test]
    fn an_unknown_type_becomes_text_rather_than_failing() {
        let m = mapping_of("geography");
        assert_eq!(m.db_type, DbType::STRING);
        assert!(m.note.as_deref().unwrap().contains("does not recognise"));
    }

    #[test]
    fn every_array_becomes_a_text_array() {
        let mut c = column("_int4");
        c.type_category = 'A';
        c.element_type_name = Some("int4".to_string());
        let m = map_column(&c, JsonMapping::Text);
        assert_eq!(m.declared, "TEXT[]");
        assert_eq!(m.db_type, DbType::ARRAY);
    }

    #[test]
    fn json_is_text_unless_asked_otherwise() {
        assert_eq!(mapping_of("jsonb").declared, "TEXT");
        assert_eq!(
            map_column(&column("jsonb"), JsonMapping::Native).declared,
            "JSONB"
        );
    }

    // ---- values ----------------------------------------------------------

    fn lit(type_name: &str, value: &str) -> Result<String, String> {
        literal(&mapping_of(type_name), Some(value))
    }

    #[test]
    fn a_null_is_a_null_whatever_the_type() {
        for pg in ["int4", "text", "numeric", "uuid", "timestamptz"] {
            assert_eq!(literal(&mapping_of(pg), None).unwrap(), "NULL");
        }
    }

    #[test]
    fn text_is_quoted_and_its_own_quotes_doubled() {
        assert_eq!(lit("text", "plain").unwrap(), "'plain'");
        assert_eq!(lit("text", "it's").unwrap(), "'it''s'");
        assert_eq!(lit("text", "").unwrap(), "''");
        // The shapes that have broken this engine's parser before.
        assert_eq!(lit("text", "a),b(").unwrap(), "'a),b('");
        assert_eq!(lit("text", "-- comment").unwrap(), "'-- comment'");
        assert_eq!(lit("text", "back\\slash").unwrap(), "'back\\slash'");
        assert_eq!(lit("text", "ünïcødé 😀").unwrap(), "'ünïcødé 😀'");
    }

    #[test]
    fn booleans_come_across_as_postgres_writes_them() {
        assert_eq!(lit("bool", "t").unwrap(), "TRUE");
        assert_eq!(lit("bool", "f").unwrap(), "FALSE");
        assert!(lit("bool", "maybe").is_err());
    }

    #[test]
    fn an_integer_too_wide_for_its_column_is_refused() {
        assert_eq!(lit("int2", "32767").unwrap(), "32767");
        assert_eq!(lit("int2", "-32768").unwrap(), "-32768");
        assert!(lit("int2", "32768").unwrap_err().contains("does not fit"));
        assert_eq!(
            lit("int8", "9223372036854775807").unwrap(),
            "9223372036854775807"
        );
        assert!(lit("int4", "notanumber").is_err());
    }

    /// The reason this module validates at all. Each of these is stored as a
    /// zero by the insert path, which then reports success.
    #[test]
    fn values_the_engine_would_silently_zero_are_refused() {
        assert!(
            lit("numeric", "0.1234567891").is_err(),
            "ten decimal places"
        );
        assert!(lit("numeric", "NaN").is_err());
        assert!(lit("numeric", "1e40").is_err(), "past the DECIMAL range");
        assert!(lit("uuid", "not-a-uuid").is_err());
        assert!(lit("inet", "garbage").is_err());
        assert!(lit("timestamptz", "infinity").is_err());
        assert!(lit("timestamptz", "0001-01-01 00:00:00 BC").is_err());
    }

    #[test]
    fn the_values_that_do_fit_are_carried_over() {
        assert_eq!(lit("numeric", "123.45").unwrap(), "'123.45'");
        assert_eq!(lit("numeric", "0.123456789").unwrap(), "'0.123456789'");
        assert_eq!(lit("numeric", "-1.5").unwrap(), "'-1.5'");
        assert_eq!(
            lit("uuid", "550e8400-e29b-41d4-a716-446655440000").unwrap(),
            "'550e8400-e29b-41d4-a716-446655440000'"
        );
        assert_eq!(lit("inet", "192.168.0.0/24").unwrap(), "'192.168.0.0/24'");
        assert_eq!(lit("inet", "2001:db8::1").unwrap(), "'2001:db8::1'");
        assert_eq!(
            lit("timestamptz", "2026-08-21 10:11:12.123456+00").unwrap(),
            "'2026-08-21 10:11:12.123456+00'"
        );
        assert_eq!(lit("date", "2026-08-21").unwrap(), "'2026-08-21'");
    }

    #[test]
    fn floats_keep_their_special_values() {
        assert_eq!(lit("float8", "1.5").unwrap(), "'1.5'");
        assert_eq!(lit("float8", "NaN").unwrap(), "'NaN'");
        assert_eq!(lit("float8", "Infinity").unwrap(), "'Infinity'");
        assert_eq!(lit("float8", "-Infinity").unwrap(), "'-Infinity'");
        assert!(lit("float8", "one and a half").is_err());
    }

    #[test]
    fn bytea_has_to_be_hex() {
        assert_eq!(lit("bytea", "\\x48656c6c6f").unwrap(), "'\\x48656c6c6f'");
        assert_eq!(lit("bytea", "\\x").unwrap(), "'\\x'");
        assert!(
            lit("bytea", "Hello")
                .unwrap_err()
                .contains("rather than hex")
        );
    }

    #[test]
    fn arrays_are_carried_across_as_written() {
        let mut c = column("_text");
        c.type_category = 'A';
        c.element_type_name = Some("text".to_string());
        let m = map_column(&c, JsonMapping::Text);
        assert_eq!(literal(&m, Some("{a,b}")).unwrap(), "'{a,b}'");
        assert_eq!(literal(&m, Some("{}")).unwrap(), "'{}'");
        assert_eq!(literal(&m, Some("{NULL,2}")).unwrap(), "'{NULL,2}'");
        assert!(literal(&m, Some("not an array")).is_err());
    }

    #[test]
    fn numeric_modifiers_are_unpacked() {
        // numeric(10, 2)
        let typmod = ((10 << 16) | 2) + 4;
        assert_eq!(numeric_precision(typmod), Some(10));
        assert_eq!(numeric_scale(typmod), Some(2));
        assert_eq!(numeric_scale(-1), None);
    }

    #[test]
    fn identifiers_are_recognised_rather_than_mangled() {
        assert!(is_plain_identifier("users"));
        assert!(is_plain_identifier("_a1"));
        assert!(!is_plain_identifier("1a"));
        assert!(!is_plain_identifier("has space"));
        assert!(!is_plain_identifier(""));
        assert!(!is_plain_identifier("Ünicode"));
    }
}
