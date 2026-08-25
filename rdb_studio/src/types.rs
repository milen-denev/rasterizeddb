//! The type catalogue behind every type picker.
//!
//! # Why a catalogue and not a text box
//!
//! A text box asks the user to remember what the server accepts, and the two
//! servers do not accept the same set. Typing `JSONB` at a RasterizedDB
//! `CREATE TABLE` works and at an `ALTER TABLE` on an older build did not;
//! typing `SERIAL` at RasterizedDB gives you an `I64` with no sequence behind
//! it, which is not what the word means anywhere else. A list is the only way
//! to be truthful about that, and it is the only place a note like "the scale
//! you write is not the scale it stores" can be attached to the thing it is
//! about.
//!
//! Every entry here is a spelling the target engine actually parses — for
//! RasterizedDB, taken from `lexer_alter::parse_column_type` and the JSON path
//! in `lexer_ct` rather than guessed.
//!
//! # What "smart" means here
//!
//! Three things:
//!
//! 1. **Only offering what works.** Filtered by dialect, so a picker never
//!    lists a type the server on the other end of the connection will refuse.
//! 2. **Asking for parameters when the type has them**, and only then — a
//!    length for `VARCHAR`, a precision and scale for `NUMERIC`.
//! 3. **Guessing from the column's name** while the user has not chosen. A
//!    column called `created_at` is a timestamp far more often than it is
//!    anything else, and `total_amount` is a decimal. The guess is a starting
//!    point, never an override.

use crate::model::Flavor;

/// Which servers offer a type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Availability {
    Both,
    RasterizedOnly,
    PostgresOnly,
}

/// What a type needs written after its name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Params {
    None,
    /// `VARCHAR(n)`. The default is what the picker pre-fills.
    Length(u32),
    /// `NUMERIC(p, s)`.
    PrecisionScale(u32, u32),
}

/// Broad groupings, so the picker can be ordered by what a person is looking
/// for rather than alphabetically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Category {
    Key,
    Number,
    Decimal,
    Text,
    Boolean,
    Time,
    Identifier,
    Network,
    Binary,
    Document,
    Array,
}

impl Category {
    pub fn label(&self) -> &'static str {
        match self {
            Category::Key => "keys",
            Category::Number => "integers",
            Category::Decimal => "approximate and exact fractions",
            Category::Text => "text",
            Category::Boolean => "booleans",
            Category::Time => "dates and times",
            Category::Identifier => "identifiers",
            Category::Network => "network addresses",
            Category::Binary => "bytes",
            Category::Document => "documents",
            Category::Array => "arrays",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlType {
    /// Exactly as it is written in DDL.
    pub name: &'static str,
    pub category: Category,
    pub params: Params,
    pub availability: Availability,
    /// One line, shown under the picker. Where the engine stores something
    /// differently from what the name implies, this is where it says so.
    pub help: &'static str,
}

impl SqlType {
    pub fn available_in(&self, flavor: Flavor) -> bool {
        match (self.availability, flavor) {
            (Availability::Both, _) => true,
            (Availability::RasterizedOnly, Flavor::Rasterized) => true,
            (Availability::PostgresOnly, Flavor::Postgres) => true,
            _ => false,
        }
    }
}

/// Every type either server accepts, in the order a picker should show them.
pub const TYPES: &[SqlType] = &[
    // --- keys
    SqlType {
        name: "BIGSERIAL",
        category: Category::Key,
        params: Params::None,
        availability: Availability::PostgresOnly,
        help: "64-bit auto-incrementing key. The usual primary key.",
    },
    SqlType {
        name: "SERIAL",
        category: Category::Key,
        params: Params::None,
        availability: Availability::PostgresOnly,
        help: "32-bit auto-incrementing key. Runs out at 2.1 billion rows.",
    },
    SqlType {
        name: "UUID",
        category: Category::Identifier,
        params: Params::None,
        availability: Availability::Both,
        help: "16 bytes, stored in network order so it sorts as its text does. \
               A v7 UUID also sorts by time, which is what makes range queries prune.",
    },
    // --- integers
    SqlType {
        name: "BIGINT",
        category: Category::Number,
        params: Params::None,
        availability: Availability::Both,
        help: "Signed 64-bit. The default for a key you supply yourself.",
    },
    SqlType {
        name: "INTEGER",
        category: Category::Number,
        params: Params::None,
        availability: Availability::Both,
        help: "Signed 32-bit.",
    },
    SqlType {
        name: "SMALLINT",
        category: Category::Number,
        params: Params::None,
        availability: Availability::Both,
        help: "Signed 16-bit.",
    },
    SqlType {
        name: "TINYINT",
        category: Category::Number,
        params: Params::None,
        availability: Availability::RasterizedOnly,
        help: "Signed 8-bit. PostgreSQL has no equivalent.",
    },
    SqlType {
        name: "UBIGINT",
        category: Category::Number,
        params: Params::None,
        availability: Availability::RasterizedOnly,
        help: "Unsigned 64-bit. Not a PostgreSQL type; a client that expects one \
               will read it as a bigint.",
    },
    SqlType {
        name: "UINT",
        category: Category::Number,
        params: Params::None,
        availability: Availability::RasterizedOnly,
        help: "Unsigned 32-bit.",
    },
    SqlType {
        name: "USMALLINT",
        category: Category::Number,
        params: Params::None,
        availability: Availability::RasterizedOnly,
        help: "Unsigned 16-bit.",
    },
    SqlType {
        name: "UTINYINT",
        category: Category::Number,
        params: Params::None,
        availability: Availability::RasterizedOnly,
        help: "Unsigned 8-bit.",
    },
    // --- fractions
    SqlType {
        name: "NUMERIC",
        category: Category::Decimal,
        params: Params::PrecisionScale(12, 2),
        availability: Availability::Both,
        help: "Exact. Use it for money — DOUBLE PRECISION cannot represent 0.10. \
               RasterizedDB stores every NUMERIC at a fixed scale of 9, so the scale \
               written here documents intent rather than changing storage.",
    },
    SqlType {
        name: "DOUBLE PRECISION",
        category: Category::Decimal,
        params: Params::None,
        availability: Availability::Both,
        help: "64-bit floating point. Fast and inexact; never for money.",
    },
    SqlType {
        name: "REAL",
        category: Category::Decimal,
        params: Params::None,
        availability: Availability::Both,
        help: "32-bit floating point.",
    },
    // --- text
    SqlType {
        name: "TEXT",
        category: Category::Text,
        params: Params::None,
        availability: Availability::Both,
        help: "Variable length, no limit. The right default for text on both servers.",
    },
    SqlType {
        name: "VARCHAR",
        category: Category::Text,
        params: Params::Length(255),
        availability: Availability::Both,
        help: "Variable length with a declared maximum. PostgreSQL enforces it; \
               RasterizedDB stores it as TEXT and does not.",
    },
    SqlType {
        name: "CHAR",
        category: Category::Text,
        params: Params::Length(1),
        availability: Availability::Both,
        help: "Fixed length, space-padded in PostgreSQL. Rarely what you want.",
    },
    // --- booleans
    SqlType {
        name: "BOOLEAN",
        category: Category::Boolean,
        params: Params::None,
        availability: Availability::Both,
        help: "One byte. Remember that a nullable boolean has three states.",
    },
    // --- time
    SqlType {
        name: "TIMESTAMPTZ",
        category: Category::Time,
        params: Params::None,
        availability: Availability::Both,
        help: "An instant, stored as microseconds from the epoch. Prefer this to \
               TIMESTAMP: a time without a zone is ambiguous the moment it crosses one.",
    },
    SqlType {
        name: "TIMESTAMP",
        category: Category::Time,
        params: Params::None,
        availability: Availability::Both,
        help: "A date and time with no zone. RasterizedDB stores it exactly as \
               TIMESTAMPTZ does, so the distinction is not preserved.",
    },
    SqlType {
        name: "DATE",
        category: Category::Time,
        params: Params::None,
        availability: Availability::Both,
        help: "A calendar day. RasterizedDB stores it as a timestamp.",
    },
    SqlType {
        name: "TIME",
        category: Category::Time,
        params: Params::None,
        availability: Availability::Both,
        help: "A time of day. RasterizedDB stores it as a timestamp.",
    },
    SqlType {
        name: "INTERVAL",
        category: Category::Time,
        params: Params::None,
        availability: Availability::PostgresOnly,
        help: "A span of time.",
    },
    // --- network
    SqlType {
        name: "INET",
        category: Category::Network,
        params: Params::None,
        availability: Availability::Both,
        help: "A host or network address, v4 or v6, sorting by family then address \
               then netmask.",
    },
    SqlType {
        name: "CIDR",
        category: Category::Network,
        params: Params::None,
        availability: Availability::Both,
        help: "A network. RasterizedDB stores it exactly as INET does.",
    },
    SqlType {
        name: "MACADDR",
        category: Category::Network,
        params: Params::None,
        availability: Availability::PostgresOnly,
        help: "A MAC address.",
    },
    // --- bytes and documents
    SqlType {
        name: "BYTEA",
        category: Category::Binary,
        params: Params::None,
        availability: Availability::Both,
        help: "Arbitrary bytes. Not indexed on RasterizedDB: it has no ordering to \
               index by.",
    },
    SqlType {
        name: "JSONB",
        category: Category::Document,
        params: Params::None,
        availability: Availability::Both,
        help: "A document. RasterizedDB maps each key onto a column of a shadow \
               table, so a field is queryable and indexable like any other column — \
               but the top level must be an object, not an array.",
    },
    SqlType {
        name: "JSON",
        category: Category::Document,
        params: Params::None,
        availability: Availability::Both,
        help: "As JSONB. PostgreSQL keeps the original text; RasterizedDB treats \
               the two identically.",
    },
    // --- arrays
    SqlType {
        name: "TEXT[]",
        category: Category::Array,
        params: Params::None,
        availability: Availability::Both,
        help: "An array of text, in PostgreSQL's binary array format. Not indexed \
               on RasterizedDB.",
    },
    SqlType {
        name: "BIGINT[]",
        category: Category::Array,
        params: Params::None,
        availability: Availability::Both,
        help: "An array of 64-bit integers.",
    },
];

/// A picker entry. Wraps a catalogue reference so `pick_list` can carry it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Choice(pub &'static SqlType);

impl std::fmt::Display for Choice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.name)
    }
}

/// Every type this server accepts, in catalogue order.
pub fn choices(flavor: Flavor) -> Vec<Choice> {
    TYPES
        .iter()
        .filter(|entry| entry.available_in(flavor))
        .map(Choice)
        .collect()
}

/// The entry a written type name refers to, if the catalogue has one.
///
/// Matches on the bare name, so `NUMERIC(12,2)` finds `NUMERIC`.
pub fn lookup(written: &str) -> Option<Choice> {
    let bare = written
        .split('(')
        .next()
        .unwrap_or(written)
        .trim()
        .to_ascii_uppercase();
    TYPES
        .iter()
        .find(|entry| entry.name.eq_ignore_ascii_case(&bare))
        .map(Choice)
}

/// The type a new column starts as.
pub fn default_for(flavor: Flavor) -> Choice {
    lookup("TEXT").unwrap_or(Choice(&TYPES[0]))
        .pick_available(flavor)
}

impl Choice {
    fn pick_available(self, flavor: Flavor) -> Choice {
        if self.0.available_in(flavor) {
            self
        } else {
            choices(flavor).first().copied().unwrap_or(self)
        }
    }
}

/// The type a primary key column starts as.
pub fn default_key(flavor: Flavor) -> Choice {
    match flavor {
        // A sequence-backed key, which is what almost every PostgreSQL table's
        // `id` is.
        Flavor::Postgres => lookup("BIGSERIAL"),
        // RasterizedDB has no sequence type: a key there is a BIGINT the client
        // supplies, or a UUID it generates.
        Flavor::Rasterized => lookup("BIGINT"),
    }
    .unwrap_or_else(|| default_for(flavor))
}

/// Guesses a type from a column's name.
///
/// A starting point, not a decision: it only ever applies while the user has
/// not picked one, and picking one turns it off for that column. The rules are
/// the conventions that actually recur in schemas — checked longest-first, so
/// `updated_at` matches the timestamp rule and not the `at` in `latitude`.
pub fn infer(column_name: &str, flavor: Flavor) -> Option<Choice> {
    let name = column_name.trim().to_ascii_lowercase();
    if name.is_empty() {
        return None;
    }

    let guess = if name == "id" {
        return Some(default_key(flavor));
    } else if name.ends_with("_id") || name.starts_with("id_") {
        // A foreign key matches the key it references, which is the key type.
        match flavor {
            Flavor::Postgres => "BIGINT",
            Flavor::Rasterized => "BIGINT",
        }
    } else if name.ends_with("_uuid") || name == "uuid" || name == "guid" {
        "UUID"
    } else if name.ends_with("_at") || name.ends_with("_on") || name.ends_with("_time")
        || name == "timestamp" || name == "created" || name == "updated"
    {
        "TIMESTAMPTZ"
    } else if name.ends_with("_date") || name == "date" || name == "birthday" || name == "dob" {
        "DATE"
    } else if name.starts_with("is_") || name.starts_with("has_") || name.starts_with("can_")
        || name.starts_with("should_") || name == "enabled" || name == "active" || name == "deleted"
    {
        "BOOLEAN"
    } else if name.ends_with("_amount") || name.ends_with("_price") || name.ends_with("_total")
        || name == "amount" || name == "price" || name == "total" || name == "balance"
        || name == "cost" || name == "vat" || name == "subtotal"
    {
        "NUMERIC"
    } else if name.ends_with("_count") || name.ends_with("_qty") || name == "count"
        || name == "quantity" || name == "qty" || name == "position" || name == "sort_order"
    {
        "INTEGER"
    } else if name.ends_with("_ip") || name == "ip" || name == "ip_address"
        || name == "client_addr"
    {
        "INET"
    } else if name.ends_with("_json") || name == "metadata" || name == "meta"
        || name == "payload" || name == "settings" || name == "attributes" || name == "config"
    {
        "JSONB"
    } else if name.ends_with("_bytes") || name == "data" || name == "blob" || name == "content_raw"
    {
        "BYTEA"
    } else if name.ends_with("_tags") || name == "tags" || name == "labels" {
        "TEXT[]"
    } else if name.ends_with("_ratio") || name.ends_with("_percent") || name.ends_with("_score") {
        "DOUBLE PRECISION"
    } else {
        "TEXT"
    };

    lookup(guess)
        .filter(|choice| choice.0.available_in(flavor))
        .or_else(|| lookup("TEXT"))
}

/// Renders a chosen type and its parameters as DDL.
///
/// A parameter that is blank or unparseable is left off rather than guessed at:
/// `NUMERIC` is valid on both servers, and `NUMERIC(,)` is valid on neither.
pub fn render(choice: Choice, length: &str, precision: &str, scale: &str) -> String {
    match choice.0.params {
        Params::None => choice.0.name.to_string(),
        Params::Length(_) => match length.trim().parse::<u32>() {
            Ok(n) if n > 0 => format!("{}({})", choice.0.name, n),
            _ => choice.0.name.to_string(),
        },
        Params::PrecisionScale(_, _) => {
            let p = precision.trim().parse::<u32>().ok().filter(|p| *p > 0);
            let s = scale.trim().parse::<u32>().ok();
            match (p, s) {
                (Some(p), Some(s)) => format!("{}({}, {})", choice.0.name, p, s),
                (Some(p), None) => format!("{}({})", choice.0.name, p),
                _ => choice.0.name.to_string(),
            }
        }
    }
}

/// The parameter values a freshly chosen type starts with.
pub fn defaults(choice: Choice) -> (String, String, String) {
    match choice.0.params {
        Params::None => (String::new(), String::new(), String::new()),
        Params::Length(n) => (n.to_string(), String::new(), String::new()),
        Params::PrecisionScale(p, s) => (String::new(), p.to_string(), s.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A picker that lists a type the server refuses is worse than a text box,
    /// because it looks authoritative.
    #[test]
    fn a_picker_only_offers_what_the_server_takes() {
        let rasterized: Vec<&str> = choices(Flavor::Rasterized)
            .iter()
            .map(|c| c.0.name)
            .collect();
        assert!(rasterized.contains(&"UBIGINT"), "the engine accepts unsigned types");
        assert!(!rasterized.contains(&"BIGSERIAL"), "there is no sequence type here");
        assert!(!rasterized.contains(&"INTERVAL"));

        let postgres: Vec<&str> = choices(Flavor::Postgres).iter().map(|c| c.0.name).collect();
        assert!(postgres.contains(&"BIGSERIAL"));
        assert!(!postgres.contains(&"UBIGINT"), "PostgreSQL has no unsigned integers");
        assert!(!postgres.contains(&"TINYINT"));
    }

    /// The two servers key their tables differently, and the default has to
    /// reflect that rather than producing DDL one of them rejects.
    #[test]
    fn the_key_default_matches_the_server() {
        assert_eq!(default_key(Flavor::Postgres).0.name, "BIGSERIAL");
        assert_eq!(default_key(Flavor::Rasterized).0.name, "BIGINT");
    }

    #[test]
    fn parameters_are_rendered_only_when_they_parse() {
        let numeric = lookup("NUMERIC").unwrap();
        assert_eq!(render(numeric, "", "12", "2"), "NUMERIC(12, 2)");
        assert_eq!(render(numeric, "", "12", ""), "NUMERIC(12)");
        // Blank is not zero, and `NUMERIC(0)` is not what an empty box means.
        assert_eq!(render(numeric, "", "", ""), "NUMERIC");
        assert_eq!(render(numeric, "", "abc", "2"), "NUMERIC");

        let varchar = lookup("VARCHAR").unwrap();
        assert_eq!(render(varchar, "255", "", ""), "VARCHAR(255)");
        assert_eq!(render(varchar, "", "", ""), "VARCHAR");
        assert_eq!(render(varchar, "0", "", ""), "VARCHAR");

        let text = lookup("TEXT").unwrap();
        assert_eq!(render(text, "255", "9", "9"), "TEXT", "TEXT takes no parameters");
    }

    #[test]
    fn a_written_type_finds_its_entry() {
        assert_eq!(lookup("numeric(12,2)").unwrap().0.name, "NUMERIC");
        assert_eq!(lookup("  TEXT  ").unwrap().0.name, "TEXT");
        assert_eq!(lookup("TEXT[]").unwrap().0.name, "TEXT[]");
        assert!(lookup("NOT_A_TYPE").is_none());
    }

    /// The guesses that actually recur in schemas.
    #[test]
    fn a_name_suggests_a_type() {
        let g = |name: &str| infer(name, Flavor::Postgres).unwrap().0.name;
        assert_eq!(g("id"), "BIGSERIAL");
        assert_eq!(g("user_id"), "BIGINT");
        assert_eq!(g("created_at"), "TIMESTAMPTZ");
        assert_eq!(g("updated_at"), "TIMESTAMPTZ");
        assert_eq!(g("birth_date"), "DATE");
        assert_eq!(g("is_active"), "BOOLEAN");
        assert_eq!(g("total_amount"), "NUMERIC");
        assert_eq!(g("login_count"), "INTEGER");
        assert_eq!(g("client_ip"), "INET");
        assert_eq!(g("metadata"), "JSONB");
        assert_eq!(g("tags"), "TEXT[]");
        assert_eq!(g("name"), "TEXT", "an unremarkable name is text");
        assert_eq!(g("session_uuid"), "UUID");
    }

    /// An inference must never produce a type the connected server refuses.
    #[test]
    fn a_guess_respects_the_dialect() {
        for name in ["id", "user_id", "created_at", "metadata", "tags", "amount", "anything"] {
            let choice = infer(name, Flavor::Rasterized).unwrap();
            assert!(
                choice.0.available_in(Flavor::Rasterized),
                "{name} guessed {}, which RasterizedDB does not accept",
                choice.0.name
            );
        }
    }

    #[test]
    fn a_blank_name_suggests_nothing() {
        assert!(infer("", Flavor::Postgres).is_none());
        assert!(infer("   ", Flavor::Postgres).is_none());
    }

    /// Every entry has to be reachable through `lookup`, or a picker can select
    /// something the renderer cannot resolve.
    /// Every suggestion has to be something the server actually evaluates.
    /// The whole reason this list exists is that a plausible-looking function
    /// name was typed and refused.
    #[test]
    fn every_suggested_default_is_one_the_engine_accepts() {
        const ACCEPTED: &[&str] = &[
            "gen_random_uuid()",
            "uuid_generate_v4()",
            "CURRENT_TIMESTAMP",
            "NOW()",
            "CURRENT_TIMESTAMP()",
            "LOCALTIMESTAMP",
            "TRUE",
            "FALSE",
            "NULL",
        ];
        for entry in DEFAULTS {
            let literal = entry.expression == "0" || entry.expression == "''";
            assert!(
                literal || ACCEPTED.contains(&entry.expression),
                "{} is not an expression the engine understands",
                entry.expression
            );
        }
        assert!(
            DEFAULT_HELP.contains("gen_random_uuid()"),
            "the help must spell the function correctly — a near-miss is what caused this"
        );
        assert!(!DEFAULT_HELP.contains("get_random_uuid"));
    }

    /// A suggestion is only useful if it fits the column it is offered for.
    #[test]
    fn suggestions_match_the_type() {
        let uuid = defaults_for(lookup("UUID").unwrap());
        assert!(uuid.iter().any(|d| d.expression == "gen_random_uuid()"));
        assert!(!uuid.iter().any(|d| d.expression == "TRUE"));

        let stamp = defaults_for(lookup("TIMESTAMPTZ").unwrap());
        assert!(stamp.iter().any(|d| d.expression == "CURRENT_TIMESTAMP"));

        // NULL fits everything.
        for name in ["UUID", "TEXT", "BIGINT", "BOOLEAN"] {
            assert!(
                defaults_for(lookup(name).unwrap())
                    .iter()
                    .any(|d| d.expression == "NULL"),
                "{name} was not offered NULL"
            );
        }
    }

    #[test]
    fn every_entry_round_trips() {
        for entry in TYPES {
            let found = lookup(entry.name)
                .unwrap_or_else(|| panic!("{} does not look itself up", entry.name));
            assert_eq!(found.0.name, entry.name);
            assert!(!entry.help.is_empty(), "{} has no help text", entry.name);
        }
    }
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

/// A `DEFAULT` expression the server will actually evaluate, offered for the
/// type it suits.
///
/// This exists because the box used to be free text and somebody typed
/// `get_random_uuid()` — a function that does not exist anywhere, one letter
/// from one that does. The engine's refusal names the right spelling, but by
/// then the dialog has closed and taken the rest of the column definition with
/// it. Offering the accepted expressions is cheaper than reporting them.
pub struct DefaultExpr {
    pub expression: &'static str,
    pub label: &'static str,
    /// Categories this is worth offering for. Empty means every type.
    pub categories: &'static [Category],
}

/// Everything RasterizedDB's `column_default` evaluator understands beyond a
/// plain literal. Taken from its parser rather than from memory: it accepts
/// `CURRENT_TIMESTAMP`, `NOW()`, `CURRENT_TIMESTAMP()`, `LOCALTIMESTAMP`,
/// `gen_random_uuid()`, `uuid_generate_v4()`, `TRUE`/`FALSE` and `NULL`.
pub const DEFAULTS: &[DefaultExpr] = &[
    DefaultExpr {
        expression: "gen_random_uuid()",
        label: "random UUID",
        categories: &[Category::Identifier],
    },
    DefaultExpr {
        expression: "CURRENT_TIMESTAMP",
        label: "now",
        categories: &[Category::Time],
    },
    DefaultExpr {
        expression: "TRUE",
        label: "true",
        categories: &[Category::Boolean],
    },
    DefaultExpr {
        expression: "FALSE",
        label: "false",
        categories: &[Category::Boolean],
    },
    DefaultExpr {
        expression: "0",
        label: "zero",
        categories: &[Category::Number, Category::Decimal],
    },
    DefaultExpr {
        expression: "''",
        label: "empty",
        categories: &[Category::Text],
    },
    DefaultExpr {
        expression: "NULL",
        label: "null",
        categories: &[],
    },
];

/// The defaults worth offering for a chosen type.
pub fn defaults_for(choice: Choice) -> Vec<&'static DefaultExpr> {
    DEFAULTS
        .iter()
        .filter(|entry| {
            entry.categories.is_empty() || entry.categories.contains(&choice.0.category)
        })
        .collect()
}

/// What the server will accept in that box, said once so nobody has to guess.
pub const DEFAULT_HELP: &str =
    "A literal, or one of: gen_random_uuid(), uuid_generate_v4(), CURRENT_TIMESTAMP, NOW(), \
     LOCALTIMESTAMP, TRUE, FALSE, NULL. Quote a text literal: 'pending'.";
