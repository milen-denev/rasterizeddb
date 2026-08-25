//! `STORAGE ...` and `SHOW STORAGE ...` — running the storage pool from SQL.
//!
//! ```text
//! STORAGE ADD LOCATION '/mnt/disk2'
//! STORAGE ADD LOCATION '/mnt/disk3' WITH (ROLE = 'primary', WEIGHT = 2)
//! STORAGE ADD LOCATION '/mnt/backup' WITH (ROLE = 'backup')
//! STORAGE DROP LOCATION '/mnt/disk3'
//! STORAGE ENABLE LOCATION '/mnt/disk2'
//! STORAGE DISABLE LOCATION '/mnt/disk2'      -- drain: keeps its tables, takes no new ones
//! STORAGE ALTER LOCATION '/mnt/disk2' SET ROLE = 'backup'
//! STORAGE ALTER LOCATION '/mnt/disk2' SET WEIGHT = 3
//!
//! STORAGE SET POLICY = 'free-space'          -- primary | round-robin | free-space
//! STORAGE SET MIN_FREE = '2GiB'
//!
//! STORAGE PLACE TABLE users AT '/mnt/disk2'  -- pin, before the table exists
//! STORAGE MOVE TABLE users TO '/mnt/disk2'   -- copy the files and flip
//! STORAGE REBALANCE [DRY RUN]                -- move what the policy would move
//!
//! SHOW STORAGE                               -- one summary line per location
//! SHOW STORAGE STATS                         -- the same facts as (metric, value, detail)
//! SHOW STORAGE LOCATIONS                     -- a row per location
//! SHOW STORAGE PLACEMENTS                    -- a row per table
//! SHOW STORAGE POLICY
//! SHOW STORAGE KEYS                          -- what SET accepts
//! ```
//!
//! Unlike the replication surface, **these changes are persisted**. They have
//! to be: a location added at runtime and forgotten at restart would leave
//! every table created into it unfindable. The command line and the statements
//! therefore differ in role — a flag is applied on top of what the pool file
//! already says, and a statement rewrites the file.

use rclite::Arc;

use crate::core::database::Database;
use crate::core::db_type::DbType;
use crate::core::row::row::{Column, Row};

use super::{
    LocationRole, PlacementPolicy, StorageManager, human_bytes, normalize, parse_bytes,
};

/// One tunable, for `SHOW STORAGE KEYS`.
pub struct Key {
    pub name: &'static str,
    pub help: &'static str,
}

pub const KEYS: &[Key] = &[
    Key {
        name: "policy",
        help: "how a new table's directory is chosen: primary | round-robin | free-space",
    },
    Key {
        name: "min_free",
        help: "a location below this much free space is skipped (e.g. 512MB, 2GiB)",
    },
];

/// Whether this statement belongs to the storage surface.
///
/// Checked before the DDL and DML prefixes so that `SHOW STORAGE` is never read
/// as a `SHOW` of something called `STORAGE`, and before the filter surface,
/// which owns a bare `SET`.
pub fn is_storage_statement(sql: &str) -> bool {
    let upper = sql.trim().to_ascii_uppercase();
    let upper = upper.trim_end_matches(';').trim_end();

    upper == "STORAGE"
        || upper.starts_with("STORAGE ")
        || upper == "SHOW STORAGE"
        || upper.starts_with("SHOW STORAGE ")
}

/// Strips one layer of `'` or `"` quoting.
fn unquote(value: &str) -> &str {
    let value = value.trim();
    for quote in ['\'', '"'] {
        if value.len() >= 2 && value.starts_with(quote) && value.ends_with(quote) {
            return &value[1..value.len() - 1];
        }
    }
    value
}

/// Splits a statement into words, keeping a quoted run together.
///
/// A path is quoted and may contain spaces, so splitting on whitespace alone
/// would cut `'/mnt/my disk'` in half — and the halves would be created as two
/// directories rather than rejected.
fn words(statement: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;

    for ch in statement.chars() {
        match quote {
            Some(q) => {
                current.push(ch);
                if ch == q {
                    quote = None;
                    out.push(std::mem::take(&mut current));
                }
            }
            None if ch == '\'' || ch == '"' => {
                if !current.is_empty() {
                    out.push(std::mem::take(&mut current));
                }
                current.push(ch);
                quote = Some(ch);
            }
            None if ch.is_whitespace() => {
                if !current.is_empty() {
                    out.push(std::mem::take(&mut current));
                }
            }
            None => current.push(ch),
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// Reads a `WITH (KEY = value, ...)` tail, if there is one.
fn with_options(statement: &str) -> Result<Vec<(String, String)>, String> {
    let upper = statement.to_ascii_uppercase();
    let Some(at) = upper.rfind(" WITH ") else {
        return Ok(Vec::new());
    };
    let tail = statement[at + " WITH ".len()..].trim();
    let inner = tail
        .strip_prefix('(')
        .and_then(|rest| rest.strip_suffix(')'))
        .ok_or_else(|| format!("expected WITH (KEY = value, ...), found {:?}", tail))?;

    let mut out = Vec::new();
    for part in inner.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (key, value) = part
            .split_once('=')
            .ok_or_else(|| format!("expected KEY = value in WITH (...), found {:?}", part))?;
        out.push((key.trim().to_ascii_lowercase(), unquote(value).to_string()));
    }
    Ok(out)
}

/// Cuts the `WITH (...)` tail off, so the words before it can be read
/// positionally.
fn without_options(statement: &str) -> &str {
    let upper = statement.to_ascii_uppercase();
    match upper.rfind(" WITH ") {
        Some(at) => statement[..at].trim_end(),
        None => statement,
    }
}

/// The pool of the database the statement was issued against.
fn pool(database: &Arc<Database>) -> Result<&StorageManager, String> {
    if database.storage.root().is_none() {
        return Err("this database is in memory and has no storage locations".to_string());
    }
    Ok(&database.storage)
}

/// Runs a statement whose answer is lines of text.
pub async fn execute(sql: &str, database: &Arc<Database>) -> Result<Vec<String>, String> {
    let statement = sql.trim().trim_end_matches(';').trim_end();
    let bare = without_options(statement);
    let tokens = words(bare);
    let keyword = |index: usize| tokens.get(index).map(|w| w.to_ascii_uppercase()).unwrap_or_default();

    if keyword(0) == "SHOW" {
        return show(&tokens, database);
    }

    match keyword(1).as_str() {
        "ADD" => add_location(statement, &tokens, database),
        "DROP" | "REMOVE" => drop_location(&tokens, database),
        "ENABLE" => set_enabled(&tokens, database, true),
        "DISABLE" => set_enabled(&tokens, database, false),
        "ALTER" => alter_location(statement, &tokens, database),
        "SET" => set(&tokens, database),
        "PLACE" => place_table(&tokens, database),
        "MOVE" => move_table(&tokens, database).await,
        "REBALANCE" => rebalance(&tokens, database).await,
        "" => Err(help()),
        other => Err(format!(
            "unknown STORAGE statement {:?}. Try SHOW STORAGE KEYS, or one of: \
             ADD LOCATION, DROP LOCATION, ENABLE LOCATION, DISABLE LOCATION, \
             ALTER LOCATION, SET, PLACE TABLE, MOVE TABLE, REBALANCE",
            other
        )),
    }
}

fn help() -> String {
    "STORAGE needs a verb: ADD LOCATION, DROP LOCATION, ENABLE LOCATION, \
     DISABLE LOCATION, ALTER LOCATION, SET, PLACE TABLE, MOVE TABLE, REBALANCE. \
     SHOW STORAGE lists what this database has."
        .to_string()
}

/// `STORAGE ADD LOCATION '<path>' [WITH (ROLE = ..., WEIGHT = ..., ENABLED = ...)]`
fn add_location(
    statement: &str,
    tokens: &[String],
    database: &Arc<Database>,
) -> Result<Vec<String>, String> {
    let path = location_argument(tokens, 2)?;
    let mut role = LocationRole::Primary;
    let mut weight = 1u32;
    let mut enabled = true;

    for (key, value) in with_options(statement)? {
        match key.as_str() {
            "role" => {
                role = LocationRole::parse(&value)
                    .ok_or_else(|| format!("unknown ROLE {:?}: expected primary or backup", value))?
            }
            "weight" => {
                weight = value
                    .parse()
                    .map_err(|_| format!("WEIGHT must be a positive number, not {:?}", value))?
            }
            "enabled" => {
                enabled = matches!(value.to_ascii_lowercase().as_str(), "true" | "yes" | "on" | "1")
            }
            other => {
                return Err(format!(
                    "unknown option {:?}: WITH accepts ROLE, WEIGHT and ENABLED",
                    other
                ));
            }
        }
    }

    let pool = pool(database)?;
    let added = pool.add_location(&path, role, weight, enabled)?;

    let mut lines = vec![format!(
        "added {} as a {} location (weight {}, {})",
        added,
        role.as_str(),
        weight,
        if enabled { "enabled" } else { "disabled" }
    )];
    if role == LocationRole::Backup {
        // Said plainly, because the alternative is an operator believing an
        // existing table is protected when it is not. Whether a file is
        // mirrored is settled when it is opened, and a table that is already
        // open cannot have its providers swapped underneath it.
        lines.push(
            "tables created from now on are mirrored; tables already open, and the \
             catalogue itself, are mirrored from the next restart. \
             SHOW STORAGE PLACEMENTS reports which is which"
                .to_string(),
        );
    }
    Ok(lines)
}

/// `STORAGE DROP LOCATION '<path>'`
fn drop_location(tokens: &[String], database: &Arc<Database>) -> Result<Vec<String>, String> {
    let path = location_argument(tokens, 2)?;
    let removed = pool(database)?.drop_location(&path)?;
    Ok(vec![format!("{} is no longer a storage location", removed)])
}

/// `STORAGE ENABLE|DISABLE LOCATION '<path>'`
fn set_enabled(
    tokens: &[String],
    database: &Arc<Database>,
    enabled: bool,
) -> Result<Vec<String>, String> {
    let path = location_argument(tokens, 2)?;
    let changed = pool(database)?.set_enabled(&path, enabled)?;
    Ok(vec![if enabled {
        format!("{} takes new tables again", changed)
    } else {
        format!(
            "{} keeps the tables it holds and takes no new ones",
            changed
        )
    }])
}

/// `STORAGE ALTER LOCATION '<path>' SET ROLE|WEIGHT = <value>`
fn alter_location(
    statement: &str,
    tokens: &[String],
    database: &Arc<Database>,
) -> Result<Vec<String>, String> {
    let path = location_argument(tokens, 2)?;

    // `SET key = value`, with or without spaces around the `=`.
    let upper = statement.to_ascii_uppercase();
    let at = upper
        .rfind(" SET ")
        .ok_or("expected STORAGE ALTER LOCATION '<path>' SET <key> = <value>")?;
    let assignment = statement[at + " SET ".len()..].trim();
    let (key, value) = assignment
        .split_once('=')
        .ok_or_else(|| format!("expected <key> = <value>, found {:?}", assignment))?;
    let key = key.trim().to_ascii_lowercase();
    let value = unquote(value);

    let pool = pool(database)?;
    match key.as_str() {
        "role" => {
            let role = LocationRole::parse(value)
                .ok_or_else(|| format!("unknown role {:?}: expected primary or backup", value))?;
            let changed = pool.set_role(&path, role)?;
            Ok(vec![format!("{} is now a {} location", changed, role.as_str())])
        }
        "weight" => {
            let weight: u32 = value
                .trim()
                .parse()
                .map_err(|_| format!("weight must be a positive number, not {:?}", value))?;
            let changed = pool.set_weight(&path, weight)?;
            Ok(vec![format!("{} now has weight {}", changed, weight)])
        }
        other => Err(format!(
            "unknown location setting {:?}: a location has ROLE and WEIGHT",
            other
        )),
    }
}

/// `STORAGE SET POLICY = '<policy>'` / `STORAGE SET MIN_FREE = '<size>'`
fn set(tokens: &[String], database: &Arc<Database>) -> Result<Vec<String>, String> {
    // `SET k = v`, `SET k=v` and `SET k v` all arrive here; the `=` is dropped
    // and the remainder joined, so every spelling reads the same.
    let rest: Vec<String> = tokens.iter().skip(2).cloned().collect();
    if rest.is_empty() {
        return Err(format!(
            "STORAGE SET needs a key and a value. Keys: {}",
            KEYS.iter().map(|k| k.name).collect::<Vec<_>>().join(", ")
        ));
    }

    let joined = rest.join(" ");
    let (key, value) = match joined.split_once('=') {
        Some((key, value)) => (key.trim().to_string(), value.trim().to_string()),
        None => {
            let mut parts = joined.splitn(2, ' ');
            (
                parts.next().unwrap_or("").trim().to_string(),
                parts.next().unwrap_or("").trim().to_string(),
            )
        }
    };
    let value = unquote(&value).to_string();
    if value.is_empty() {
        return Err(format!("STORAGE SET {} needs a value", key));
    }

    let pool = pool(database)?;
    match key.to_ascii_lowercase().as_str() {
        "policy" => {
            let policy = PlacementPolicy::parse(&value).ok_or_else(|| {
                format!("unknown policy {:?}: expected one of {}", value, PlacementPolicy::names())
            })?;
            pool.set_policy(policy);
            let next = pool.would_place().unwrap_or_default();
            Ok(vec![
                format!("placement policy is now {}", policy.as_str()),
                format!("the next table created would go to {}", next),
            ])
        }
        "min_free" | "min_free_bytes" | "minfree" => {
            let bytes = parse_bytes(&value).ok_or_else(|| {
                format!("{:?} is not a size; write it as 512MB, 2GiB or a plain byte count", value)
            })?;
            pool.set_min_free_bytes(bytes);
            Ok(vec![format!(
                "locations with less than {} free are skipped",
                human_bytes(bytes)
            )])
        }
        other => Err(format!(
            "unknown storage setting {:?}. Keys: {}",
            other,
            KEYS.iter().map(|k| k.name).collect::<Vec<_>>().join(", ")
        )),
    }
}

/// `STORAGE PLACE TABLE <name> AT '<path>'`
fn place_table(tokens: &[String], database: &Arc<Database>) -> Result<Vec<String>, String> {
    let (table, path) = table_and_path(tokens, "AT")?;
    let pool = pool(database)?;

    if database.tables.contains_key(&table) {
        return Err(format!(
            "{} already exists and its files are in {}. Use STORAGE MOVE TABLE {} TO '{}' \
             to move them",
            table,
            pool.location_of(&table).unwrap_or_else(|| pool.root().unwrap_or("").to_string()),
            table,
            path
        ));
    }

    pool.pin(&table, &path)?;
    Ok(vec![format!(
        "{} will be created in {} when it is created",
        table,
        normalize(&path)
    )])
}

/// `STORAGE MOVE TABLE <name> TO '<path>'`
async fn move_table(tokens: &[String], database: &Arc<Database>) -> Result<Vec<String>, String> {
    let (table, path) = table_and_path(tokens, "TO")?;
    let destination = normalize(&path);
    Ok(vec![super::relocate::move_table(database, &table, &destination).await?])
}

/// `STORAGE REBALANCE [DRY RUN]`
async fn rebalance(tokens: &[String], database: &Arc<Database>) -> Result<Vec<String>, String> {
    let dry = tokens
        .iter()
        .skip(2)
        .any(|word| word.eq_ignore_ascii_case("DRY"));
    super::relocate::rebalance(database, dry).await
}

/// Reads `LOCATION '<path>'`, tolerating the keyword being left out.
fn location_argument(tokens: &[String], index: usize) -> Result<String, String> {
    let mut index = index;
    if tokens
        .get(index)
        .is_some_and(|word| word.eq_ignore_ascii_case("LOCATION"))
    {
        index += 1;
    }
    let path = tokens
        .get(index)
        .ok_or("expected a path, as LOCATION '/mnt/disk2'")?;
    Ok(unquote(path).to_string())
}

/// Reads `TABLE <name> <keyword> '<path>'`.
fn table_and_path(tokens: &[String], keyword: &str) -> Result<(String, String), String> {
    let mut index = 2;
    if tokens
        .get(index)
        .is_some_and(|word| word.eq_ignore_ascii_case("TABLE"))
    {
        index += 1;
    }
    let table = tokens
        .get(index)
        .ok_or_else(|| format!("expected a table name, as TABLE users {} '/mnt/disk2'", keyword))?;
    let table = unquote(table).trim_matches('"').to_string();

    let at = tokens
        .iter()
        .position(|word| word.eq_ignore_ascii_case(keyword))
        .ok_or_else(|| format!("expected {} '<path>'", keyword))?;
    let path = tokens
        .get(at + 1)
        .ok_or_else(|| format!("expected a path after {}", keyword))?;

    Ok((table, unquote(path).to_string()))
}

// ---------------------------------------------------------------------------
// SHOW
// ---------------------------------------------------------------------------

fn show(tokens: &[String], database: &Arc<Database>) -> Result<Vec<String>, String> {
    let what = tokens.get(2).map(|w| w.to_ascii_uppercase()).unwrap_or_default();
    let pool = pool(database)?;

    match what.as_str() {
        "" => Ok(summary(pool, database)),
        // Answered by `execute_structured`; reaching here means the caller did
        // not check `is_structured` first.
        "STATS" => Err("SHOW STORAGE STATS returns rows, not text".to_string()),
        "POLICY" => Ok(vec![
            format!("policy: {}", pool.policy().as_str()),
            format!("min free: {}", human_bytes(pool.min_free_bytes())),
            format!(
                "the next table created would go to {}",
                pool.would_place().unwrap_or_default()
            ),
        ]),
        "KEYS" => Ok(KEYS
            .iter()
            .map(|key| format!("{:<10} {}", key.name, key.help))
            .collect()),
        // `LOCATIONS` and `PLACEMENTS` return columns, not lines, and are
        // answered by `execute_structured`. Reaching here means the caller did
        // not check `is_structured` first.
        "LOCATIONS" | "PLACEMENTS" => {
            Err(format!("SHOW STORAGE {} returns rows, not text", what))
        }
        other => Err(format!(
            "unknown SHOW STORAGE {:?}: try SHOW STORAGE, SHOW STORAGE LOCATIONS, \
             SHOW STORAGE PLACEMENTS, SHOW STORAGE POLICY or SHOW STORAGE KEYS",
            other
        )),
    }
}

/// How many tables each location actually holds right now.
///
/// Counted from the live table list resolved through the placement map, not
/// from the map alone: a table created before this database had a pool file has
/// no entry and is a root table. The map's own count is a floor, and reporting
/// it as the answer would show a root directory full of tables as empty.
fn table_counts(
    pool: &StorageManager,
    database: &Arc<Database>,
) -> std::collections::HashMap<String, u64> {
    let placed: std::collections::HashMap<String, String> =
        pool.placements().into_iter().collect();
    let root = pool.root().unwrap_or("").to_string();

    let mut counts = std::collections::HashMap::new();
    for entry in database.tables.iter() {
        let at = placed.get(entry.key()).cloned().unwrap_or_else(|| root.clone());
        *counts.entry(at).or_insert(0u64) += 1;
    }
    counts
}

fn summary(pool: &StorageManager, database: &Arc<Database>) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!(
        "policy {}, min free {}",
        pool.policy().as_str(),
        human_bytes(pool.min_free_bytes())
    ));

    let counts = table_counts(pool, database);
    for report in pool.locations() {
        let tables = counts.get(&report.location.path).copied().unwrap_or(0);
        let space = match (report.available_bytes, report.total_bytes) {
            (Some(free), Some(total)) if total > 0 => format!(
                "{} free of {} ({}%)",
                human_bytes(free),
                human_bytes(total),
                (free as f64 / total as f64 * 100.0).round() as u64
            ),
            _ => "free space unknown".to_string(),
        };
        lines.push(format!(
            "{}{}: {}, weight {}, {} table(s), {}{}",
            report.location.path,
            if report.is_root { " (database directory)" } else { "" },
            report.location.role.as_str(),
            report.location.weight,
            tables,
            space,
            if report.location.enabled { "" } else { ", disabled" },
        ));
    }
    lines
}

// ---------------------------------------------------------------------------
// The structured forms
// ---------------------------------------------------------------------------
//
// A console aligns and sorts columns and colours a location that is nearly
// full; turning "12.4 GiB free of 100 GiB (12%)" back into numbers with a
// regular expression works until someone improves the wording. So the two forms
// an operator actually reads programmatically return rows, and the rest return
// prose.

#[derive(Clone, Copy, PartialEq, Eq)]
enum Structured {
    /// Pool-wide state as `(metric, value, detail)`, the same shape
    /// `SHOW REPLICATION STATS` uses.
    Stats,
    Locations,
    Placements,
}

fn structured_kind(sql: &str) -> Option<Structured> {
    let upper = sql.trim().trim_end_matches(';').trim_end().to_ascii_uppercase();
    let normalised = upper.split_whitespace().collect::<Vec<_>>().join(" ");
    match normalised.as_str() {
        "SHOW STORAGE STATS" => Some(Structured::Stats),
        "SHOW STORAGE LOCATIONS" => Some(Structured::Locations),
        "SHOW STORAGE PLACEMENTS" | "SHOW STORAGE TABLES" => Some(Structured::Placements),
        _ => None,
    }
}

/// Whether this statement is one of the structured forms.
pub fn is_structured(sql: &str) -> bool {
    structured_kind(sql).is_some()
}

/// The columns a structured form returns, or `None` for the text ones.
pub fn structured_fields(sql: &str) -> Option<Vec<(String, DbType)>> {
    let text = |name: &str| (name.to_string(), DbType::STRING);
    let number = |name: &str| (name.to_string(), DbType::U64);
    let flag = |name: &str| (name.to_string(), DbType::BOOL);

    Some(match structured_kind(sql)? {
        Structured::Stats => vec![text("metric"), text("value"), text("detail")],
        Structured::Locations => vec![
            text("location"),
            text("role"),
            number("weight"),
            flag("enabled"),
            flag("is_database_directory"),
            number("tables"),
            number("available_bytes"),
            number("total_bytes"),
        ],
        Structured::Placements => vec![text("table_name"), text("location"), flag("mirrored")],
    })
}

/// The single column the text forms return.
pub fn result_fields() -> Vec<(String, DbType)> {
    vec![("storage".to_string(), DbType::STRING)]
}

pub fn execute_structured(sql: &str, database: &Arc<Database>) -> Result<Vec<u8>, String> {
    let kind = structured_kind(sql).ok_or_else(|| format!("not a structured form: `{}`", sql))?;
    let pool = pool(database)?;

    let rows = match kind {
        Structured::Stats => stats_rows(pool, database),
        Structured::Locations => location_rows(pool, database),
        Structured::Placements => placement_rows(pool, database),
    };

    let mut out = vec![2u8];
    for row in &rows {
        row.extend_serialized(&mut out);
    }
    Ok(out)
}

/// Pool-wide state, as one row per figure.
///
/// The summary line is prose and a console would have to parse it; these are
/// the same facts as fields. Both exist for the reason `SHOW REPLICATION` has
/// both: prose is right for `psql` and a runbook, columns are right for
/// anything that sorts or colours them.
fn stats_rows(pool: &StorageManager, database: &Arc<Database>) -> Vec<Row> {
    let reports = pool.locations();
    let counts = table_counts(pool, database);

    let primaries = reports
        .iter()
        .filter(|r| r.location.role == LocationRole::Primary && r.location.enabled)
        .count();
    let backups = reports
        .iter()
        .filter(|r| r.location.role == LocationRole::Backup && r.location.enabled)
        .count();
    let disabled = reports.iter().filter(|r| !r.location.enabled).count();

    // Summed over the primaries only, and once per filesystem. Free space on a
    // backup is not space a new table can be created in; and two directories on
    // one disk report the same bytes, so adding both claims twice the room the
    // machine has — which is the number an operator decides against when asking
    // whether a database will fit.
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let free: u64 = reports
        .iter()
        .filter(|r| r.location.role == LocationRole::Primary)
        .filter(|r| {
            // Falls back to the path when the filesystem cannot be identified,
            // which counts every location separately — the same answer as
            // before, for the platform that has no better one.
            let key = super::filesystem_id(&r.location.path)
                .map(|id| format!("fs:{id}"))
                .unwrap_or_else(|| format!("path:{}", r.location.path));
            seen.insert(key)
        })
        .filter_map(|r| r.available_bytes)
        .sum();
    let tight = reports
        .iter()
        .filter(|r| r.location.role == LocationRole::Primary && r.location.enabled)
        .filter(|r| r.available_bytes.is_some_and(|free| free < pool.min_free_bytes()))
        .count();

    let mut out = Vec::new();
    let mut push = |metric: &str, value: String, detail: &str| {
        out.push(structured_row(&[
            Cell::Text(metric.to_string()),
            Cell::Text(value),
            Cell::Text(detail.to_string()),
        ]));
    };

    push(
        "policy",
        pool.policy().as_str().to_string(),
        "how a new table's directory is chosen",
    );
    push(
        "min_free_bytes",
        pool.min_free_bytes().to_string(),
        "a location below this is not chosen for new tables",
    );
    push("root", pool.root().unwrap_or("").to_string(), "the database's own directory");
    push("locations", reports.len().to_string(), "directories in the pool");
    push("primaries", primaries.to_string(), "enabled locations that hold tables");
    push("backups", backups.to_string(), "enabled locations that receive a mirrored copy");
    push("disabled", disabled.to_string(), "locations taking no new tables");
    push("tables", counts.values().sum::<u64>().to_string(), "tables this database holds");
    push(
        "free_bytes",
        free.to_string(),
        "free space across the primaries, one count per filesystem",
    );
    push(
        "locations_below_floor",
        tight.to_string(),
        "enabled primaries with less free space than the floor",
    );
    push(
        "next_placement",
        pool.would_place().unwrap_or_default(),
        "where a table created now would go",
    );
    out
}

fn location_rows(pool: &StorageManager, database: &Arc<Database>) -> Vec<Row> {
    let counts = table_counts(pool, database);
    pool.locations()
        .into_iter()
        .map(|report| {
            let tables = counts.get(&report.location.path).copied().unwrap_or(0);
            structured_row(&[
                Cell::Text(report.location.path.clone()),
                Cell::Text(report.location.role.as_str().to_string()),
                Cell::Number(report.location.weight as u64),
                Cell::Flag(report.location.enabled),
                Cell::Flag(report.is_root),
                Cell::Number(tables),
                // Unknown free space is reported as zero here rather than as
                // NULL: this column is a number and a console sorts on it. The
                // text form says "unknown" in words.
                Cell::Number(report.available_bytes.unwrap_or(0)),
                Cell::Number(report.total_bytes.unwrap_or(0)),
            ])
        })
        .collect()
}

fn placement_rows(pool: &StorageManager, database: &Arc<Database>) -> Vec<Row> {
    let root = pool.root().unwrap_or("").to_string();
    let placed: std::collections::HashMap<String, String> = pool.placements().into_iter().collect();

    let mut names: Vec<String> = database.tables.iter().map(|e| e.key().clone()).collect();
    // Placements for tables that do not exist are worth showing rather than
    // hiding: a pinned name is how `STORAGE PLACE TABLE` works before the table
    // is created, and a leftover entry is something an operator wants to see.
    for name in placed.keys() {
        if !names.contains(name) {
            names.push(name.clone());
        }
    }
    names.sort_unstable();

    // Asked of the table's own provider rather than of the pool. Whether a
    // file is mirrored is decided when it is opened, so a backup location added
    // after a table was opened does not apply to it — and a column that
    // reported the pool's intent would say a table was protected when it is
    // not. A name with no table yet (a `STORAGE PLACE TABLE` pin) has nothing
    // to ask, so it reports what it *will* get.
    let intended = !pool.mirrors().is_empty();
    names
        .into_iter()
        .map(|name| {
            let at = placed.get(&name).cloned().unwrap_or_else(|| root.clone());
            let mirrored = match database.tables.get(&name) {
                Some(table) => table.io_rows.backup_count() > 0,
                None => intended,
            };
            structured_row(&[Cell::Text(name), Cell::Text(at), Cell::Flag(mirrored)])
        })
        .collect()
}

enum Cell {
    Text(String),
    Number(u64),
    Flag(bool),
}

/// Builds one result row out of cells, in the same encoding a table row uses.
fn structured_row(cells: &[Cell]) -> Row {
    let mut columns = smallvec::SmallVec::new();
    for (index, cell) in cells.iter().enumerate() {
        let (column_type, bytes): (DbType, Vec<u8>) = match cell {
            Cell::Text(text) => (DbType::STRING, text.as_bytes().to_vec()),
            Cell::Number(value) => (DbType::U64, value.to_le_bytes().to_vec()),
            Cell::Flag(value) => (DbType::BOOL, vec![*value as u8]),
        };
        let mut block = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
        block.into_slice_mut().copy_from_slice(&bytes);
        columns.push(Column { schema_id: index as u64, data: block, column_type });
    }
    Row { position: 0, columns, length: 0 }
}

/// Wraps text lines in the result encoding a client reads as one column.
pub fn text_result(lines: &[String]) -> Vec<u8> {
    let mut result = vec![2u8];
    for line in lines {
        let mut block = crate::memory_pool::MEMORY_POOL.acquire(line.len());
        block.into_slice_mut().copy_from_slice(line.as_bytes());
        let row = Row {
            position: 0,
            columns: smallvec::smallvec![Column {
                schema_id: 0,
                data: block,
                column_type: DbType::STRING,
            }],
            length: 0,
        };
        row.extend_serialized(&mut result);
    }
    result
}
