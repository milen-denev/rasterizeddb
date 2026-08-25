//! `SET` / `SHOW` / `RESET FILTER` — tuning the index from SQL.
//!
//! ```text
//! SET FILTER max_f = 20                        -- global
//! SET FILTER max_f = 20 FOR TABLE users        -- one table
//! SET FILTER max_f = 24 FOR COLUMN users.email -- one column
//! SET FILTER zone_bounds = off FOR COLUMN users.uuid
//! SET FILTER indexed = off FOR COLUMN users.blob
//!
//! SHOW FILTER                                  -- effective globals
//! SHOW FILTER FOR TABLE users                  -- as resolved for that table
//! SHOW FILTER FOR COLUMN users.email           -- as resolved for that column
//! SHOW FILTER KEYS                             -- what is tunable
//!
//! RESET FILTER                                 -- everything back to defaults
//! RESET FILTER FOR TABLE users
//! RESET FILTER FOR COLUMN users.email
//! ```
//!
//! Scopes layer, most specific winning, so a column override names only the
//! knob it changes and inherits the rest.
//!
//! A change applies to filters built afterwards: tables opened later, and
//! widths chosen at the next zone seal. Existing generations keep the width
//! they were sealed with — the same way growth already works — so tuning a
//! table that is already large takes effect from that point rather than
//! rewriting what exists. `Table::backfill_filters()` rebuilds if you want it
//! applied retroactively.

use super::settings::{Overrides, settings};

/// One tunable knob, with the scopes it accepts.
pub struct Key {
    pub name: &'static str,
    pub global_only: bool,
    pub help: &'static str,
}

pub const KEYS: &[Key] = &[
    Key { name: "enabled", global_only: true, help: "master switch (bool)" },
    Key { name: "backfill_on_open", global_only: true, help: "index pre-existing rows at open (bool)" },
    Key { name: "min_pruning_ratio", global_only: true, help: "skip the mask if it prunes less than this (0..1); overridden when the mask lets a scan start late" },
    Key { name: "checkpoint_interval_secs", global_only: true, help: "how often the maintenance task checkpoints" },
    Key { name: "indexed", global_only: false, help: "index this table/column at all (bool)" },
    Key { name: "zone_bounds", global_only: false, help: "keep per-zone min/max for range pruning (bool)" },
    Key { name: "bloom", global_only: false, help: "keep a per-superzone span summary; cuts bytes streamed, not rows fetched (bool)" },
    Key { name: "min_f", global_only: false, help: "floor on fingerprint bits (4..16)" },
    Key { name: "default_f", global_only: false, help: "fingerprint bits before cardinality is known" },
    Key { name: "max_f", global_only: false, help: "ceiling on fingerprint bits (4..16)" },
    Key { name: "min_selectivity", global_only: false, help: "largest row fraction a predicate may match and still be indexed" },
    Key { name: "min_table_rows", global_only: false, help: "tables smaller than this are not indexed" },
];

#[derive(Debug, PartialEq)]
enum Scope {
    Global,
    Table(String),
    Column(String, String),
}

fn parse_bool(v: &str) -> Result<bool, String> {
    match v.trim().to_ascii_lowercase().as_str() {
        "true" | "on" | "yes" | "1" | "t" => Ok(true),
        "false" | "off" | "no" | "0" | "f" => Ok(false),
        other => Err(format!("`{}` is not a boolean (use on/off, true/false, 1/0)", other)),
    }
}

/// Split a trailing `FOR TABLE t` / `FOR COLUMN t.c` off the statement.
fn split_scope(rest: &str) -> Result<(String, Scope), String> {
    let upper = rest.to_ascii_uppercase();
    let Some(at) = upper.rfind(" FOR ") else {
        return Ok((rest.trim().to_string(), Scope::Global));
    };
    let (head, tail) = (rest[..at].trim().to_string(), rest[at + 5..].trim());
    let mut it = tail.split_whitespace();
    let kind = it.next().unwrap_or("");
    let name = it.next().unwrap_or("").trim_matches('"');
    if it.next().is_some() {
        return Err(format!("trailing text after scope: `{}`", tail));
    }
    if name.is_empty() {
        return Err(format!("`FOR {}` needs a name", kind));
    }

    if kind.eq_ignore_ascii_case("TABLE") {
        Ok((head, Scope::Table(name.to_string())))
    } else if kind.eq_ignore_ascii_case("COLUMN") {
        // Accept `t.c`, and `"t"."c"`.
        let cleaned = name.replace('"', "");
        let mut parts = cleaned.splitn(2, '.');
        let t = parts.next().unwrap_or("");
        let c = parts.next().unwrap_or("");
        if t.is_empty() || c.is_empty() {
            return Err(format!("`FOR COLUMN` needs table.column, got `{}`", name));
        }
        Ok((head, Scope::Column(t.to_string(), c.to_string())))
    } else {
        Err(format!("expected FOR TABLE or FOR COLUMN, got `FOR {}`", kind))
    }
}

fn set_in(scope: &Scope, f: impl FnOnce(&mut Overrides) + Copy) {
    let s = settings();
    match scope {
        Scope::Global => s.update_global(f),
        Scope::Table(t) => s.update_table(t, f),
        Scope::Column(t, c) => s.update_column(t, c, f),
    }
}

fn apply(key: &str, value: &str, scope: &Scope) -> Result<String, String> {
    let k = key.trim().to_ascii_lowercase();
    let spec = KEYS
        .iter()
        .find(|x| x.name == k)
        .ok_or_else(|| format!("unknown filter key `{}` (try SHOW FILTER KEYS)", key.trim()))?;

    if spec.global_only && *scope != Scope::Global {
        return Err(format!("`{}` is a global setting and takes no FOR clause", spec.name));
    }

    let s = settings();
    match k.as_str() {
        "enabled" => s.set_enabled(parse_bool(value)?),
        "backfill_on_open" => s.set_backfill_on_open(parse_bool(value)?),
        "min_pruning_ratio" => {
            let v: f64 = value.trim().parse().map_err(|_| format!("`{}` is not a number", value.trim()))?;
            if !(0.0..=1.0).contains(&v) {
                return Err("min_pruning_ratio must be between 0 and 1".to_string());
            }
            s.set_min_pruning_ratio(v);
        }
        "checkpoint_interval_secs" => {
            let v: u64 = value.trim().parse().map_err(|_| format!("`{}` is not an integer", value.trim()))?;
            s.set_checkpoint_interval_secs(v);
        }
        "indexed" => {
            let v = parse_bool(value)?;
            set_in(scope, move |o| o.indexed = Some(v));
        }
        "bloom" => {
            let v = parse_bool(value)?;
            set_in(scope, move |o| o.bloom = Some(v));
        }
        "zone_bounds" => {
            let v = parse_bool(value)?;
            set_in(scope, move |o| o.zone_bounds = Some(v));
        }
        "min_f" | "default_f" | "max_f" => {
            let v: u8 = value.trim().parse().map_err(|_| format!("`{}` is not an integer", value.trim()))?;
            if !(super::kernels::MIN_F..=super::kernels::MAX_F).contains(&v) {
                return Err(format!(
                    "{} must be between {} and {}",
                    k,
                    super::kernels::MIN_F,
                    super::kernels::MAX_F
                ));
            }
            match k.as_str() {
                "min_f" => set_in(scope, move |o| o.min_f = Some(v)),
                "default_f" => set_in(scope, move |o| o.default_f = Some(v)),
                _ => set_in(scope, move |o| o.max_f = Some(v)),
            }
        }
        "min_selectivity" => {
            let v: f64 = value.trim().parse().map_err(|_| format!("`{}` is not a number", value.trim()))?;
            if !(v > 0.0 && v <= 1.0) {
                return Err("min_selectivity must be greater than 0 and at most 1".to_string());
            }
            set_in(scope, move |o| o.min_selectivity = Some(v));
        }
        "min_table_rows" => {
            let v: u64 = value.trim().parse().map_err(|_| format!("`{}` is not an integer", value.trim()))?;
            set_in(scope, move |o| o.min_table_rows = Some(v));
        }
        _ => unreachable!("key list and match are out of step"),
    }

    Ok(match scope {
        Scope::Global => format!("filter {} set", k),
        Scope::Table(t) => format!("filter {} set for table {}", k, t),
        Scope::Column(t, c) => format!("filter {} set for column {}.{}", k, t, c),
    })
}

/// Does this statement belong to the filter control surface?
pub fn is_filter_statement(sql: &str) -> bool {
    let t = sql.trim_start();
    let u = t.to_ascii_uppercase();
    let u = u.trim_end_matches(';').trim_end();
    u.starts_with("SET FILTER")
        || u.starts_with("SHOW FILTER")
        || u.starts_with("RESET FILTER")
}

/// Run one control statement. Returns lines of output, or an error message.
pub fn execute(sql: &str) -> Result<Vec<String>, String> {
    let t = sql.trim().trim_end_matches(';').trim();
    let u = t.to_ascii_uppercase();

    if let Some(rest) = u.strip_prefix("SET FILTER") {
        let rest = &t[t.len() - rest.len()..];
        let (assignment, scope) = split_scope(rest)?;
        let (key, value) = assignment
            .split_once('=')
            .ok_or_else(|| "SET FILTER needs `key = value`".to_string())?;
        return apply(key, value.trim().trim_matches('\''), &scope).map(|m| vec![m]);
    }

    if let Some(rest) = u.strip_prefix("RESET FILTER") {
        let rest = &t[t.len() - rest.len()..];
        let (head, scope) = split_scope(rest)?;
        if !head.trim().is_empty() {
            return Err(format!("unexpected text in RESET FILTER: `{}`", head.trim()));
        }
        let s = settings();
        return Ok(vec![match &scope {
            Scope::Global => {
                s.reset_all();
                "filter settings reset".to_string()
            }
            Scope::Table(t) => {
                s.reset_table(t);
                format!("filter settings reset for table {}", t)
            }
            Scope::Column(t, c) => {
                s.reset_column(t, c);
                format!("filter settings reset for column {}.{}", t, c)
            }
        }]);
    }

    if let Some(rest) = u.strip_prefix("SHOW FILTER") {
        let rest_orig = &t[t.len() - rest.len()..];
        if rest.trim() == "KEYS" {
            return Ok(KEYS
                .iter()
                .map(|k| {
                    format!(
                        "{:<26} {:<8} {}",
                        k.name,
                        if k.global_only { "global" } else { "scoped" },
                        k.help
                    )
                })
                .collect());
        }
        let (head, scope) = split_scope(rest_orig)?;
        if !head.trim().is_empty() {
            return Err(format!("unexpected text in SHOW FILTER: `{}`", head.trim()));
        }
        let s = settings();
        return Ok(match &scope {
            Scope::Global => s.describe(None, None),
            Scope::Table(t) => s.describe(Some(t), None),
            Scope::Column(t, c) => s.describe(Some(t), Some(c)),
        });
    }

    Err(format!("not a filter statement: `{}`", t))
}
