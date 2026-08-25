//! Turning a result set into something you can paste somewhere else.
//!
//! The whole point of these is the distinction the grid already makes visible:
//! a SQL `NULL` and an empty string are different values. CSV has no way to say
//! `NULL` — an unquoted empty field is the closest thing, and a *quoted* empty
//! field is the empty string, so the two survive the round trip. JSON says it
//! properly. Markdown cannot, and says so with a marker rather than pretending.

use crate::model::Grid;

/// What a result set can be copied as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Csv,
    Json,
    Markdown,
    /// `INSERT` statements that would recreate the rows.
    Inserts,
}

impl Format {
    pub const ALL: [Format; 4] = [Format::Csv, Format::Json, Format::Markdown, Format::Inserts];

    /// The extension a file of this kind takes.
    pub fn extension(&self) -> &'static str {
        match self {
            Format::Csv => "csv",
            Format::Json => "json",
            Format::Markdown => "md",
            Format::Inserts => "sql",
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Format::Csv => "CSV",
            Format::Json => "JSON",
            Format::Markdown => "MD",
            Format::Inserts => "INSERTS",
        }
    }
}

/// Renders a grid in the chosen format.
///
/// `table` names the relation for [`Format::Inserts`]; anything else ignores
/// it.
pub fn render(grid: &Grid, format: Format, table: &str) -> String {
    match format {
        Format::Csv => to_csv(grid),
        Format::Json => to_json(grid),
        Format::Markdown => to_markdown(grid),
        Format::Inserts => to_inserts(grid, table),
    }
}

/// RFC 4180: quote a field that contains a comma, a quote or a newline, and
/// double the quotes inside it.
///
/// An unquoted empty field is `NULL`; a quoted one is the empty string. That
/// is the only place CSV can carry the distinction, so it is used deliberately
/// rather than falling out of the encoder.
pub fn to_csv(grid: &Grid) -> String {
    let mut out = String::new();
    out.push_str(&grid.columns.iter().map(|c| csv_field(c)).collect::<Vec<_>>().join(","));
    out.push('\n');

    for row in &grid.rows {
        let line: Vec<String> = row
            .iter()
            .map(|value| match value {
                None => String::new(),
                Some(text) => csv_field(text),
            })
            .collect();
        out.push_str(&line.join(","));
        out.push('\n');
    }
    out
}

fn csv_field(value: &str) -> String {
    if value.is_empty() || value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

/// An array of objects, with `null` where the value is `NULL`.
///
/// Every value is a string even when it looks like a number: the wire hands
/// text over and the console does not know a column's type here, so emitting
/// `12` for a `TEXT` column holding `"12"` would be inventing one.
pub fn to_json(grid: &Grid) -> String {
    let rows: Vec<serde_json::Value> = grid
        .rows
        .iter()
        .map(|row| {
            let object: serde_json::Map<String, serde_json::Value> = grid
                .columns
                .iter()
                .enumerate()
                .map(|(index, name)| {
                    let value = match row.get(index).and_then(|v| v.as_ref()) {
                        None => serde_json::Value::Null,
                        Some(text) => serde_json::Value::String(text.clone()),
                    };
                    (name.clone(), value)
                })
                .collect();
            serde_json::Value::Object(object)
        })
        .collect();

    serde_json::to_string_pretty(&rows).unwrap_or_else(|_| "[]".to_string())
}

/// A pipe table. `NULL` becomes `∅`, because Markdown has no way to say it and
/// an empty cell would be indistinguishable from an empty string.
pub fn to_markdown(grid: &Grid) -> String {
    let escape = |value: &str| value.replace('|', "\\|").replace('\n', " ");

    let mut out = String::new();
    out.push_str(&format!(
        "| {} |\n",
        grid.columns.iter().map(|c| escape(c)).collect::<Vec<_>>().join(" | ")
    ));
    out.push_str(&format!(
        "|{}|\n",
        grid.columns.iter().map(|_| "---").collect::<Vec<_>>().join("|")
    ));

    for row in &grid.rows {
        let cells: Vec<String> = (0..grid.columns.len())
            .map(|index| match row.get(index).and_then(|v| v.as_ref()) {
                None => "∅".to_string(),
                Some(text) => escape(text),
            })
            .collect();
        out.push_str(&format!("| {} |\n", cells.join(" | ")));
    }
    out
}

/// `INSERT` statements that would recreate the rows.
///
/// One statement per row rather than one multi-row statement: a row that a
/// target rejects then fails on its own instead of taking the batch with it,
/// which is what anyone pasting these into another database wants.
pub fn to_inserts(grid: &Grid, table: &str) -> String {
    let table = if table.trim().is_empty() { "table_name" } else { table.trim() };
    let columns = grid
        .columns
        .iter()
        .map(|c| crate::dialect::quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    let mut out = String::new();
    for row in &grid.rows {
        let values = (0..grid.columns.len())
            .map(|index| crate::dialect::sql_value(row.get(index).and_then(|v| v.as_deref())))
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!(
            "INSERT INTO {} ({}) VALUES ({});\n",
            crate::dialect::quote_ident(table),
            columns,
            values
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grid() -> Grid {
        Grid {
            columns: vec!["id".into(), "note".into()],
            rows: vec![
                vec![Some("1".into()), Some("plain".into())],
                vec![Some("2".into()), None],
                vec![Some("3".into()), Some(String::new())],
                vec![Some("4".into()), Some("has, comma \"and\" quote".into())],
            ],
            tag: String::new(),
        }
    }

    /// The distinction the whole grid is careful about must survive export.
    #[test]
    fn csv_distinguishes_null_from_empty() {
        let csv = to_csv(&grid());
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(lines[0], "id,note");
        assert_eq!(lines[2], "2,", "NULL is an unquoted empty field");
        assert_eq!(lines[3], "3,\"\"", "the empty string is a quoted empty field");
    }

    #[test]
    fn csv_quotes_what_it_must() {
        let csv = to_csv(&grid());
        assert!(
            csv.contains("4,\"has, comma \"\"and\"\" quote\""),
            "commas and quotes have to survive: {csv}"
        );
    }

    #[test]
    fn json_says_null_properly() {
        let json = to_json(&grid());
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed[1]["note"], serde_json::Value::Null);
        assert_eq!(parsed[2]["note"], serde_json::Value::String(String::new()));
        assert_eq!(parsed[3]["note"], "has, comma \"and\" quote");
    }

    /// Every value is a string, even one that looks like a number: the console
    /// does not know the column's type, and guessing would turn a `TEXT` column
    /// holding `"12"` into a number.
    #[test]
    fn json_does_not_invent_types() {
        let parsed: serde_json::Value = serde_json::from_str(&to_json(&grid())).unwrap();
        assert_eq!(parsed[0]["id"], serde_json::Value::String("1".into()));
    }

    #[test]
    fn markdown_escapes_pipes_and_marks_null() {
        let mut g = grid();
        g.rows.push(vec![Some("5".into()), Some("a|b".into())]);
        let md = to_markdown(&g);
        assert!(md.contains("| a\\|b |"), "{md}");
        assert!(md.contains("| ∅ |"), "a NULL cannot be an empty cell: {md}");
    }

    #[test]
    fn inserts_quote_and_carry_nulls() {
        let sql = to_inserts(&grid(), "users");
        assert!(sql.contains("INSERT INTO \"users\" (\"id\", \"note\") VALUES ('1', 'plain');"));
        assert!(sql.contains("VALUES ('2', NULL);"), "NULL must not be quoted: {sql}");
        assert!(sql.contains("VALUES ('3', '');"));
        assert_eq!(sql.lines().count(), 4, "one statement per row");
    }

    #[test]
    fn an_empty_grid_still_renders_its_header() {
        let empty = Grid { columns: vec!["a".into()], rows: vec![], tag: String::new() };
        assert_eq!(to_csv(&empty), "a\n");
        assert_eq!(to_json(&empty), "[]");
        assert!(to_markdown(&empty).starts_with("| a |"));
        assert_eq!(to_inserts(&empty, "t"), "");
    }
}
