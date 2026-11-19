//! Reading a CSV file into a table.
//!
//! The reading is the export's in reverse ([`crate::export::to_csv`]): RFC
//! 4180 quoting, a header row naming the columns, and the one distinction
//! CSV can carry — an unquoted empty field is `NULL`, a quoted one is the
//! empty string. So a table exported here comes back as it was.
//!
//! Every value is sent as a string literal and the server converts it to the
//! column's type, as the data browser's own edits do; a value that does not
//! convert fails its statement with the server's message.
//!
//! # All or nothing, per request
//!
//! Rows go in [`ROWS_PER_INSERT`] to an `INSERT` and [`INSERTS_PER_SCRIPT`]
//! `INSERT`s to a request. A request is one implicit transaction on both
//! servers, so a file that fits in one request (20,000 rows) imports whole or
//! not at all. A bigger one imports request by request; a failure stops it,
//! and the report says exactly which rows went in before it.

use crate::dialect::{quote_ident, quote_literal};
use crate::model::ColumnInfo;

pub const ROWS_PER_INSERT: usize = 1_000;
pub const INSERTS_PER_SCRIPT: usize = 20;

/// A file read: its header and its rows, `None` for an unquoted empty field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Csv {
    pub header: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

/// Reads RFC 4180 text: fields split on `,`, quoted with `"` (doubled inside),
/// records ended by `\n` or `\r\n`, a quoted field free to hold either. A
/// trailing line break ends the last record rather than starting an empty one.
pub fn parse(text: &str) -> Result<Csv, String> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut records: Vec<Vec<Option<String>>> = Vec::new();
    let mut record: Vec<Option<String>> = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut chars = text.chars().peekable();
    let mut line = 1usize;
    // Whether anything of the current record has been read, so the line
    // break after the last record does not make an empty one.
    let mut started = false;

    while let Some(c) = chars.next() {
        started = true;
        match c {
            '"' if field.is_empty() && !quoted => {
                quoted = true;
                let opened = line;
                let mut closed = false;
                while let Some(c) = chars.next() {
                    match c {
                        '"' if chars.peek() == Some(&'"') => {
                            chars.next();
                            field.push('"');
                        }
                        '"' => {
                            closed = true;
                            break;
                        }
                        '\n' => {
                            line += 1;
                            field.push(c);
                        }
                        _ => field.push(c),
                    }
                }
                if !closed {
                    return Err(format!("a quoted field opened on line {opened} never closes"));
                }
                match chars.peek() {
                    None | Some(',') | Some('\n') | Some('\r') => {}
                    Some(other) => {
                        return Err(format!(
                            "line {line}: `{other}` after a closing quote; a quote inside a quoted \
                             field is written twice"
                        ));
                    }
                }
            }
            ',' => {
                record.push(take(&mut field, &mut quoted));
            }
            '\r' if chars.peek() == Some(&'\n') => {}
            '\n' => {
                record.push(take(&mut field, &mut quoted));
                records.push(std::mem::take(&mut record));
                line += 1;
                started = false;
            }
            _ => field.push(c),
        }
    }
    if started {
        record.push(take(&mut field, &mut quoted));
        records.push(record);
    }

    let mut records = records.into_iter();
    let header: Vec<String> = match records.next() {
        Some(header) => header
            .into_iter()
            .map(|name| name.unwrap_or_default().trim().to_string())
            .collect(),
        None => return Err("the file is empty".to_string()),
    };
    if header.iter().any(String::is_empty) {
        return Err("the header row names every column; one of its fields is empty".to_string());
    }
    let rows: Vec<_> = records.collect();
    for (index, row) in rows.iter().enumerate() {
        if row.len() != header.len() {
            return Err(format!(
                "record {} has {} field{}, and the header {}",
                index + 2,
                row.len(),
                if row.len() == 1 { "" } else { "s" },
                header.len()
            ));
        }
    }
    Ok(Csv { header, rows })
}

fn take(field: &mut String, quoted: &mut bool) -> Option<String> {
    let value = std::mem::take(field);
    let was_quoted = std::mem::replace(quoted, false);
    if value.is_empty() && !was_quoted { None } else { Some(value) }
}

/// One request's worth of the import.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Script {
    /// The file's record numbers it carries (the header is record 1).
    pub first: usize,
    pub last: usize,
    pub sql: String,
}

/// The requests that insert `csv` into `table`, its header matched to the
/// table's columns by name (ignoring case). A header naming a column the
/// table lacks is refused: a typo there would otherwise drop a column of the
/// file without a word.
pub fn scripts(table: &str, csv: &Csv, columns: &[ColumnInfo]) -> Result<Vec<Script>, String> {
    if csv.rows.is_empty() {
        return Err("the file has a header and no rows".to_string());
    }
    let mut names = Vec::with_capacity(csv.header.len());
    for wanted in &csv.header {
        let Some(column) = columns.iter().find(|c| c.name.eq_ignore_ascii_case(wanted)) else {
            return Err(format!("`{table}` has no column `{wanted}` (from the file's header)"));
        };
        if names.contains(&column.name) {
            return Err(format!("the header names `{}` twice", column.name));
        }
        names.push(column.name.clone());
    }
    let into = format!(
        "INSERT INTO {} ({}) VALUES ",
        quote_ident(table),
        names.iter().map(|n| quote_ident(n)).collect::<Vec<_>>().join(", ")
    );

    let mut scripts = Vec::new();
    let per_script = ROWS_PER_INSERT * INSERTS_PER_SCRIPT;
    for (chunk_index, chunk) in csv.rows.chunks(per_script).enumerate() {
        let first = chunk_index * per_script + 2;
        let mut statements = Vec::new();
        for insert in chunk.chunks(ROWS_PER_INSERT) {
            let mut sql = into.clone();
            for (i, row) in insert.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push('(');
                for (j, value) in row.iter().enumerate() {
                    if j > 0 {
                        sql.push_str(", ");
                    }
                    match value {
                        None => sql.push_str("NULL"),
                        Some(text) => sql.push_str(&quote_literal(text)),
                    }
                }
                sql.push(')');
            }
            statements.push(sql);
        }
        scripts.push(Script {
            first,
            last: first + chunk.len() - 1,
            sql: statements.join(";\n"),
        });
    }
    Ok(scripts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Grid;

    fn column(name: &str) -> ColumnInfo {
        ColumnInfo { name: name.to_string(), ..ColumnInfo::default() }
    }

    /// What the export writes, this reads back: NULL and the empty string
    /// apart, quotes, commas and line breaks inside fields.
    #[test]
    fn an_export_reads_back_as_it_was() {
        let grid = Grid {
            columns: vec!["id".into(), "note".into()],
            rows: vec![
                vec![Some("1".into()), None],
                vec![Some("2".into()), Some(String::new())],
                vec![Some("3".into()), Some("say \"hi\", then\r\nleave".into())],
            ],
            tag: String::new(),
            total: None,
        };
        let csv = parse(&crate::export::to_csv(&grid)).unwrap();
        assert_eq!(csv.header, ["id", "note"]);
        assert_eq!(csv.rows, grid.rows);
    }

    #[test]
    fn a_damaged_file_says_where() {
        assert!(parse("").is_err());
        assert!(parse("a,b\n1\n").unwrap_err().contains("record 2"));
        assert!(parse("a,b\n\"1,2\n").unwrap_err().contains("never closes"));
        assert!(parse("a,b\n\"1\"x,2\n").unwrap_err().contains("closing quote"));
        assert!(parse("a,\n1,2\n").is_err());
        // A BOM and CRLF are a spreadsheet's export, not damage.
        assert_eq!(parse("\u{feff}a\r\n1\r\n").unwrap().rows, vec![vec![Some("1".to_string())]]);
    }

    #[test]
    fn rows_go_in_by_the_header_s_names_in_bounded_requests() {
        let columns = [column("id"), column("Label")];
        let csv = parse("ID,label\n1,it's\n2,\n").unwrap();
        let written = scripts("t", &csv, &columns).unwrap();
        assert_eq!(
            written,
            [Script {
                first: 2,
                last: 3,
                sql: "INSERT INTO \"t\" (\"id\", \"Label\") VALUES ('1', 'it''s'), ('2', NULL)"
                    .into(),
            }]
        );
        assert!(scripts_err("x,id\n1,2\n", &columns).contains("no column `x`"));
        assert!(scripts_err("id,ID\n1,2\n", &columns).contains("twice"));
        assert!(scripts_err("id\n", &columns).contains("no rows"));

        let many = format!(
            "id\n{}",
            (0..ROWS_PER_INSERT * INSERTS_PER_SCRIPT + 5)
                .map(|i| format!("{i}\n"))
                .collect::<String>()
        );
        let written = scripts("t", &parse(&many).unwrap(), &columns).unwrap();
        assert_eq!(written.len(), 2);
        assert_eq!(written[0].sql.matches("INSERT").count(), INSERTS_PER_SCRIPT);
        assert_eq!((written[0].first, written[0].last), (2, 20_001));
        assert_eq!((written[1].first, written[1].last), (20_002, 20_006));
    }

    fn scripts_err(text: &str, columns: &[ColumnInfo]) -> String {
        scripts("t", &parse(text).unwrap(), columns).unwrap_err()
    }
}
