//! Colours SQL in the editor, and marks what the linter found.
//!
//! The editor hands this one line at a time, starting from the first line that
//! changed, so all it keeps between calls is the state each line began in —
//! see [`crate::sqllex`]. Names are coloured against what the console knows
//! about the database: a word that is a table in it reads as one, which is
//! also the quickest way to notice that a name is *not* one.
//!
//! Linted spans are passed in with the settings rather than worked out here,
//! because the linter needs the whole buffer and the highlighter only ever sees
//! a line. When the findings change the settings compare unequal and the editor
//! re-highlights from the top — a few hundred lines of a scratch buffer, which
//! costs nothing next to shaping them.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use iced::advanced::text::highlighter::{self, Format};
use iced::{Font, Theme, font};

use crate::sqllex::{self, Carry, Kind, Token};
use crate::sqllint::{Diagnostic, Severity};
use crate::theme;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Highlight {
    Keyword,
    Type,
    Function,
    Table,
    Column,
    String,
    /// Numbers, `NULL`, `TRUE`, `FALSE` and `$1` — values written in place.
    Constant,
    Comment,
    Operator,
    Punct,
    Error,
    Warning,
}

/// A linted span on one line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mark {
    pub line: usize,
    pub range: Range<usize>,
    pub severity: Severity,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Settings {
    /// Table names, lower-cased.
    pub tables: Arc<HashSet<String>>,
    /// Column names of the selected table, lower-cased.
    pub columns: Arc<HashSet<String>>,
    pub marks: Arc<Vec<Mark>>,
}

impl Settings {
    pub fn marks_from(diagnostics: &[Diagnostic]) -> Vec<Mark> {
        diagnostics
            .iter()
            .map(|d| Mark { line: d.line, range: d.start..d.end, severity: d.severity })
            .collect()
    }
}

pub struct Sql {
    settings: Settings,
    /// The state each line began in; `carries[n]` is line `n`'s.
    carries: Vec<Carry>,
    current: usize,
}

impl highlighter::Highlighter for Sql {
    type Settings = Settings;
    type Highlight = Highlight;
    type Iterator<'a> = std::vec::IntoIter<(Range<usize>, Highlight)>;

    fn new(settings: &Settings) -> Self {
        Sql { settings: settings.clone(), carries: vec![Carry::None], current: 0 }
    }

    fn update(&mut self, settings: &Settings) {
        self.settings = settings.clone();
        self.change_line(0);
    }

    fn change_line(&mut self, line: usize) {
        self.current = self.current.min(line);
        self.carries.truncate(self.current + 1);
    }

    fn highlight_line(&mut self, line: &str) -> Self::Iterator<'_> {
        let carry = self.carries.get(self.current).cloned().unwrap_or_default();
        let (tokens, next) = sqllex::lex_line(line, &carry);
        self.carries.truncate(self.current + 1);
        self.carries.push(next);

        let mut spans = classify(line, &tokens, &self.settings);

        // Pushed after the syntax so they win where they overlap: the span
        // list is a range map, and a later insert clips an earlier one.
        for mark in self.settings.marks.iter().filter(|mark| mark.line == self.current) {
            let start = mark.range.start.min(line.len());
            let end = mark.range.end.min(line.len());
            if start < end && line.is_char_boundary(start) && line.is_char_boundary(end) {
                let highlight = match mark.severity {
                    Severity::Error => Highlight::Error,
                    Severity::Warning => Highlight::Warning,
                };
                spans.push((start..end, highlight));
            }
        }

        self.current += 1;
        spans.into_iter()
    }

    fn current_line(&self) -> usize {
        self.current
    }
}

/// What colour each token on a line is. Plain names get no span at all.
pub fn classify(line: &str, tokens: &[Token], settings: &Settings) -> Vec<(Range<usize>, Highlight)> {
    let text = |token: &Token| &line[token.start..token.end];
    let mut spans = Vec::with_capacity(tokens.len());

    for (index, token) in tokens.iter().enumerate() {
        // `count(*) AS orders` names a result column; colouring it as the
        // `orders` table would say something false about it.
        let alias = index > 0
            && tokens[index - 1].kind == Kind::Word
            && text(&tokens[index - 1]).eq_ignore_ascii_case("AS");
        let highlight = match token.kind {
            Kind::Comment => Some(Highlight::Comment),
            Kind::String => Some(Highlight::String),
            Kind::Number | Kind::Param => Some(Highlight::Constant),
            Kind::Operator => Some(Highlight::Operator),
            Kind::Punct => Some(Highlight::Punct),
            Kind::Quoted if alias => None,
            Kind::Quoted => name(&sqllex::identifier(Kind::Quoted, text(token)), false, settings),
            Kind::Word => {
                let upper = text(token).to_ascii_uppercase();
                let called = tokens
                    .get(index + 1)
                    .is_some_and(|next| next.kind == Kind::Punct && text(next) == "(");
                if sqllex::is_constant(&upper) {
                    Some(Highlight::Constant)
                } else if sqllex::is_type(&upper) {
                    Some(Highlight::Type)
                } else if sqllex::is_keyword(&upper) {
                    if called && sqllex::FUNCTION_KEYWORDS.contains(&upper.as_str()) {
                        Some(Highlight::Function)
                    } else {
                        Some(Highlight::Keyword)
                    }
                } else if alias {
                    None
                } else {
                    name(&upper.to_lowercase(), called, settings)
                }
            }
        };
        if let Some(highlight) = highlight {
            spans.push((token.start..token.end, highlight));
        }
    }

    spans
}

fn name(lower: &str, called: bool, settings: &Settings) -> Option<Highlight> {
    let lower = lower.to_lowercase();
    if settings.tables.contains(&lower) {
        Some(Highlight::Table)
    } else if called {
        Some(Highlight::Function)
    } else if settings.columns.contains(&lower) {
        Some(Highlight::Column)
    } else {
        None
    }
}

pub fn format(highlight: &Highlight, _theme: &Theme) -> Format<Font> {
    let (color, font) = match highlight {
        Highlight::Keyword => (theme::SYN_KEYWORD, None),
        Highlight::Type => (theme::SYN_TYPE, None),
        Highlight::Function => (theme::SYN_FUNCTION, None),
        Highlight::Table => (theme::SYN_TABLE, None),
        Highlight::Column => (theme::SYN_COLUMN, None),
        Highlight::String => (theme::SYN_STRING, None),
        Highlight::Constant => (theme::SYN_NUMBER, None),
        Highlight::Comment => {
            (theme::SYN_COMMENT, Some(Font { style: font::Style::Italic, ..theme::MONO }))
        }
        Highlight::Operator => (theme::SYN_OPERATOR, None),
        Highlight::Punct => (theme::SYN_PUNCT, None),
        Highlight::Error => (theme::BAD, Some(Font { weight: font::Weight::Bold, ..theme::MONO })),
        Highlight::Warning => (theme::WARN, Some(Font { weight: font::Weight::Bold, ..theme::MONO })),
    };
    Format { color: Some(color), font }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iced::advanced::text::Highlighter as _;

    fn settings(tables: &[&str], columns: &[&str]) -> Settings {
        Settings {
            tables: Arc::new(tables.iter().map(|t| t.to_string()).collect()),
            columns: Arc::new(columns.iter().map(|c| c.to_string()).collect()),
            marks: Arc::default(),
        }
    }

    fn colours<'a>(line: &'a str, settings: &Settings) -> Vec<(&'a str, Highlight)> {
        let (tokens, _) = sqllex::lex_line(line, &Carry::None);
        classify(line, &tokens, settings)
            .into_iter()
            .map(|(range, highlight)| (&line[range], highlight))
            .collect()
    }

    #[test]
    fn words_are_told_apart() {
        let s = settings(&["users"], &["email"]);
        let found = colours("SELECT count(email), left(name, 2)::text FROM Users LEFT JOIN x", &s);
        let of = |word: &str| found.iter().find(|(w, _)| *w == word).map(|(_, h)| *h);
        assert_eq!(of("SELECT"), Some(Highlight::Keyword));
        assert_eq!(of("count"), Some(Highlight::Function));
        assert_eq!(of("email"), Some(Highlight::Column));
        assert_eq!(of("left"), Some(Highlight::Function));
        assert_eq!(of("LEFT"), Some(Highlight::Keyword));
        assert_eq!(of("text"), Some(Highlight::Type));
        assert_eq!(of("Users"), Some(Highlight::Table));
        assert_eq!(of("name"), None, "an unknown name stays plain");
        assert_eq!(of("x"), None);

        let found = colours("SELECT count(*) AS users FROM users", &s);
        assert_eq!(found.iter().filter(|(_, h)| *h == Highlight::Table).count(), 1, "{found:?}");
    }

    #[test]
    fn constants_and_literals() {
        let found = colours("WHERE a = NULL OR b = 'x' OR c = 3 OR d = $1", &Settings::default());
        for (word, highlight) in [("NULL", Highlight::Constant), ("'x'", Highlight::String), ("3", Highlight::Constant), ("$1", Highlight::Constant)] {
            assert!(found.contains(&(word, highlight)), "{word}: {found:?}");
        }
    }

    /// The editor re-feeds from the changed line on; a comment opened above it
    /// must still colour the lines below.
    #[test]
    fn state_survives_a_change_line() {
        let mut sql = Sql::new(&Settings::default());
        let lines = ["select 1 /* open", "still a comment", "*/ select 2"];
        for line in lines {
            let _ = sql.highlight_line(line).count();
        }
        sql.change_line(1);
        assert_eq!(sql.current_line(), 1);
        let spans: Vec<_> = sql.highlight_line(lines[1]).collect();
        assert_eq!(spans, vec![(0..15, Highlight::Comment)]);
        let spans: Vec<_> = sql.highlight_line(lines[2]).collect();
        assert_eq!(spans[0], (0..2, Highlight::Comment));
        assert_eq!(spans[1], (3..9, Highlight::Keyword));
    }

    #[test]
    fn marks_come_after_syntax_on_their_line() {
        let s = Settings {
            marks: Arc::new(vec![Mark { line: 1, range: 0..6, severity: Severity::Warning }]),
            ..Settings::default()
        };
        let mut sql = Sql::new(&s);
        let first: Vec<_> = sql.highlight_line("select 1").collect();
        assert!(first.iter().all(|(_, h)| *h != Highlight::Warning));
        let second: Vec<_> = sql.highlight_line("UPDATE t SET a = 1").collect();
        assert_eq!(second.last(), Some(&(0..6, Highlight::Warning)));
    }

    #[test]
    fn a_stale_mark_past_the_line_is_dropped() {
        let s = Settings {
            marks: Arc::new(vec![Mark { line: 0, range: 40..50, severity: Severity::Error }]),
            ..Settings::default()
        };
        let mut sql = Sql::new(&s);
        let spans: Vec<_> = sql.highlight_line("select 1").collect();
        assert!(spans.iter().all(|(_, h)| *h != Highlight::Error));
    }
}
