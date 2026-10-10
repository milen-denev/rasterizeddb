//! Editing commands the SQL editor adds over plain text entry.
//!
//! Each is done through the editor's own actions — move the cursor, insert,
//! delete — rather than by rebuilding the buffer from a string. Rebuilding is
//! simpler, but it throws away the scroll position and puts the view back at
//! the top, which on a long script is the line someone was not looking at.

use std::sync::Arc;

use iced::widget::text_editor::{Action, Content, Cursor, Edit, Motion, Position};

/// The width a new level of indentation adds.
const INDENT: &str = "    ";

/// A line break that keeps the indentation of the line it breaks, and adds a
/// level after an open bracket, so a subquery or a column list can be typed
/// one item a line without reaching for the space bar each time.
pub fn newline(content: &mut Content) {
    let cursor = content.cursor().position;
    let line = line_text(content, cursor.line);
    let before = &line[..floor_boundary(&line, cursor.column)];

    let mut indent: String = before.chars().take_while(|c| *c == ' ' || *c == '\t').collect();
    if before.trim_end().ends_with('(') {
        indent.push_str(INDENT);
    }

    content.perform(Action::Edit(Edit::Enter));
    if !indent.is_empty() {
        content.perform(Action::Edit(Edit::Paste(Arc::new(indent))));
    }
}

/// Comments out the lines the cursor or selection touches with `-- `, or
/// uncomments them if every one of them that has anything on it already is.
pub fn toggle_comment(content: &mut Content) {
    let cursor = content.cursor();
    let anchor = cursor.selection.unwrap_or(cursor.position);
    let (first, last) = ordered(anchor, cursor.position);

    // A selection that ends at the very start of a line has not taken that
    // line in — it is how a line-wise selection made with the keyboard ends.
    let last_line = if cursor.selection.is_some() && last.column == 0 && last.line > first.line {
        last.line - 1
    } else {
        last.line
    };

    let lines: Vec<String> = (first.line..=last_line).map(|i| line_text(content, i)).collect();
    if lines.iter().all(|line| line.trim().is_empty()) {
        return;
    }
    let uncomment = lines
        .iter()
        .filter(|line| !line.trim().is_empty())
        .all(|line| line.trim_start().starts_with("--"));

    if cursor.selection.is_some() {
        // A collapsed cursor is what `move_to` then places; left in place, the
        // old selection would be what the first insert replaced.
        content.perform(Action::Move(Motion::Left));
    }

    // How each line's columns moved: from this column on, by this much.
    let mut shifts: Vec<(usize, isize)> = Vec::with_capacity(lines.len());

    if uncomment {
        for (offset, line) in lines.iter().enumerate() {
            let trimmed = line.trim_start();
            if !trimmed.starts_with("--") {
                shifts.push((0, 0));
                continue;
            }
            let at = line.len() - trimmed.len();
            let width = if trimmed.starts_with("-- ") { 3 } else { 2 };
            let number = first.line + offset;
            content.move_to(Cursor {
                position: Position { line: number, column: at + width },
                selection: Some(Position { line: number, column: at }),
            });
            content.perform(Action::Edit(Edit::Delete));
            shifts.push((at, -(width as isize)));
        }
    } else {
        let indent = lines
            .iter()
            .filter(|line| !line.trim().is_empty())
            .map(|line| line.len() - line.trim_start().len())
            .min()
            .unwrap_or(0);
        for (offset, line) in lines.iter().enumerate() {
            if line.trim().is_empty() {
                shifts.push((0, 0));
                continue;
            }
            content.move_to(Cursor {
                position: Position { line: first.line + offset, column: indent },
                selection: None,
            });
            content.perform(Action::Edit(Edit::Paste(Arc::new("-- ".to_string()))));
            shifts.push((indent, 3));
        }
    }

    let shift = |position: Position| -> Position {
        let Some(&(from, by)) = position
            .line
            .checked_sub(first.line)
            .and_then(|offset| shifts.get(offset))
        else {
            return position;
        };
        // At the insertion column itself it stays put, so a selection that
        // began at the indentation grows to take the `-- ` in.
        if position.column <= from {
            return position;
        }
        let column = (position.column as isize + by).max(from as isize) as usize;
        Position { line: position.line, column }
    };

    content.move_to(Cursor {
        position: shift(cursor.position),
        selection: cursor.selection.map(shift),
    });
}

/// Puts the cursor at a line and column, with nothing selected.
pub fn place_cursor(content: &mut Content, line: usize, column: usize) {
    if content.cursor().selection.is_some() {
        content.perform(Action::Move(Motion::Left));
    }
    let line = line.min(content.line_count().saturating_sub(1));
    let text = line_text(content, line);
    content.move_to(Cursor {
        position: Position { line, column: floor_boundary(&text, column) },
        selection: None,
    });
}

fn line_text(content: &Content, line: usize) -> String {
    content.line(line).map(|line| line.text.into_owned()).unwrap_or_default()
}

fn ordered(a: Position, b: Position) -> (Position, Position) {
    if (a.line, a.column) <= (b.line, b.column) { (a, b) } else { (b, a) }
}

/// The nearest character boundary at or before `index`, so a column taken
/// from one version of a line can never slice another mid-character.
fn floor_boundary(text: &str, index: usize) -> usize {
    let mut index = index.min(text.len());
    while !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(content: &mut Content, line: usize, column: usize) {
        content.move_to(Cursor { position: Position { line, column }, selection: None });
    }

    #[test]
    fn newline_keeps_indentation_and_indents_after_a_bracket() {
        let mut content = Content::with_text("  SELECT a\n  FROM t WHERE id IN (");
        at(&mut content, 0, 10);
        newline(&mut content);
        assert_eq!(content.text(), "  SELECT a\n  \n  FROM t WHERE id IN (");
        assert_eq!(content.cursor().position, Position { line: 1, column: 2 });

        let mut content = Content::with_text("WHERE id IN (");
        at(&mut content, 0, 13);
        newline(&mut content);
        assert_eq!(content.text(), "WHERE id IN (\n    ");
    }

    #[test]
    fn comment_and_uncomment_round_trip() {
        let original = "SELECT a\n  FROM t\n\n  WHERE b = 1";
        let mut content = Content::with_text(original);
        content.move_to(Cursor {
            position: Position { line: 3, column: 4 },
            selection: Some(Position { line: 1, column: 2 }),
        });
        toggle_comment(&mut content);
        assert_eq!(content.text(), "SELECT a\n  -- FROM t\n\n  -- WHERE b = 1");
        // The selection still covers the same text.
        let cursor = content.cursor();
        assert_eq!(cursor.position, Position { line: 3, column: 7 });
        assert_eq!(cursor.selection, Some(Position { line: 1, column: 2 }));

        toggle_comment(&mut content);
        assert_eq!(content.text(), original);
    }

    #[test]
    fn a_single_line_toggles_under_a_bare_cursor() {
        let mut content = Content::with_text("SELECT 1;\nSELECT 2;");
        at(&mut content, 1, 3);
        toggle_comment(&mut content);
        assert_eq!(content.text(), "SELECT 1;\n-- SELECT 2;");
        assert_eq!(content.cursor().position, Position { line: 1, column: 6 });
        toggle_comment(&mut content);
        assert_eq!(content.text(), "SELECT 1;\nSELECT 2;");
    }

    #[test]
    fn a_selection_ending_at_column_zero_leaves_that_line_alone() {
        let mut content = Content::with_text("a\nb\nc");
        content.move_to(Cursor {
            position: Position { line: 2, column: 0 },
            selection: Some(Position { line: 0, column: 0 }),
        });
        toggle_comment(&mut content);
        assert_eq!(content.text(), "-- a\n-- b\nc");
    }

    #[test]
    fn place_cursor_clamps() {
        let mut content = Content::with_text("ab\nc");
        place_cursor(&mut content, 9, 9);
        assert_eq!(content.cursor().position, Position { line: 1, column: 1 });
        assert_eq!(content.cursor().selection, None);
    }
}
