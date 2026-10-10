//! The SQL editor, its tools, and what came back.
//!
//! Several statements can be sent at once and each reports separately. That is
//! not a convenience: a migration whose fourth statement failed needs to say
//! *which*, and a console that collapsed the run into one success or one
//! failure would lose exactly the information being looked for.
//!
//! # About the timings
//!
//! Every figure here is a **round trip** — sent to answered — and is labelled
//! as one. It includes the network and the driver, so it is not the server's
//! execution time and is never presented as one. Over a loopback connection
//! the difference is microseconds; over a link to another continent it is most
//! of the number, and a console that called that "execution time" would have
//! people optimising the wrong thing.
//!
//! Several statements sent together are one exchange, so they get one figure
//! between them rather than a fabricated split.
//!
//! # The editor
//!
//! SQL is coloured as it is typed ([`crate::sqlhl`]) and checked as it is typed
//! ([`crate::sqllint`]); what the checker finds is marked in the text and
//! listed under it, and clicking a finding puts the cursor on it. Findings
//! never stop anything from running: the server is the authority on what is
//! valid, and a warning about a missing `WHERE` is a question, not a veto.
//!
//! The keys, beyond plain typing:
//!
//! | Keys | Does |
//! |---|---|
//! | Ctrl+Enter | runs everything |
//! | Ctrl+Shift+Enter | runs the statement the cursor is in |
//! | Enter | a new line at the same indentation, one deeper after `(` |
//! | Tab / Shift+Tab | indent / unindent — the selected lines, if any |
//! | Ctrl+/ | comments the lines out, or back in |
//! | Ctrl+Shift+F | formats |
//! | Ctrl+S | saves to a `.sql` file |

use std::collections::HashSet;
use std::sync::Arc;

use iced::widget::text_editor::{Action, Binding, Edit};
use iced::widget::{Space, button, column, container, row, rule, scrollable, text, text_editor};
use iced::{Center, Element, Fill, Length};

use crate::app::{Message, Studio, Template};
use crate::export::Format;
use crate::model::{Flavor, Outcome, age, duration};
use crate::sqlhl;
use crate::sqllint::{self, Diagnostic, Severity};
use crate::theme;

use super::{framed, plain_cell, result_grid, small_button};

/// The editor's widget id, so a click on a finding can hand it the focus.
pub fn editor_id() -> iced::widget::Id {
    iced::widget::Id::new("sql-editor")
}

pub fn view(studio: &Studio) -> Element<'_, Message> {
    let (settings, diagnostics) = analyse(studio);

    let editor = text_editor(&studio.editor)
        .id(editor_id())
        .placeholder("-- SQL here. Ctrl+Enter runs it all; Ctrl+Shift+Enter, the statement under the cursor.")
        .on_action(Message::EditorAction)
        .font(theme::MONO)
        .size(13)
        .padding(10)
        .height(Length::Fixed(if studio.editor_tall { 560.0 } else { 260.0 }))
        .style(theme::editor)
        .highlight_with::<sqlhl::Sql>(settings, sqlhl::format)
        .key_binding(key_binding);

    let mut panel = column![
        toolbar(studio),
        container(editor).width(Fill),
    ]
    .spacing(8);
    if !diagnostics.is_empty() {
        panel = panel.push(problems(&diagnostics));
    }
    let panel = panel.push(footer(studio, &diagnostics))
        .push(rule::horizontal(1).style(theme::hairline))
        .push(results(studio));

    row![
        container(framed("SQL", None, panel.into())).width(Fill),
        history(studio),
    ]
    .spacing(10)
    .into()
}

// ---------------------------------------------------------------------------
// Editing
// ---------------------------------------------------------------------------

/// The highlighter's settings and the linter's findings, from one pass over
/// the buffer and what the console knows about the database.
fn analyse(studio: &Studio) -> (sqlhl::Settings, Vec<Diagnostic>) {
    let text = studio.editor.text();
    let tables: HashSet<String> = studio.tables.iter().map(|t| t.name.to_lowercase()).collect();
    let columns: HashSet<String> = studio.columns.iter().map(|c| c.name.to_lowercase()).collect();
    let names: HashSet<String> = tables.union(&columns).cloned().collect();

    // Only RasterizedDB's table list is the whole truth: PostgreSQL's leaves
    // out views, and every one of them would be called unknown.
    let authoritative = studio.flavor() == Flavor::Rasterized && !tables.is_empty();
    let diagnostics = sqllint::lint(
        &text,
        sqllint::Context { tables: authoritative.then_some(&tables), names: Some(&names) },
    );

    let settings = sqlhl::Settings {
        tables: Arc::new(tables),
        columns: Arc::new(columns),
        marks: Arc::new(sqlhl::Settings::marks_from(&diagnostics)),
    };
    (settings, diagnostics)
}

fn key_binding(press: text_editor::KeyPress) -> Option<Binding<Message>> {
    use iced::keyboard::Key;
    use iced::keyboard::key::Named;

    let command = press.modifiers.command() || press.modifiers.control();
    let shift = press.modifiers.shift();

    // Running works wherever the focus is on this screen. A bare Enter has to
    // stay a newline: this is an editor for statements that span lines, and a
    // console that ran on Enter would fire half a statement every time.
    if let Key::Named(Named::Enter) = press.key.as_ref() {
        if command && shift {
            return Some(Binding::Custom(Message::RunStatement));
        }
        if command {
            return Some(Binding::Custom(Message::RunQuery));
        }
    }

    // Everything else edits, and only the editor that has the focus: a Tab
    // pressed in the sidebar's filter must not indent the SQL.
    if !matches!(press.status, text_editor::Status::Focused { .. }) {
        return Binding::from_key_press(press);
    }

    let letter = press.key.to_latin(press.physical_key);
    let edit = |edit| Some(Binding::Custom(Message::EditorAction(Action::Edit(edit))));
    match press.key.as_ref() {
        Key::Named(Named::Enter) if !press.modifiers.alt() => {
            Some(Binding::Custom(Message::EditorNewline))
        }
        Key::Named(Named::Tab) if shift => edit(Edit::Unindent),
        Key::Named(Named::Tab) if !command => edit(Edit::Indent),
        Key::Character("/") if command => Some(Binding::Custom(Message::ToggleComment)),
        _ if command && shift && letter == Some('f') => Some(Binding::Custom(Message::FormatSql)),
        _ if command && !shift && letter == Some('s') => {
            Some(Binding::Custom(Message::SaveSqlFile))
        }
        _ => Binding::from_key_press(press),
    }
}

/// What the linter found, one clickable line each.
fn problems<'a>(diagnostics: &[Diagnostic]) -> Element<'a, Message> {
    let mut list = column![].spacing(1);
    for diagnostic in diagnostics {
        let (mark, colour) = match diagnostic.severity {
            Severity::Error => ("✗", theme::BAD),
            Severity::Warning => ("▲", theme::WARN),
        };
        list = list.push(
            button(
                row![
                    text(mark).size(10).font(theme::MONO).color(colour),
                    text(format!("{}:{}", diagnostic.line + 1, diagnostic.start + 1))
                        .size(10)
                        .font(theme::MONO)
                        .color(theme::TEXT_FAINT)
                        .width(Length::Fixed(52.0)),
                    text(diagnostic.message.clone()).size(11).color(theme::TEXT),
                ]
                .spacing(8)
                .align_y(Center),
            )
            .width(Fill)
            .padding([2, 8])
            .style(theme::tree_item(false))
            .on_press(Message::JumpTo(diagnostic.line, diagnostic.start)),
        );
    }

    container(scrollable(list).style(theme::scroller))
        .max_height(92.0)
        .width(Fill)
        .padding(2)
        .style(theme::panel)
        .into()
}

// ---------------------------------------------------------------------------
// Tools
// ---------------------------------------------------------------------------

fn toolbar(studio: &Studio) -> Element<'_, Message> {
    let has_table = studio.selected_table.is_some();

    let mut templates = row![
        text("insert a statement for")
            .size(10)
            .color(theme::TEXT_FAINT),
        text(studio.selected_table.as_deref().unwrap_or("no table"))
            .size(10)
            .font(theme::MONO)
            .color(if has_table { theme::RUST } else { theme::TEXT_FAINT }),
    ]
    .spacing(6)
    .align_y(Center);

    for template in Template::ALL {
        templates = templates.push(small_button(
            template.label(),
            has_table.then_some(Message::InsertTemplate(template)),
        ));
    }

    container(
        column![
            row![
                super::primary_button("RUN   CTRL+ENTER", Some(Message::RunQuery)),
                small_button("RUN STATEMENT", Some(Message::RunStatement)),
                small_button("RUN SELECTION", Some(Message::RunSelection)),
                small_button("FORMAT", Some(Message::FormatSql)),
                small_button("COMMENT", Some(Message::ToggleComment)),
                small_button("CLEAR", Some(Message::ClearEditor)),
                Space::new().width(Fill),
                small_button(
                    if studio.editor_tall { "SHORTER" } else { "TALLER" },
                    Some(Message::ToggleEditorSize),
                ),
                small_button("OPEN .SQL", Some(Message::OpenSqlFile)),
                small_button("SAVE .SQL", Some(Message::SaveSqlFile)),
            ]
            .spacing(6)
            .align_y(Center),
            templates,
        ]
        .spacing(6),
    )
    .padding(8)
    .width(Fill)
    .style(theme::header)
    .into()
}

fn footer<'a>(studio: &'a Studio, diagnostics: &[Diagnostic]) -> Element<'a, Message> {
    let cursor = studio.editor.cursor().position;
    // The cursor's column is a byte offset; people count characters.
    let column = studio
        .editor
        .line(cursor.line)
        .map(|line| line.text.get(..cursor.column).map_or(cursor.column, |s| s.chars().count()))
        .unwrap_or(0);

    let errors = diagnostics.iter().filter(|d| d.severity == Severity::Error).count();
    let warnings = diagnostics.len() - errors;
    let plural = |n: usize, word: &str| format!("{n} {word}{}", if n == 1 { "" } else { "s" });

    row![
        text(format!("Ln {}, Col {}", cursor.line + 1, column + 1))
            .size(10)
            .font(theme::MONO)
            .color(theme::TEXT_DIM),
        text(format!("· {} lines", studio.editor.line_count()))
            .size(10)
            .font(theme::MONO)
            .color(theme::TEXT_FAINT),
        if diagnostics.is_empty() {
            text("· no problems").size(10).font(theme::MONO).color(theme::TEXT_FAINT)
        } else {
            text(format!("· {} · {}", plural(errors, "error"), plural(warnings, "warning")))
                .size(10)
                .font(theme::MONO)
                .color(if errors > 0 { theme::BAD } else { theme::WARN })
        },
        Space::new().width(Fill),
        text("Ctrl+/ comment · Tab indent · Ctrl+Shift+F format")
            .size(10)
            .color(theme::TEXT_FAINT),
        match studio.last_elapsed {
            None => text(String::new()).size(10),
            Some(elapsed) => text(format!("last run: {} round trip", duration(elapsed)))
                .size(10)
                .font(theme::MONO)
                .color(theme::TEXT_DIM),
        },
    ]
    .spacing(8)
    .align_y(Center)
    .into()
}

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

fn results(studio: &Studio) -> Element<'_, Message> {
    if let Some(error) = &studio.query_error {
        let mut panel = column![text(error.clone()).size(12).font(theme::MONO)].spacing(4);
        if let Some(elapsed) = studio.last_elapsed {
            panel = panel.push(
                text(format!("failed after {}", duration(elapsed)))
                    .size(10)
                    .color(theme::TEXT_DIM),
            );
        }
        return container(panel)
            .padding(10)
            .width(Fill)
            .style(theme::error_box)
            .into();
    }

    if studio.results.is_empty() {
        return container(text("Nothing run yet.").size(12).color(theme::TEXT_FAINT))
            .padding(10)
            .into();
    }

    let total_rows: usize = studio
        .results
        .iter()
        .map(|outcome| match outcome {
            Outcome::Rows(grid) => grid.rows.len(),
            Outcome::Affected { .. } => 0,
        })
        .sum();

    let mut panes = column![].spacing(10);

    panes = panes.push(
        row![
            text(format!(
                "{} statement{} · {} row{}",
                studio.results.len(),
                if studio.results.len() == 1 { "" } else { "s" },
                crate::model::count(total_rows as u64),
                if total_rows == 1 { "" } else { "s" },
            ))
            .size(11)
            .font(theme::MONO)
            .color(theme::TEXT_DIM),
            Space::new().width(Fill),
            match studio.last_elapsed {
                None => text(String::new()).size(11),
                Some(elapsed) => text(format!("{} round trip", duration(elapsed)))
                    .size(11)
                    .font(theme::MONO)
                    .color(theme::RUST),
            },
        ]
        .align_y(Center),
    );

    for (index, outcome) in studio.results.iter().enumerate() {
        panes = panes.push(match outcome {
            Outcome::Affected { tag, .. } => {
                let element: Element<'_, Message> = container(
                    text(format!("statement {} — {}", index + 1, tag))
                        .size(12)
                        .font(theme::MONO),
                )
                .padding(8)
                .width(Fill)
                .style(theme::ok_box)
                .into();
                element
            }
            Outcome::Rows(grid) => {
                let usable = !grid.rows.is_empty();
                let mut copies = row![
                    text("copy").size(10).color(theme::TEXT_FAINT),
                ]
                .spacing(4)
                .align_y(Center);
                for format in Format::ALL {
                    copies = copies.push(small_button(
                        format.label(),
                        usable.then_some(Message::CopyResult(index, format)),
                    ));
                }
                copies = copies.push(
                    text("export").size(10).color(theme::TEXT_FAINT),
                );
                for format in Format::ALL {
                    copies = copies.push(small_button(
                        format.label(),
                        usable.then_some(Message::ExportResult(index, format)),
                    ));
                }

                let grid_view = result_grid(
                    grid,
                    super::GridLayout::new(studio, super::QUERY_SCOPE),
                    |_| false,
                    |_, _, value| plain_cell(value, false),
                    None,
                );

                column![
                    row![
                        text(format!("statement {} — {}", index + 1, grid.tag))
                            .size(11)
                            .font(theme::MONO)
                            .color(theme::TEXT_FAINT),
                        Space::new().width(Fill),
                        copies,
                    ]
                    .align_y(Center),
                    container(grid_view)
                        .height(Length::Fixed(320.0))
                        .width(Fill)
                        .style(theme::panel),
                ]
                .spacing(4)
                .into()
            }
        });
    }

    scrollable(panes).height(Fill).style(theme::scroller).into()
}

// ---------------------------------------------------------------------------
// History
// ---------------------------------------------------------------------------

fn history(studio: &Studio) -> Element<'_, Message> {
    let mut list = column![].spacing(3);

    // Newest first: the statement someone wants back is almost always the last
    // one they ran.
    for (index, entry) in studio.history.iter().enumerate().rev() {
        let failed = entry.outcome.failed();

        let summary = row![
            text(if failed { "✗" } else { "✓" })
                .size(10)
                .font(theme::MONO)
                .color(if failed { theme::BAD } else { theme::OK }),
            text(entry.outcome.summary())
                .size(9)
                .font(theme::MONO)
                .color(if failed { theme::BAD } else { theme::TEXT_FAINT }),
            text("·").size(9).color(theme::TEXT_FAINT),
            text(duration(entry.elapsed))
                .size(9)
                .font(theme::MONO)
                .color(theme::TEXT_FAINT),
            Space::new().width(Fill),
            text(age(entry.at)).size(9).color(theme::TEXT_FAINT),
        ]
        .spacing(5)
        .align_y(Center);

        // Clicking loads; it deliberately does not run. This list holds the
        // session's DROP and DELETE statements as readily as its SELECTs, and
        // a click that re-ran one would be a loaded gun.
        let body = iced::widget::button(
            column![
                text(super::elide(&entry.sql, 160))
                    .size(11)
                    .font(theme::MONO)
                    .color(if failed { theme::TEXT_DIM } else { theme::TEXT }),
                summary,
            ]
            .spacing(2),
        )
        .width(Fill)
        .padding([4, 6])
        .style(theme::tree_item(false))
        .on_press(Message::LoadHistory(index));

        list = list.push(
            column![
                body,
                row![
                    small_button("RUN", Some(Message::RunHistory(index))),
                    small_button("COPY", Some(Message::CopyHistory(index))),
                    Space::new().width(Fill),
                    iced::widget::button(text("×").size(11).font(theme::MONO))
                        .padding([3, 8])
                        .style(theme::destructive)
                        .on_press(Message::DeleteHistory(index)),
                ]
                .spacing(4)
                .align_y(Center),
                rule::horizontal(1).style(theme::hairline),
            ]
            .spacing(3),
        );
    }

    if studio.history.is_empty() {
        list = list.push(super::faint(
            "Every statement this session — typed or issued by a dialog — appears here with how \
             long it took.",
        ));
    }

    let failures = studio
        .history
        .iter()
        .filter(|entry| entry.outcome.failed())
        .count();

    let heading = row![
        text(format!("{}", studio.history.len()))
            .size(10)
            .font(theme::MONO)
            .color(theme::TEXT_DIM),
        text(if failures > 0 { format!("· {} failed", failures) } else { String::new() })
            .size(10)
            .font(theme::MONO)
            .color(theme::BAD),
        Space::new().width(Fill),
        small_button(
            "EXPORT",
            (!studio.history.is_empty()).then_some(Message::ExportHistory),
        ),
        small_button(
            "CLEAR ALL",
            (!studio.history.is_empty()).then_some(Message::ClearHistory),
        ),
    ]
    .spacing(6)
    .align_y(Center);

    container(framed(
        "HISTORY",
        Some(heading.into()),
        scrollable(list).height(Fill).style(theme::scroller).into(),
    ))
    .width(Length::Fixed(360.0))
    .into()
}
