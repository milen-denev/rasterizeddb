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

use iced::widget::{Space, column, container, row, rule, scrollable, text, text_editor};
use iced::{Center, Element, Fill, Length};

use crate::app::{Message, Studio, Template};
use crate::export::Format;
use crate::model::{Outcome, age, duration};
use crate::theme;

use super::{framed, plain_cell, result_grid, small_button};

pub fn view(studio: &Studio) -> Element<'_, Message> {
    let editor = text_editor(&studio.editor)
        .on_action(Message::EditorAction)
        .font(theme::MONO)
        .size(13)
        .padding(10)
        .height(Length::Fixed(230.0))
        .key_binding(|press| {
            use iced::keyboard::key;
            // Ctrl/Cmd+Enter runs the whole buffer. A bare Enter has to stay a
            // newline: this is an editor for statements that span lines, and a
            // console that ran on Enter would fire half a statement every time.
            if press.key == iced::keyboard::Key::Named(key::Named::Enter)
                && (press.modifiers.command() || press.modifiers.control())
            {
                return Some(text_editor::Binding::Custom(Message::RunQuery));
            }
            text_editor::Binding::from_key_press(press)
        });

    let panel = column![
        toolbar(studio),
        container(editor).style(theme::panel).width(Fill),
        footer(studio),
        rule::horizontal(1).style(theme::hairline),
        results(studio),
    ]
    .spacing(8);

    row![
        container(framed("SQL", None, panel.into())).width(Fill),
        history(studio),
    ]
    .spacing(10)
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
                small_button("RUN SELECTION", Some(Message::RunSelection)),
                small_button("FORMAT", Some(Message::FormatSql)),
                small_button("CLEAR", Some(Message::ClearEditor)),
                Space::new().width(Fill),
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

fn footer(studio: &Studio) -> Element<'_, Message> {
    row![
        text(format!("{} lines", studio.editor.line_count()))
            .size(10)
            .font(theme::MONO)
            .color(theme::TEXT_FAINT),
        Space::new().width(Fill),
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
