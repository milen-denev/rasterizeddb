//! The data browser: a page of rows, editable in place.
//!
//! # What makes an edit safe
//!
//! Every write here is addressed by the table's primary key, worked out when
//! the edit begins. A table without a single-column key is shown read-only and
//! says why, because the alternative — matching an `UPDATE` on the row's own
//! values — silently writes to every duplicate of the row that was clicked. A
//! console that did that would corrupt data while reporting success, and it
//! would report `UPDATE 3` in a way nobody reads.

use iced::widget::{Space, column, container, row, text, text_input};
use iced::{Center, Element, Fill, Length};

use crate::app::{Message, ModalKind, Studio};
use crate::model::*;
use crate::theme;

use super::{framed, plain_cell, result_grid, small_button};

pub fn view(studio: &Studio) -> Element<'_, Message> {
    // Borrowed from the state rather than cloned: the grid keeps a reference
    // to it for as long as the returned element lives.
    let Some(table) = studio.selected_table.as_deref() else {
        return framed(
            "DATA",
            None,
            container(text("Select a table.").size(13).color(theme::TEXT_FAINT))
                .center_x(Fill)
                .center_y(Fill)
                .into(),
        );
    };

    let key = studio.row_key();
    let editable = key.is_some();

    let numeric: Vec<bool> = studio
        .data
        .columns
        .iter()
        .map(|name| {
            studio
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(name))
                .is_some_and(ColumnInfo::is_numeric)
        })
        .collect();

    let editing = studio.editing.clone();
    let numeric_for_grid = numeric.clone();

    let grid = result_grid(
        &studio.data,
        // The server did the sorting: this grid holds one page of the table,
        // and reordering the page here would rearrange a hundred arbitrary rows
        // under a heading that claims to order the whole thing.
        super::GridLayout::new(studio, format!("{}{}", super::DATA_SCOPE, table))
            .virtualised(studio.data_viewport, Message::DataScrolled)
            .sorted_by_server(),
        move |index| numeric.get(index).copied().unwrap_or(false),
        move |row_index, column_index, value| {
            let is_editing = editing
                .as_ref()
                .is_some_and(|e| e.row == row_index && e.column == column_index);

            if is_editing {
                let edit = editing.as_ref().unwrap();
                // One line, because the grid is virtualised and every row has
                // to be the same height — a taller editing row would put every
                // row below it in the wrong place. It also stops the grid
                // jumping when a cell is clicked, which the two-line version
                // did.
                return container(
                    row![
                        text_input("", &edit.value)
                            .on_input(Message::EditValue)
                            .on_submit(Message::CommitEdit)
                            .padding([1, 4])
                            .size(11)
                            .font(theme::MONO)
                            .style(theme::input)
                            .width(Fill),
                        iced::widget::button(
                            text("∅").size(10).font(theme::MONO)
                        )
                        .padding([1, 5])
                        .style(if edit.null { theme::primary } else { theme::secondary })
                        .on_press(Message::EditNull(!edit.null)),
                        iced::widget::button(text("✓").size(10).font(theme::MONO))
                            .padding([1, 5])
                            .style(theme::primary)
                            .on_press(Message::CommitEdit),
                        iced::widget::button(text("✗").size(10).font(theme::MONO))
                            .padding([1, 5])
                            .style(theme::secondary)
                            .on_press(Message::CancelEdit),
                    ]
                    .spacing(2)
                    .align_y(Center),
                )
                .padding([1, 2])
                .into();
            }

            let content = plain_cell(
                value,
                numeric_for_grid.get(column_index).copied().unwrap_or(false),
            );

            if !editable {
                return content;
            }

            iced::widget::button(content)
                .padding(0)
                .width(Fill)
                .style(theme::cell(false))
                .on_press(Message::BeginEdit { row: row_index, column: column_index })
                .into()
        },
        editable.then(|| delete_column()),
    );

    let body = column![
        key_note(&key, table),
        insert_row(studio),
        grid,
        footer(studio),
    ]
    .spacing(8);

    framed(
        format!("DATA · {}", table),
        Some(
            row![
                small_button("INSERT ROW", editable.then_some(Message::BeginInsert)),
                small_button("ADD COLUMN", Some(Message::OpenModal(ModalKind::AddColumn))),
                small_button("REFRESH", Some(Message::SelectTable(table.to_string()))),
                // Labelled for what it is. Exporting page 3 and getting a file
                // called `users.csv` invites someone to think they have the
                // table; to export all of it, run a SELECT and export that.
                text("export page").size(10).color(theme::TEXT_FAINT),
                small_button("CSV", Some(Message::ExportData(crate::export::Format::Csv))),
                small_button("JSON", Some(Message::ExportData(crate::export::Format::Json))),
                small_button("SQL", Some(Message::ExportData(crate::export::Format::Inserts))),
            ]
            .spacing(6)
            .align_y(Center)
            .into(),
        ),
        body.into(),
    )
}

/// Says, once and plainly, whether this table can be edited.
fn key_note<'a>(key: &Option<RowKey>, table: &str) -> Element<'a, Message> {
    match key {
        Some(key) => container(
            text(key.describe())
                .size(11)
                .font(theme::MONO)
                .color(if key.source == KeySource::PrimaryKey {
                    theme::TEXT_FAINT
                } else {
                    theme::WARN
                }),
        )
        .into(),
        None => container(
            text(format!(
                "Nothing identifies a single row of `{}`: it declares no primary key and has no \
                 UNIQUE NOT NULL column. This page is read-only, and its order is whatever the \
                 server returned — so paging can repeat a row or skip one. Add a primary key or a \
                 UNIQUE NOT NULL column to edit here.",
                table
            ))
            .size(11),
        )
        .padding(8)
        .width(Fill)
        .style(theme::error_box)
        .into(),
    }
}

fn insert_row(studio: &Studio) -> Element<'_, Message> {
    let Some(values) = &studio.inserting else {
        return Space::new().height(0).into();
    };

    let mut fields = row![].spacing(6);
    for (index, name) in studio.data.columns.iter().enumerate() {
        fields = fields.push(
            column![
                text(name.as_str()).size(10).font(theme::MONO).color(theme::TEXT_DIM),
                text_input("", values.get(index).map(String::as_str).unwrap_or(""))
                    .on_input(move |v| Message::InsertValue(index, v))
                    .padding([4, 6])
                    .size(12)
                    .font(theme::MONO)
                    .style(theme::input)
                    .width(Length::Fixed(140.0)),
            ]
            .spacing(2),
        );
    }

    container(
        column![
            text("NEW ROW — blank fields are left out, so a DEFAULT or a generated key still applies. Type NULL for an explicit null.")
                .size(10)
                .color(theme::TEXT_FAINT),
            iced::widget::scrollable(fields)
                .direction(iced::widget::scrollable::Direction::Horizontal(
                    iced::widget::scrollable::Scrollbar::default(),
                ))
                .style(theme::scroller),
            // Both the same width, and CANCEL actually cancels: it used to
            // send `BeginInsert`, which re-opened the row it was supposed to
            // close — a button that looked like it did nothing.
            row![
                iced::widget::button(
                    text("INSERT")
                        .size(12)
                        .font(theme::MONO)
                        .align_x(iced::alignment::Horizontal::Center)
                        .width(Fill)
                )
                .padding([8, 0])
                .width(Length::Fixed(120.0))
                .style(theme::primary)
                .on_press(Message::CommitInsert),
                iced::widget::button(
                    text("CANCEL")
                        .size(12)
                        .font(theme::MONO)
                        .align_x(iced::alignment::Horizontal::Center)
                        .width(Fill)
                )
                .padding([8, 0])
                .width(Length::Fixed(120.0))
                .style(theme::secondary)
                .on_press(Message::CancelInsert),
            ]
            .spacing(6),
        ]
        .spacing(6),
    )
    .padding(8)
    .width(Fill)
    .style(theme::tile)
    .into()
}

fn footer(studio: &Studio) -> Element<'_, Message> {
    let first = studio.data_page * studio.page_size;
    let shown = studio.data.rows.len();
    let range = if shown == 0 {
        "no rows".to_string()
    } else {
        format!("rows {}–{}", count(first as u64 + 1), count((first + shown) as u64))
    };

    let total = match studio.data_total {
        Some(total) => format!("of {}", count(total)),
        None => String::new(),
    };

    row![
        small_button("◀", (studio.data_page > 0).then_some(Message::PagePrev)),
        small_button(
            "▶",
            (shown == studio.page_size).then_some(Message::PageNext),
        ),
        text(range).size(11).font(theme::MONO).color(theme::TEXT_DIM),
        text(total).size(11).font(theme::MONO).color(theme::TEXT_FAINT),
        Space::new().width(Fill),
        text("page size").size(10).color(theme::TEXT_FAINT),
        text_input("", &studio.page_size.to_string())
            .on_input(Message::PageSize)
            .padding([3, 6])
            .size(11)
            .font(theme::MONO)
            .style(theme::input)
            .width(Length::Fixed(70.0)),
    ]
    .spacing(8)
    .align_y(Center)
    .into()
}

/// The trailing "delete" cell of each row.
///
/// A free function rather than a closure written inline: the grid wants a
/// `Box<dyn Fn(usize) -> Element<'a, _>>`, and an inline closure has no way to
/// name the lifetime of the elements it produces. Generic over that lifetime
/// rather than `'static`, because a boxed `Fn` returning `Element<'static, _>`
/// is not a subtype of one returning `Element<'a, _>` — trait objects do not
/// vary in their output type.
fn delete_column<'a>() -> Box<dyn Fn(usize) -> Element<'a, Message> + 'a> {
    Box::new(|row_index: usize| {
        container(
            iced::widget::button(text("DEL").size(10).font(theme::MONO))
                .padding([2, 6])
                .style(theme::destructive)
                .on_press(Message::DeleteSelectedRow(row_index)),
        )
        .padding([2, 6])
        .into()
    })
}
