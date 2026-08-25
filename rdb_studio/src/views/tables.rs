//! Two panes: every table with its sizes, and everything about the one
//! selected.
//!
//! The list is sorted by the server. Sorting it here by size would be more
//! useful and is deliberately not done yet — a console that re-sorts on the
//! client shows a different order from the one `SHOW TABLES` returns, which
//! makes the two disagree about what "the first table" means.

use iced::widget::{column, container, row, rule, text};
use iced::{Element, Fill, Length};

use crate::app::{Message, ModalKind, Refresh, Studio};
use crate::model::*;
use crate::theme;

use super::{confirm, framed, section_label, small_button};

pub fn view(studio: &Studio) -> Element<'_, Message> {
    row![list(studio), detail(studio)].spacing(10).height(Fill).into()
}

fn list(studio: &Studio) -> Element<'_, Message> {
    let layout = super::GridLayout::new(studio, "tables");
    let columns = columns();
    let widths = super::column_widths(&layout, &columns);

    let mut body = column![].spacing(0);
    for (index, table) in studio.filtered_tables().into_iter().enumerate() {
        let active = studio.selected_table.as_deref() == Some(table.name.as_str());
        let line = super::grid_row(
            &widths,
            vec![
                super::grid_cell(table.name.clone(), false, theme::TEXT),
                super::grid_cell(count(table.row_slots), true, theme::TEXT),
                super::grid_cell(count(table.columns), true, theme::TEXT_DIM),
                super::grid_cell(bytes(table.data_bytes), true, theme::TEXT),
                super::grid_cell(bytes(table.index_bytes), true, theme::TEXT_DIM),
                super::grid_cell(bytes(table.total_bytes), true, theme::TEXT),
                super::grid_cell(
                    bytes(table.reclaimable_bytes),
                    true,
                    if table.reclaimable_bytes > 0 { theme::WARN } else { theme::TEXT_FAINT },
                ),
                super::grid_cell(
                    if table.primary_key.is_empty() {
                        "—".to_string()
                    } else {
                        table.primary_key.clone()
                    },
                    false,
                    theme::TEXT_DIM,
                ),
            ],
        );

        // The whole row is the target: picking a table is what this grid is
        // for, and the name alone is a smaller target than the row someone is
        // already looking at.
        let selectable = iced::widget::button(line)
            .width(Length::Fixed(super::grid_total(&widths)))
            .padding(0)
            .style(theme::tree_item(active))
            .on_press(Message::SelectTable(table.name.clone()));

        body = body.push(if active {
            container(selectable).into()
        } else {
            super::grid_stripe(index, selectable.into())
        });
    }

    framed(
        format!("TABLES · {}", studio.tables.len()),
        Some(small_button("NEW TABLE", Some(Message::OpenModal(ModalKind::CreateTable)))),
        super::grid_frame(
            &layout,
            &widths,
            super::grid_header(&layout, &columns, &widths),
            body.into(),
        ),
    )
}

/// The table list's columns.
///
/// `PRIMARY KEY` is back now that the grid scrolls and every column can be
/// dragged; it was dropped when the list had to fit inside a fixed pane, which
/// is no longer the constraint.
fn columns() -> Vec<super::GridColumn> {
    vec![
        super::GridColumn::new("table", "TABLE", 200.0),
        super::GridColumn::new("row_slots", "ROW SLOTS", 96.0).right(),
        super::GridColumn::new("columns", "COLS", 60.0).right(),
        super::GridColumn::new("data", "DATA", 92.0).right(),
        super::GridColumn::new("index", "INDEX", 92.0).right(),
        super::GridColumn::new("total", "TOTAL", 92.0).right(),
        super::GridColumn::new("reclaimable", "RECLAIMABLE", 108.0).right(),
        super::GridColumn::new("primary_key", "PRIMARY KEY", 150.0),
    ]
}

fn detail(studio: &Studio) -> Element<'_, Message> {
    let Some(name) = studio.selected_table.clone() else {
        return container(
            text("Select a table.").size(13).color(theme::TEXT_FAINT),
        )
        .width(Length::Fixed(560.0))
        .center_y(Fill)
        .center_x(Fill)
        .style(theme::panel)
        .into();
    };

    let layout = super::GridLayout::new(studio, format!("columns:{}", name));
    let specs = vec![
        super::GridColumn::new("column", "COLUMN", 170.0),
        super::GridColumn::new("type", "TYPE", 140.0),
        super::GridColumn::new("flags", "", 170.0),
        super::GridColumn::new("default", "DEFAULT", 160.0),
    ];
    let widths = super::column_widths(&layout, &specs);

    let mut column_rows = column![].spacing(0);
    for (index, info) in studio.columns.iter().enumerate() {
        let mut flags = row![].spacing(4);
        if info.primary_key {
            flags = flags.push(super::pill("PK", theme::RUST));
        }
        if info.unique && !info.primary_key {
            flags = flags.push(super::pill("UNQ", theme::TEXT_DIM));
        }
        if !info.nullable {
            flags = flags.push(super::pill("NN", theme::TEXT_FAINT));
        }
        if info.indexed {
            flags = flags.push(super::pill("IDX", theme::OK));
        }
        if !info.json_table.is_empty() {
            flags = flags.push(super::pill("JSON", theme::WARN));
        }

        let line = super::grid_row(
            &widths,
            vec![
                super::grid_cell(info.name.clone(), false, theme::TEXT),
                super::grid_cell(info.data_type.clone(), false, theme::TEXT_DIM),
                container(flags).padding([3, 6]).into(),
                super::grid_cell(info.default.clone(), false, theme::TEXT_FAINT),
            ],
        );
        column_rows = column_rows.push(super::grid_stripe(
            index,
            container(line)
                .width(Length::Fixed(super::grid_total(&widths)))
                .into(),
        ));
    }
    let columns = super::grid_frame(
        &layout,
        &widths,
        super::grid_header(&layout, &specs, &widths),
        column_rows.into(),
    );

    let metrics = super::metric_grid(studio, format!("stats:{}", name), &studio.table_stats);

    let actions = row![
        small_button("ADD COLUMN", Some(Message::OpenModal(ModalKind::AddColumn))),
        small_button("BROWSE", Some(Message::SelectTab(crate::app::Tab::Data))),
        iced::widget::button(text("DROP TABLE").size(11).font(theme::MONO))
            .padding([5, 10])
            .style(theme::destructive)
            .on_press(confirm(
                "Drop table",
                format!("This permanently removes `{}` and every row in it.", name),
                format!("DROP TABLE {}", crate::dialect::quote_ident(&name)),
                Refresh::Tables,
            )),
    ]
    .spacing(6);

    let body = column![
        actions,
        section_label("COLUMNS"),
        container(columns).height(Length::FillPortion(1)),
        rule::horizontal(1).style(theme::hairline),
        section_label("STATISTICS"),
        container(metrics).height(Length::FillPortion(1)),
    ]
    .spacing(6);

    container(framed(name.to_uppercase(), None, body.into()))
        .width(Length::Fixed(560.0))
        .into()
}
