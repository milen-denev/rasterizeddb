//! Two panes: every table with its sizes, and everything about the one
//! selected.
//!
//! The list arrives in the server's order and stays in it until a header is
//! clicked. "Which table is the big one" is the question this pane exists to
//! answer and it is one click away now; the third click puts the list back
//! into `SHOW TABLES` order, so the two never silently disagree about what
//! "the first table" means.

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

    let mut tables = studio.filtered_tables();
    super::sort_rows(&layout, &mut tables, |table, column| match column {
        "storage" => super::SortKey::text(&table.storage),
        "row_slots" => super::SortKey::number(table.row_slots as f64),
        "columns" => super::SortKey::number(table.columns as f64),
        "data" => super::SortKey::number(table.data_bytes as f64),
        "index" => super::SortKey::number(table.index_bytes as f64),
        "total" => super::SortKey::number(table.total_bytes as f64),
        "reclaimable" => super::SortKey::number(table.reclaimable_bytes as f64),
        "primary_key" => super::SortKey::text(&table.primary_key),
        _ => super::SortKey::text(&table.name),
    });

    let mut body = column![].spacing(0);
    for (index, table) in tables.into_iter().enumerate() {
        let active = studio.selected_table.as_deref() == Some(table.name.as_str());
        let line = super::grid_row(
            &widths,
            vec![
                super::grid_cell(table.name.clone(), false, theme::TEXT),
                storage_cell(table),
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
        super::GridColumn::new("storage", "STORAGE", 116.0),
        super::GridColumn::new("row_slots", "ROW SLOTS", 96.0).right(),
        super::GridColumn::new("columns", "COLS", 60.0).right(),
        super::GridColumn::new("data", "DATA", 92.0).right(),
        super::GridColumn::new("index", "INDEX", 92.0).right(),
        super::GridColumn::new("total", "TOTAL", 92.0).right(),
        super::GridColumn::new("reclaimable", "RECLAIMABLE", 108.0).right(),
        super::GridColumn::new("primary_key", "PRIMARY KEY", 150.0),
    ]
}

/// What is holding the table, as a badge.
///
/// Coloured by whether the contents survive the server going away, not by which
/// backend it is: a table in memory and a PostgreSQL `UNLOGGED` table are
/// different technologies and the same fact about a restart, which is the thing
/// somebody scanning this list needs to notice. A server that does not report
/// the column gets no badge rather than a wrong one.
fn storage_cell(table: &TableInfo) -> Element<'_, Message> {
    if table.storage.is_empty() {
        return super::grid_cell("—", false, theme::TEXT_FAINT);
    }
    container(super::pill(
        table.storage.to_ascii_uppercase(),
        if table.durable() { theme::TEXT_DIM } else { theme::WARN },
    ))
    .padding([3, 6])
    .into()
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
        super::GridColumn::new("flags", "", 170.0).unsortable(),
        super::GridColumn::new("default", "DEFAULT", 160.0),
    ];
    let widths = super::column_widths(&layout, &specs);

    let mut infos: Vec<&ColumnInfo> = studio.columns.iter().collect();
    super::sort_rows(&layout, &mut infos, |info, column| match column {
        "type" => super::SortKey::text(&info.data_type),
        "default" => super::SortKey::text(&info.default),
        _ => super::SortKey::text(&info.name),
    });

    let mut column_rows = column![].spacing(0);
    for (index, info) in infos.into_iter().enumerate() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::GridSort;

    fn table(name: &str, total: u64, storage: &str, key: &str) -> TableInfo {
        TableInfo {
            name: name.into(),
            storage: storage.into(),
            row_slots: total / 8,
            columns: 4,
            data_bytes: total / 2,
            index_bytes: total / 4,
            total_bytes: total,
            reclaimable_bytes: 0,
            primary_key: key.into(),
            immutable: false,
            indexed: true,
        }
    }

    fn studio_with_tables() -> Studio {
        let (mut studio, _) = Studio::new(None);
        studio.tables = vec![
            table("users", 4096, "file", "id"),
            table("events", 1_048_576, "memory", ""),
            table("audit", 512, "s3", "uuid"),
        ];
        studio.selected_table = Some("users".into());
        studio.columns = vec![ColumnInfo {
            position: 1,
            name: "id".into(),
            data_type: "BIGINT".into(),
            nullable: false,
            unique: true,
            primary_key: true,
            default: String::new(),
            indexed: true,
            json_table: String::new(),
        }];
        studio
    }

    /// Sorting must not lose a row, whichever column it is asked for — a
    /// table list that quietly dropped one is a console reporting a database
    /// that does not exist.
    #[test]
    fn every_column_sorts_and_keeps_every_table() {
        let mut studio = studio_with_tables();
        for column in columns() {
            for descending in [false, true] {
                studio.sort.insert(
                    "tables".to_string(),
                    GridSort { column: column.key.to_string(), descending },
                );
                let layout = super::super::GridLayout::new(&studio, "tables");
                let mut tables = studio.filtered_tables();
                let before = tables.len();
                super::super::sort_rows(&layout, &mut tables, |table, key| match key {
                    "storage" => super::super::SortKey::text(&table.storage),
                    _ => super::super::SortKey::text(&table.name),
                });
                assert_eq!(tables.len(), before, "{} lost a row", column.key);
            }
        }
    }

    /// The question this pane is opened to answer: which table is the big one.
    #[test]
    fn sorting_by_size_puts_the_biggest_table_first() {
        let mut studio = studio_with_tables();
        studio.sort.insert(
            "tables".to_string(),
            GridSort { column: "total".into(), descending: true },
        );
        let layout = super::super::GridLayout::new(&studio, "tables");
        let mut tables = studio.filtered_tables();
        super::super::sort_rows(&layout, &mut tables, |table, _| {
            super::super::SortKey::number(table.total_bytes as f64)
        });
        assert_eq!(tables[0].name, "events");
        assert_eq!(tables[2].name, "audit");
    }

    /// Both panes render with a sort applied. A header declaring nine columns
    /// over rows supplying eight is a runtime fault, and the sorted path is a
    /// path nobody looks at until it is already wrong.
    #[test]
    fn both_panes_render_sorted() {
        let mut studio = studio_with_tables();
        for column in columns() {
            studio.sort.insert(
                "tables".to_string(),
                GridSort { column: column.key.to_string(), descending: true },
            );
            let _ = view(&studio);
        }
        studio.sort.insert(
            "columns:users".to_string(),
            GridSort { column: "type".into(), descending: false },
        );
        let _ = view(&studio);
    }
}
