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

use crate::app::{DetailSection, Message, ModalKind, Refresh, Studio};
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

/// The table pane's width. Wide enough that the index grid's drop buttons
/// are on screen without scrolling, which is where they are needed.
const DETAIL_WIDTH: f32 = 700.0;

fn detail(studio: &Studio) -> Element<'_, Message> {
    let Some(name) = studio.selected_table.clone() else {
        return container(
            text("Select a table.").size(13).color(theme::TEXT_FAINT),
        )
        .width(Length::Fixed(DETAIL_WIDTH))
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

    let partition_key: Vec<&str> = studio
        .partition_key()
        .map(|k| k.split(',').map(str::trim).filter(|k| !k.is_empty()).collect())
        .unwrap_or_default();

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
        if let Some(at) = partition_key.iter().position(|k| k.eq_ignore_ascii_case(&info.name)) {
            flags = flags.push(super::pill(format!("PART {}", at + 1), theme::RUST));
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

    let partitioned = studio.partition_key().is_some();
    // Refused by the server until partitioned tables take metadata DDL
    // (docs/partitioning.md, phase 3); a button that cannot work is not
    // offered as though it could.
    let new_index = (!partitioned).then_some(Message::OpenModal(ModalKind::CreateIndex));
    let mut actions = row![
        small_button("ADD COLUMN", (!partitioned).then_some(Message::OpenModal(ModalKind::AddColumn))),
        small_button("NEW INDEX", new_index),
    ]
    .spacing(6);
    if studio.flavor() == crate::model::Flavor::Rasterized {
        actions = actions.push(small_button("CHECK INDEXES", Some(Message::CheckIndexes)));
    }
    let actions = actions.push(row![
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
    .spacing(6));

    let mut body = column![actions].spacing(6);
    if partitioned {
        body = body.push(
            text(format!(
                "Partitioned by hash ({}) · {} partitions, {} with rows. Columns, indexes and the \
                 key cannot be changed on a partitioned table yet; the server refuses ALTER and \
                 CREATE INDEX on one.",
                studio.partition_key().unwrap_or_default(),
                stat(studio, "partitions_declared"),
                stat(studio, "partitions_materialized"),
            ))
            .size(11)
            .color(theme::TEXT_DIM),
        );
    }
    let body = body
        .push(section_label("COLUMNS"))
        .push(container(columns).height(Length::FillPortion(1)))
        .push(rule::horizontal(1).style(theme::hairline))
        .push(sections(studio, partitioned))
        .push(container(lower(studio, &name)).height(Length::FillPortion(1)));

    container(framed(name.to_uppercase(), None, body.into()))
        .width(Length::Fixed(DETAIL_WIDTH))
        .into()
}

fn stat<'a>(studio: &'a Studio, metric: &str) -> &'a str {
    studio
        .table_stats
        .iter()
        .find(|s| s.metric == metric)
        .map(|s| s.value.as_str())
        .unwrap_or("?")
}

/// The switch between the lower half's lenses. PARTITIONS is there only for
/// a partitioned table, which is the only kind that has any.
fn sections(studio: &Studio, partitioned: bool) -> Element<'_, Message> {
    let mut choices = vec![(DetailSection::Indexes, format!("INDEXES · {}", studio.indexes.len()))];
    if partitioned {
        choices.push((DetailSection::Partitions, format!("PARTITIONS · {}", studio.partitions.len())));
    }
    choices.push((DetailSection::Statistics, "STATISTICS".to_string()));

    let mut strip = row![].spacing(0);
    for (section, label) in choices {
        let active = studio.detail_section == section;
        strip = strip.push(
            iced::widget::button(
                column![
                    text(label).size(10).font(theme::MONO),
                    container(iced::widget::Space::new().height(2)).width(Fill).style(if active {
                        theme::accent_underline
                    } else {
                        theme::no_underline
                    }),
                ]
                .spacing(4),
            )
            .padding([5, 10])
            .style(theme::tab(active))
            .on_press(Message::SelectDetailSection(section)),
        );
    }
    strip.into()
}

fn lower<'a>(studio: &'a Studio, name: &str) -> Element<'a, Message> {
    match studio.detail_section {
        DetailSection::Indexes => indexes(studio, name),
        DetailSection::Partitions => partitions(studio, name),
        DetailSection::Statistics => {
            super::metric_grid(studio, format!("stats:{}", name), &studio.table_stats)
        }
    }
}

/// Every index the table has, what kind it is, and how it would go.
///
/// A constraint's index goes with its constraint, so a unique one or the
/// primary key's offers DROP CONSTRAINT rather than DROP INDEX, which both
/// servers refuse for it. Every drop is checked afterwards: a RasterizedDB
/// server before fc3849c answered it with success and kept the index.
fn indexes<'a>(studio: &'a Studio, table: &str) -> Element<'a, Message> {
    let layout = super::GridLayout::new(studio, format!("indexes:{}", table));
    let specs = vec![
        super::GridColumn::new("name", "INDEX", 170.0),
        super::GridColumn::new("kind", "KIND", 76.0),
        super::GridColumn::new("columns", "COLUMNS", 170.0),
        super::GridColumn::new("owner", "OWNED BY", 110.0),
        super::GridColumn::new("action", "", 150.0).unsortable(),
    ];
    let widths = super::column_widths(&layout, &specs);

    let mut list: Vec<&IndexInfo> = studio.indexes.iter().collect();
    super::sort_rows(&layout, &mut list, |index, column| match column {
        "kind" => super::SortKey::text(index.kind()),
        "columns" => super::SortKey::text(&index.columns),
        "owner" => super::SortKey::text(owner_label(index.owner)),
        _ => super::SortKey::text(&index.name),
    });

    let mut body = column![].spacing(0);
    for (row_index, index) in list.into_iter().enumerate() {
        let drop = |title: &str, body: String, statement: String| Message::DropIndex {
            title: title.to_string(),
            body,
            statement,
            index: index.name.clone(),
        };
        let action: Element<'_, Message> = match index.owner {
            IndexOwner::Primary => drop_button(
                "DROP PRIMARY KEY",
                drop(
                    "Drop primary key",
                    format!(
                        "This removes the primary key `{}` and its index: `{}` will accept \
                         duplicates and the data browser can edit rows here only while a UNIQUE \
                         NOT NULL column remains.",
                        index.name, index.columns
                    ),
                    crate::dialect::drop_constraint_sql(table, &index.name),
                ),
            ),
            IndexOwner::Declared => drop_button(
                "DROP",
                drop(
                    "Drop index",
                    format!(
                        "This removes the index `{}`. Queries it answered scan instead; no rows change.",
                        index.name
                    ),
                    crate::dialect::drop_index_sql(&index.name),
                ),
            ),
            IndexOwner::Unique => drop_button(
                "DROP CONSTRAINT",
                drop(
                    "Drop unique constraint",
                    format!(
                        "This removes the UNIQUE constraint `{}` and its index: `{}` will accept \
                         duplicates from now on.",
                        index.name, index.columns
                    ),
                    crate::dialect::drop_constraint_sql(table, &index.name),
                ),
            ),
        };
        let line = super::grid_row(
            &widths,
            vec![
                super::grid_cell(index.name.clone(), false, theme::TEXT),
                container(super::pill(index.kind().to_ascii_uppercase(), kind_colour(index))).padding([3, 6]).into(),
                super::grid_cell(index.columns.clone(), false, theme::TEXT_DIM),
                super::grid_cell(
                    owner_label(index.owner),
                    false,
                    if index.owner == IndexOwner::Declared { theme::TEXT_FAINT } else { theme::TEXT_DIM },
                ),
                action,
            ],
        );
        body = body.push(super::grid_stripe(
            row_index,
            container(line).width(Length::Fixed(super::grid_total(&widths))).into(),
        ));
    }
    if studio.indexes.is_empty() {
        body = body.push(super::faint("No indexes. NEW INDEX adds one."));
    }

    super::grid_frame(&layout, &widths, super::grid_header(&layout, &specs, &widths), body.into())
}

/// A directory shortened from the front, so the part that tells two disks
/// apart — the end — stays. Wrapped instead, a long path makes every row of
/// the partitions grid three lines tall.
fn path_tail(path: &str, limit: usize) -> String {
    let count = path.chars().count();
    if count <= limit {
        return path.to_string();
    }
    let tail: String = path.chars().skip(count - (limit - 1)).collect();
    format!("…{}", tail)
}

fn owner_label(owner: IndexOwner) -> &'static str {
    match owner {
        IndexOwner::Declared => "CREATE INDEX",
        IndexOwner::Unique => "UNIQUE",
        IndexOwner::Primary => "PRIMARY KEY",
    }
}

fn kind_colour(index: &IndexInfo) -> iced::Color {
    match index.kind() {
        "hash" => theme::WARN,
        "gin" | "path" => theme::OK,
        _ => theme::TEXT_DIM,
    }
}

fn drop_button(label: &'static str, on_press: Message) -> Element<'static, Message> {
    container(
        iced::widget::button(text(label).size(10).font(theme::MONO))
            .padding([2, 8])
            .style(theme::destructive)
            .on_press(on_press),
    )
    .padding([2, 6])
    .into()
}

/// The partitions of a partitioned table: which slice of the hash space each
/// holds, whether it has files yet, and how big it is.
fn partitions<'a>(studio: &'a Studio, table: &str) -> Element<'a, Message> {
    let layout = super::GridLayout::new(studio, format!("partitions:{}", table));
    let specs = vec![
        super::GridColumn::new("pid", "PID", 56.0).right(),
        super::GridColumn::new("state", "STATE", 130.0),
        super::GridColumn::new("rows", "ROW SLOTS", 96.0).right(),
        super::GridColumn::new("size", "SIZE", 92.0).right(),
        super::GridColumn::new("share", "HASH SHARE", 96.0).right(),
        super::GridColumn::new("lo", "FROM", 180.0),
        super::GridColumn::new("location", "LOCATION", 320.0),
    ];
    let widths = super::column_widths(&layout, &specs);

    let mut list: Vec<&PartitionInfo> = studio
        .partitions
        .iter()
        .filter(|p| studio.show_empty_partitions || p.materialized())
        .collect();
    let shown = list.len();
    super::sort_rows(&layout, &mut list, |p, column| match column {
        "state" => super::SortKey::text(&p.state),
        "rows" => super::SortKey::number(p.row_slots as f64),
        "size" => super::SortKey::number(p.total_bytes as f64),
        "share" => super::SortKey::number(p.share().unwrap_or(0.0)),
        "lo" => super::SortKey::text(&p.lo),
        "location" => super::SortKey::text(&p.location),
        _ => super::SortKey::number(p.pid as f64),
    });

    let mut body = column![].spacing(0);
    for (row_index, p) in list.into_iter().enumerate() {
        let dim = if p.materialized() { theme::TEXT } else { theme::TEXT_FAINT };
        let line = super::grid_row(
            &widths,
            vec![
                super::grid_cell(p.pid.to_string(), true, dim),
                container(super::pill(
                    p.state.to_ascii_uppercase(),
                    if p.materialized() { theme::OK } else { theme::TEXT_FAINT },
                ))
                .padding([3, 6])
                .into(),
                super::grid_cell(count(p.row_slots), true, dim),
                super::grid_cell(bytes(p.total_bytes), true, dim),
                super::grid_cell(
                    p.share().map(|s| format!("{:.2}%", s * 100.0)).unwrap_or_else(|| "—".into()),
                    true,
                    theme::TEXT_DIM,
                ),
                super::grid_cell(p.lo.clone(), false, theme::TEXT_FAINT),
                super::grid_cell(
                    if p.location.is_empty() { "—".to_string() } else { path_tail(&p.location, 44) },
                    false,
                    theme::TEXT_FAINT,
                ),
            ],
        );
        body = body.push(super::grid_stripe(
            row_index,
            container(line).width(Length::Fixed(super::grid_total(&widths))).into(),
        ));
    }

    if shown == 0 {
        body = body.push(super::faint(format!(
            "No partition holds rows yet; SHOW ALL lists the {} declared.",
            studio.partitions.len()
        )));
    }

    let toggle = row![
        super::toggle(
            format!("SHOW ALL {} · {} with rows", studio.partitions.len(),
                studio.partitions.iter().filter(|p| p.materialized()).count()),
            studio.show_empty_partitions,
            Message::ShowEmptyPartitions,
        ),
    ];
    column![
        toggle,
        super::grid_frame(&layout, &widths, super::grid_header(&layout, &specs, &widths), body.into()),
    ]
    .spacing(6)
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

    /// Every lens of the lower half, and every dialog the pane opens, built
    /// with data in them and each grid sorted by each of its columns.
    #[test]
    fn the_index_and_partition_lenses_render() {
        use crate::app::{DetailSection, Message, ModalKind};
        let mut studio = studio_with_tables();
        studio.table_stats = vec![Stat {
            metric: "partition_key".into(),
            value: "id".into(),
            detail: String::new(),
        }];
        studio.indexes = vec![
            IndexInfo {
                name: "users_pkey".into(),
                method: "btree".into(),
                columns: "id".into(),
                unique: true,
                owner: IndexOwner::Primary,
                definition: String::new(),
            },
            IndexInfo {
                name: "by_doc".into(),
                method: "btree".into(),
                columns: "((doc->>'k'))".into(),
                unique: false,
                owner: IndexOwner::Declared,
                definition: String::new(),
            },
        ];
        studio.partitions = vec![PartitionInfo {
            pid: 0,
            lo: "0x0000000000000000".into(),
            hi: "0x10000000000000000".into(),
            location: String::new(),
            state: "unmaterialized".into(),
            row_slots: 0,
            total_bytes: 0,
        }];
        for (section, show_empty) in [
            (DetailSection::Indexes, false),
            (DetailSection::Partitions, false),
            (DetailSection::Partitions, true),
            (DetailSection::Statistics, false),
        ] {
            studio.detail_section = section;
            studio.show_empty_partitions = show_empty;
            for (scope, keys) in [
                ("indexes:users", &["name", "kind", "columns", "owner"][..]),
                ("partitions:users", &["pid", "state", "rows", "size", "share", "lo", "location"][..]),
            ] {
                for key in keys {
                    studio.sort.insert(scope.into(), GridSort { column: key.to_string(), descending: true });
                    let _ = view(&studio);
                }
            }
        }

        studio.columns.push(ColumnInfo {
            position: 2,
            name: "doc".into(),
            data_type: "JSONB".into(),
            nullable: true,
            unique: false,
            primary_key: false,
            default: String::new(),
            indexed: false,
            json_table: String::new(),
        });
        for kind in [ModalKind::CreateIndex, ModalKind::CreateTable] {
            let _ = studio.update(Message::OpenModal(kind));
            let _ = crate::views::view(&studio);
        }
        for kind in crate::app::IndexKind::ALL {
            let _ = studio.update(Message::OpenModal(ModalKind::CreateIndex));
            let _ = studio.update(Message::IndexForm(crate::app::IndexEdit::Kind(kind)));
            let _ = crate::views::view(&studio);
        }
        let _ = studio.update(Message::OpenModal(ModalKind::CreateTable));
        let _ = studio.update(Message::PartitionForm(crate::app::PartitionEdit::Enabled(true)));
        let _ = studio.update(Message::PartitionForm(crate::app::PartitionEdit::Key(0)));
        let _ = crate::views::view(&studio);
    }

    /// A drop the server answered with success but did not do is reported as
    /// a failure once the list reloads, and one it did do is left alone.
    #[test]
    fn a_drop_the_server_ignored_is_caught() {
        use crate::app::{Message, Refresh, Status};
        use std::time::Duration;
        let key = IndexInfo {
            name: "users_pkey".into(),
            method: "btree".into(),
            columns: "id".into(),
            unique: true,
            owner: IndexOwner::Primary,
            definition: String::new(),
        };
        for kept in [true, false] {
            let mut studio = studio_with_tables();
            let _ = studio.update(Message::DropIndex {
                title: "Drop primary key".into(),
                body: String::new(),
                statement: "ALTER TABLE \"users\" DROP CONSTRAINT \"users_pkey\"".into(),
                index: "users_pkey".into(),
            });
            let _ = studio.update(Message::StatementFinished(
                Refresh::CurrentTable,
                "ALTER TABLE …".into(),
                Duration::ZERO,
                Ok("ALTER TABLE".into()),
            ));
            let reloaded = if kept { vec![key.clone()] } else { Vec::new() };
            let _ = studio.update(Message::IndexesLoaded("users".into(), Ok(reloaded)));
            assert_eq!(
                matches!(studio.status, Status::Failed(_)),
                kept,
                "kept={kept}: {:?}",
                studio.status
            );
            assert!(studio.verify_gone.is_none() && studio.expect_gone.is_none());
        }
    }

    #[test]
    fn a_long_location_keeps_its_end() {
        assert_eq!(path_tail("/mnt/a", 44), "/mnt/a");
        let long = "/tmp/claude-1000/a-very-long-scratch-directory/session/scratchpad/db";
        let short = path_tail(long, 20);
        assert_eq!(short.chars().count(), 20);
        assert!(short.starts_with('…') && short.ends_with("scratchpad/db"), "{short}");
    }

    /// Picking a partition key column makes it NOT NULL, and taking a column
    /// out of the dialog renumbers the key rather than pointing it elsewhere.
    #[test]
    fn the_partition_key_follows_the_column_list() {
        use crate::app::{Message, Modal, ModalKind, PartitionEdit};
        let (mut studio, _) = Studio::new(None);
        let _ = studio.update(Message::OpenModal(ModalKind::CreateTable));
        let _ = studio.update(Message::ModalAddColumn);
        let _ = studio.update(Message::ModalAddColumn);
        let _ = studio.update(Message::PartitionForm(PartitionEdit::Key(2)));
        let _ = studio.update(Message::PartitionForm(PartitionEdit::Key(1)));
        let Some(Modal::CreateTable { columns, partitioning, .. }) = &studio.modal else { panic!() };
        assert_eq!(partitioning.key, [2, 1]);
        assert!(!columns[2].nullable && !columns[1].nullable);

        let _ = studio.update(Message::ModalRemoveColumn(1));
        let Some(Modal::CreateTable { partitioning, .. }) = &studio.modal else { panic!() };
        assert_eq!(partitioning.key, [1], "column 2 moved up to 1; column 1 left the key");

        // A fourth is refused: the hash has three widths.
        let _ = studio.update(Message::ModalAddColumn);
        let _ = studio.update(Message::ModalAddColumn);
        for at in [0, 2, 3] {
            let _ = studio.update(Message::PartitionForm(PartitionEdit::Key(at)));
        }
        let Some(Modal::CreateTable { partitioning, .. }) = &studio.modal else { panic!() };
        assert_eq!(partitioning.key.len(), 3);
    }
}
