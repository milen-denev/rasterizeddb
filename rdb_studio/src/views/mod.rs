//! The shell — title bar, sidebar, tabs, status bar — and the pieces every
//! screen shares.
//!
//! The layout is fixed rather than dockable: a console is used the same way
//! every time, and a movable panel is a preference to configure rather than a
//! question answered. What is here is the shape of the work — pick a table on
//! the left, pick a lens across the top, read it in the middle.

pub mod backup;
pub mod config;
pub mod dashboard;
pub mod connect;
pub mod data;
pub mod overview;
pub mod migrate;
pub mod query;
pub mod replication;
pub mod schema;
pub mod storage;
pub mod tables;

use iced::widget::{
    Space, button, column, container, pick_list, row, rule, scrollable, stack, text, text_input,
};
use iced::{Center, Element, Fill, Length};

use crate::app::{Message, Modal, ModalKind, Refresh, Status, Studio, Tab};
use crate::model::*;
use crate::theme;

pub fn view(studio: &Studio) -> Element<'_, Message> {
    let body: Element<'_, Message> = if studio.session.is_none() {
        connect::view(studio)
    } else {
        workspace(studio)
    };

    let screen = column![titlebar(studio), body, statusbar(studio)];

    let screen: Element<'_, Message> = match &studio.modal {
        None => screen.into(),
        Some(modal) => stack![
            screen,
            // The scrim swallows clicks on what is behind it, which is what
            // makes the dialog modal rather than merely on top.
            container(modal_view(modal, studio.flavor()))
                .style(theme::scrim)
                .width(Fill)
                .height(Fill)
                .center_x(Fill)
                .center_y(Fill),
        ]
        .into(),
    };

    container(screen).style(theme::root).width(Fill).height(Fill).into()
}

// ---------------------------------------------------------------------------
// Shell
// ---------------------------------------------------------------------------

fn titlebar(studio: &Studio) -> Element<'_, Message> {
    let brand = row![
        text("rdb").size(16).font(theme::MONO).color(theme::RUST),
        text("_studio").size(16).font(theme::MONO).color(theme::TEXT),
    ]
    .align_y(Center);

    let mut bar = row![brand, Space::new().width(20)].align_y(Center).spacing(0);

    if let Some(session) = &studio.session {
        bar = bar.push(
            row![
                pill(session.flavor.label(), theme::RUST),
                text(&session.database).size(13).font(theme::MONO).color(theme::TEXT),
                text("·").size(13).color(theme::TEXT_FAINT),
                text(&session.server).size(13).font(theme::MONO).color(theme::TEXT_DIM),
                pill(
                    if session.encrypted { "TLS" } else { "PLAIN" },
                    if session.encrypted { theme::OK } else { theme::WARN },
                ),
            ]
            .spacing(10)
            .align_y(Center),
        );
        bar = bar.push(Space::new().width(Fill));
        bar = bar.push(
            row![
                small_button("REFRESH", Some(Message::RefreshAll)),
                small_button("DISCONNECT", Some(Message::Disconnect)),
            ]
            .spacing(6),
        );
    } else {
        bar = bar.push(
            text("not connected").size(13).color(theme::TEXT_FAINT),
        );
        bar = bar.push(Space::new().width(Fill));
    }

    container(bar.padding([8, 14]))
        .style(theme::titlebar)
        .width(Fill)
        .into()
}

fn workspace(studio: &Studio) -> Element<'_, Message> {
    let mut content = column![tabs(studio), rule::horizontal(1).style(theme::hairline)];
    if let Some(warning) = &studio.flavor_warning {
        content = content.push(
            container(text(warning.clone()).size(12))
                .padding(8)
                .width(Fill)
                .style(theme::error_box),
        );
    }
    let content = content.push(pane(studio)).width(Fill).height(Fill);

    row![
        sidebar(studio),
        rule::vertical(1).style(theme::hairline),
        content,
    ]
    .height(Fill)
    .into()
}

fn tabs(studio: &Studio) -> Element<'_, Message> {
    let mut bar = row![].spacing(0);
    for tab in Tab::ALL {
        let active = studio.tab == tab;
        let label = column![
            text(tab.label()).size(11).font(theme::MONO),
            // The accent underline, rather than a filled tab: it marks the
            // active lens without turning the strip into six coloured blocks.
            container(Space::new().height(2))
                .width(Fill)
                .style(if active { theme::accent_underline } else { theme::no_underline }),
        ]
        .spacing(6)
        .align_x(Center);

        bar = bar.push(
            button(label)
                .padding([9, 18])
                .style(theme::tab(active))
                .on_press(Message::SelectTab(tab)),
        );
    }
    container(bar).width(Fill).style(theme::panel_flat).into()
}

fn pane(studio: &Studio) -> Element<'_, Message> {
    let inner = match studio.tab {
        Tab::Dashboard => dashboard::view(studio),
        Tab::Overview => overview::view(studio),
        Tab::Tables => tables::view(studio),
        Tab::Schema => schema::view(studio),
        Tab::Data => data::view(studio),
        Tab::Query => query::view(studio),
        Tab::Backup => backup::view(studio),
        Tab::Storage => storage::view(studio),
        Tab::Replication => replication::view(studio),
        Tab::Migrate => migrate::view(studio),
        Tab::Config => config::view(studio),
    };
    container(inner).width(Fill).height(Fill).padding(12).into()
}

fn sidebar(studio: &Studio) -> Element<'_, Message> {
    let mut tree = column![].spacing(1).width(Fill);

    // Databases first: switching one reconnects, so it is the outermost thing
    // a person picks.
    tree = tree.push(section_label("DATABASES"));
    if studio.databases.is_empty() {
        tree = tree.push(faint("none reported"));
    }
    for database in &studio.databases {
        let label = row![
            text(if database.is_current { "▸" } else { " " })
                .size(11)
                .font(theme::MONO)
                .color(theme::RUST),
            text(&database.name).size(13).font(theme::MONO).width(Fill),
            text(count(database.tables)).size(11).color(theme::TEXT_FAINT),
        ]
        .spacing(6)
        .align_y(Center);

        tree = tree.push(
            button(label)
                .width(Fill)
                .padding([4, 8])
                .style(theme::tree_item(database.is_current))
                .on_press(Message::SelectDatabase(database.name.clone())),
        );
    }

    tree = tree.push(Space::new().height(10));
    tree = tree.push(
        row![
            section_label("TABLES"),
            Space::new().width(Fill),
            small_button("+", Some(Message::OpenModal(ModalKind::CreateTable))),
        ]
        .align_y(Center)
        .padding([0, 8]),
    );
    tree = tree.push(
        container(
            text_input("filter…", &studio.table_filter)
                .on_input(Message::TableFilter)
                .padding([4, 6])
                .size(12)
                .font(theme::MONO)
                .style(theme::input),
        )
        .padding([4, 8]),
    );

    let filtered = studio.filtered_tables();
    if filtered.is_empty() {
        tree = tree.push(faint(if studio.tables.is_empty() {
            "no tables"
        } else {
            "nothing matches"
        }));
    }

    let mut list = column![].spacing(1).width(Fill);
    for table in filtered {
        let active = studio.selected_table.as_deref() == Some(table.name.as_str());
        let label = row![
            text(&table.name).size(13).font(theme::MONO).width(Fill),
            text(count(table.row_slots)).size(11).color(theme::TEXT_FAINT),
        ]
        .spacing(6)
        .align_y(Center);

        list = list.push(
            button(label)
                .width(Fill)
                .padding([4, 8])
                .style(theme::tree_item(active))
                .on_press(Message::SelectTable(table.name.clone())),
        );
    }
    tree = tree.push(scrollable(list).height(Fill).style(theme::scroller));

    tree = tree.push(rule::horizontal(1).style(theme::hairline));
    tree = tree.push(
        container(small_button(
            "NEW DATABASE",
            Some(Message::OpenModal(ModalKind::CreateDatabase)),
        ))
        .padding(8),
    );

    container(tree.padding([8, 0]))
        .width(268)
        .height(Fill)
        .style(theme::panel_flat)
        .into()
}

fn statusbar(studio: &Studio) -> Element<'_, Message> {
    let (marker, message, colour) = match &studio.status {
        Status::Idle => ("·", "ready".to_string(), theme::TEXT_FAINT),
        Status::Busy(what) => ("◐", what.clone(), theme::WARN),
        Status::Done(what) => ("✓", what.clone(), theme::OK),
        Status::Failed(what) => ("✗", what.clone(), theme::BAD),
    };

    let bar = row![
        text(marker).size(12).font(theme::MONO).color(colour),
        text(message).size(12).font(theme::MONO).color(colour).width(Fill),
        text(match &studio.selected_table {
            Some(table) => format!("table: {}", table),
            None => String::new(),
        })
        .size(12)
        .font(theme::MONO)
        .color(theme::TEXT_FAINT),
    ]
    .spacing(8)
    .align_y(Center);

    container(bar.padding([5, 12]))
        .style(theme::header)
        .width(Fill)
        .into()
}

// ---------------------------------------------------------------------------
// Modals
// ---------------------------------------------------------------------------

fn modal_view(modal: &Modal, flavor: Flavor) -> Element<'_, Message> {
    let body: Element<'_, Message> = match modal {
        Modal::Confirm { title, body, .. } => column![
            dialog_title(title),
            text(body).size(13).width(Fill),
            Space::new().height(6),
            row![
                Space::new().width(Fill),
                button(text("CANCEL").size(12).font(theme::MONO))
                    .padding([7, 14])
                    .style(theme::secondary)
                    .on_press(Message::ModalCancel),
                button(text("CONFIRM").size(12).font(theme::MONO))
                    .padding([7, 14])
                    .style(theme::destructive)
                    .on_press(Message::ModalSubmit),
            ]
            .spacing(8),
        ]
        .spacing(12)
        .width(440)
        .into(),

        // A restore is destructive to its destination and, when the target
        // names a point in the past, to the commits after it — so it gets the
        // destructive button, and the body it was built with rather than a
        // generic "are you sure".
        Modal::ConfirmImport { body, .. } => column![
            dialog_title("Import database"),
            container(text(body.clone()).size(12).font(theme::MONO))
                .padding(10)
                .width(Fill)
                .style(theme::error_box),
            Space::new().height(4),
            row![
                Space::new().width(Fill),
                button(text("CANCEL").size(12).font(theme::MONO))
                    .padding([7, 14])
                    .style(theme::secondary)
                    .on_press(Message::ModalCancel),
                button(text("IMPORT").size(12).font(theme::MONO))
                    .padding([7, 14])
                    .style(theme::destructive)
                    .on_press(Message::ModalSubmit),
            ]
            .spacing(8),
        ]
        .spacing(12)
        .width(600)
        .into(),

        Modal::ConfirmRestore { body, .. } => column![
            dialog_title("Restore database"),
            container(text(body.clone()).size(12).font(theme::MONO))
                .padding(10)
                .width(Fill)
                .style(theme::error_box),
            Space::new().height(4),
            row![
                Space::new().width(Fill),
                button(text("CANCEL").size(12).font(theme::MONO))
                    .padding([7, 14])
                    .style(theme::secondary)
                    .on_press(Message::ModalCancel),
                button(text("RESTORE").size(12).font(theme::MONO))
                    .padding([7, 14])
                    .style(theme::destructive)
                    .on_press(Message::ModalSubmit),
            ]
            .spacing(8),
        ]
        .spacing(12)
        .width(600)
        .into(),

        Modal::CreateDatabase { name, error } => column![
            dialog_title("New database"),
            field("Name", name, |v| Message::ModalField(
                crate::app::ModalField::Name,
                v
            )),
            error_line(error),
            dialog_actions("CREATE"),
        ]
        .spacing(12)
        .width(420)
        .into(),

        Modal::AddColumn { table, column: col, error } => column![
            dialog_title(format!("Add column to {}", table)),
            field("Name", &col.name, |v| Message::ModalField(
                crate::app::ModalField::Name,
                v
            )),
            type_picker(flavor, col, Message::ModalType, |field, v| Message::ModalField(
                field, v
            )),
            default_field(col, &col.default, |v| Message::ModalField(
                crate::app::ModalField::Default,
                v
            )),
            row![
                toggle("Nullable", col.nullable, |v| Message::ModalToggle(
                    crate::app::ModalField::Nullable,
                    v
                )),
                toggle("Unique", col.unique, |v| Message::ModalToggle(
                    crate::app::ModalField::Unique,
                    v
                )),
            ]
            .spacing(16),
            text(format!("Will be written as:  {}", col.data_type()))
                .size(11)
                .font(theme::MONO)
                .color(theme::TEXT_FAINT),
            error_line(error),
            dialog_actions("ADD COLUMN"),
        ]
        .spacing(12)
        .width(560)
        .into(),

        Modal::CreateTable { name, columns, error } => {
            let options = crate::types::choices(flavor);
            let mut list = column![].spacing(8);
            for (index, col) in columns.iter().enumerate() {
                let mut params = row![].spacing(6).align_y(Center);
                match col.choice.0.params {
                    crate::types::Params::None => {}
                    crate::types::Params::Length(_) => {
                        params = params.push(param_box("length", &col.length, move |v| {
                            Message::ModalColumnField(index, crate::app::ModalField::Length, v)
                        }));
                    }
                    crate::types::Params::PrecisionScale(_, _) => {
                        params = params.push(param_box("precision", &col.precision, move |v| {
                            Message::ModalColumnField(index, crate::app::ModalField::Precision, v)
                        }));
                        params = params.push(param_box("scale", &col.scale, move |v| {
                            Message::ModalColumnField(index, crate::app::ModalField::Scale, v)
                        }));
                    }
                }

                list = list.push(
                    column![
                        row![
                            text_input("name", &col.name)
                                .on_input(move |v| Message::ModalColumnField(
                                    index,
                                    crate::app::ModalField::Name,
                                    v
                                ))
                                .padding([5, 7])
                                .size(12)
                                .font(theme::MONO)
                                .style(theme::input)
                                .width(Length::Fixed(190.0)),
                            pick_list(options.clone(), Some(col.choice), move |choice| {
                                Message::ModalColumnType(index, choice)
                            })
                            .padding([5, 7])
                            .text_size(12)
                            .width(Length::Fixed(200.0)),
                            params,
                            Space::new().width(Fill),
                            toggle("PK", col.primary_key, move |v| Message::ModalColumnToggle(
                                index,
                                crate::app::ModalField::PrimaryKey,
                                v
                            )),
                            toggle("NULL", col.nullable, move |v| Message::ModalColumnToggle(
                                index,
                                crate::app::ModalField::Nullable,
                                v
                            )),
                            toggle("UNQ", col.unique, move |v| Message::ModalColumnToggle(
                                index,
                                crate::app::ModalField::Unique,
                                v
                            )),
                            button(text("×").size(14).font(theme::MONO))
                                .padding([3, 8])
                                .style(theme::secondary)
                                .on_press(Message::ModalRemoveColumn(index)),
                        ]
                        .spacing(6)
                        .align_y(Center),
                        text(col.choice.0.help)
                            .size(10)
                            .color(theme::TEXT_FAINT)
                            .width(Fill),
                    ]
                    .spacing(2),
                );
            }

            column![
                dialog_title("New table"),
                field("Name", name, |v| Message::ModalField(
                    crate::app::ModalField::Name,
                    v
                )),
                section_label("COLUMNS"),
                scrollable(list).height(Length::Fixed(260.0)).style(theme::scroller),
                small_button("+ COLUMN", Some(Message::ModalAddColumn)),
                error_line(error),
                dialog_actions("CREATE TABLE"),
            ]
            .spacing(12)
            .width(900)
            .into()
        }
    };

    container(container(body).padding(18).style(theme::dialog))
        .center_x(Fill)
        .center_y(Fill)
        .into()
}

fn dialog_title(title: impl Into<String>) -> Element<'static, Message> {
    column![
        text(title.into()).size(15).color(theme::TEXT),
        container(Space::new().height(2)).width(52).style(theme::accent_underline),
    ]
    .spacing(6)
    .into()
}

fn dialog_actions(confirm: &'static str) -> Element<'static, Message> {
    row![
        Space::new().width(Fill),
        button(text("CANCEL").size(12).font(theme::MONO))
            .padding([7, 14])
            .style(theme::secondary)
            .on_press(Message::ModalCancel),
        button(text(confirm).size(12).font(theme::MONO))
            .padding([7, 14])
            .style(theme::primary)
            .on_press(Message::ModalSubmit),
    ]
    .spacing(8)
    .into()
}

fn error_line(error: &Option<String>) -> Element<'_, Message> {
    match error {
        None => Space::new().height(0).into(),
        Some(message) => container(text(message.clone()).size(12).font(theme::MONO))
            .padding(8)
            .width(Fill)
            .style(theme::error_box)
            .into(),
    }
}

// ---------------------------------------------------------------------------
// Shared pieces
// ---------------------------------------------------------------------------

/// A small capsule of coloured text — the flavour, the TLS state, a row's type.
pub fn pill(label: impl Into<String>, colour: iced::Color) -> Element<'static, Message> {
    container(text(label.into()).size(10).font(theme::MONO).color(colour))
        .padding([2, 6])
        .style(move |_: &iced::Theme| container::Style {
            border: iced::Border {
                color: colour,
                width: 1.0,
                radius: iced::border::radius(0),
            },
            ..container::Style::default()
        })
        .into()
}

pub fn section_label(label: impl Into<String>) -> Element<'static, Message> {
    container(
        text(label.into())
            .size(10)
            .font(theme::MONO)
            .color(theme::TEXT_FAINT),
    )
    .padding([6, 8])
    .into()
}

pub fn faint(message: impl Into<String>) -> Element<'static, Message> {
    container(text(message.into()).size(12).color(theme::TEXT_FAINT))
        .padding([4, 10])
        .into()
}

pub fn small_button(label: impl Into<String>, on_press: Option<Message>) -> Element<'static, Message> {
    let mut widget = button(text(label.into()).size(11).font(theme::MONO))
        .padding([5, 10])
        .style(theme::secondary);
    if let Some(message) = on_press {
        widget = widget.on_press(message);
    }
    widget.into()
}

pub fn primary_button(label: impl Into<String>, on_press: Option<Message>) -> Element<'static, Message> {
    let mut widget = button(text(label.into()).size(12).font(theme::MONO))
        .padding([8, 16])
        .style(theme::primary);
    if let Some(message) = on_press {
        widget = widget.on_press(message);
    }
    widget.into()
}

pub fn field<'a>(
    label: impl Into<String>,
    value: &str,
    on_input: impl Fn(String) -> Message + 'a,
) -> Element<'a, Message> {
    column![
        text(label.into()).size(11).font(theme::MONO).color(theme::TEXT_DIM),
        text_input("", value)
            .on_input(on_input)
            .padding([6, 8])
            .size(13)
            .font(theme::MONO)
            .style(theme::input),
    ]
    .spacing(4)
    .into()
}

pub fn toggle<'a>(
    label: impl Into<String>,
    value: bool,
    on_toggle: impl Fn(bool) -> Message + 'a,
) -> Element<'a, Message> {
    iced::widget::checkbox(value)
        .label(label.into())
        .on_toggle(on_toggle)
        .size(14)
        .text_size(12)
        .spacing(6)
        .into()
}

/// The `DEFAULT` box, with one-click suggestions for the chosen type.
fn default_field<'a>(
    column: &'a crate::app::NewColumn,
    value: &'a str,
    on_input: impl Fn(String) -> Message + Copy + 'a,
) -> Element<'a, Message> {
    let mut suggestions = row![].spacing(4).align_y(Center);
    for entry in crate::types::defaults_for(column.choice) {
        suggestions = suggestions.push(
            button(text(entry.label).size(10).font(theme::MONO))
                .padding([3, 7])
                .style(theme::secondary)
                .on_press(on_input(entry.expression.to_string())),
        );
    }

    column![
        text("Default").size(11).font(theme::MONO).color(theme::TEXT_DIM),
        text_input("", value)
            .on_input(on_input)
            .padding([6, 8])
            .size(13)
            .font(theme::MONO)
            .style(theme::input),
        suggestions,
        text(crate::types::DEFAULT_HELP).size(10).color(theme::TEXT_FAINT),
    ]
    .spacing(4)
    .into()
}

/// A type picker, the parameter boxes the chosen type actually takes, and the
/// note that says what the server will really store.
fn type_picker<'a>(
    flavor: Flavor,
    column: &'a crate::app::NewColumn,
    on_type: impl Fn(crate::types::Choice) -> Message + 'a,
    on_param: impl Fn(crate::app::ModalField, String) -> Message + Copy + 'a,
) -> Element<'a, Message> {
    let mut controls = row![
        pick_list(crate::types::choices(flavor), Some(column.choice), on_type)
            .padding([6, 8])
            .text_size(13)
            .width(Length::Fixed(230.0)),
    ]
    .spacing(6)
    .align_y(Center);

    match column.choice.0.params {
        crate::types::Params::None => {}
        crate::types::Params::Length(_) => {
            controls = controls.push(param_box("length", &column.length, move |v| {
                on_param(crate::app::ModalField::Length, v)
            }));
        }
        crate::types::Params::PrecisionScale(_, _) => {
            controls = controls.push(param_box("precision", &column.precision, move |v| {
                on_param(crate::app::ModalField::Precision, v)
            }));
            controls = controls.push(param_box("scale", &column.scale, move |v| {
                on_param(crate::app::ModalField::Scale, v)
            }));
        }
    }

    column![
        text("Type").size(11).font(theme::MONO).color(theme::TEXT_DIM),
        controls,
        text(column.choice.0.help).size(10).color(theme::TEXT_FAINT),
    ]
    .spacing(4)
    .into()
}

/// One numeric parameter of a type.
fn param_box<'a>(
    label: &'a str,
    value: &'a str,
    on_input: impl Fn(String) -> Message + 'a,
) -> Element<'a, Message> {
    column![
        text(label).size(9).font(theme::MONO).color(theme::TEXT_FAINT),
        text_input("", value)
            .on_input(on_input)
            .padding([4, 6])
            .size(12)
            .font(theme::MONO)
            .style(theme::input)
            .width(Length::Fixed(78.0)),
    ]
    .spacing(2)
    .into()
}

/// A panel with a title strip, which is what every screen is built from.
pub fn framed<'a>(
    title: impl Into<String>,
    trailing: Option<Element<'a, Message>>,
    body: Element<'a, Message>,
) -> Element<'a, Message> {
    let mut strip = row![
        text(title.into()).size(11).font(theme::MONO).color(theme::TEXT_DIM),
        Space::new().width(Fill),
    ]
    .align_y(Center);
    if let Some(trailing) = trailing {
        strip = strip.push(trailing);
    }

    container(
        column![
            container(strip.padding([6, 10])).width(Fill).style(theme::header),
            container(body).width(Fill).height(Fill).padding(10),
        ]
        .height(Fill),
    )
    .style(theme::panel)
    .width(Fill)
    .height(Fill)
    .into()
}

/// One figure with its name under it, and the accent rule that ties the row of
/// them together.
pub fn tile(
    label: impl Into<String>,
    value: String,
    note: Option<String>,
) -> Element<'static, Message> {
    let mut inner = column![
        text(value).size(21).font(theme::MONO).color(theme::TEXT),
        text(label.into()).size(10).font(theme::MONO).color(theme::TEXT_DIM),
    ]
    .spacing(3);
    if let Some(note) = note {
        inner = inner.push(text(note).size(10).color(theme::TEXT_FAINT));
    }

    container(
        column![
            container(Space::new().height(2)).width(28).style(theme::accent_underline),
            inner,
        ]
        .spacing(7),
    )
    .padding([10, 12])
    .width(Fill)
    .style(theme::tile)
    .into()
}

/// Renders a value the way the metric's name says it should be read.
pub fn format_metric(metric: &str, value: &str) -> String {
    if let Ok(number) = value.trim().parse::<u64>() {
        if is_byte_metric(metric) {
            return bytes(number);
        }
        return count(number);
    }
    value.to_string()
}

// ---------------------------------------------------------------------------
// Grids
// ---------------------------------------------------------------------------
//
// Every table-shaped thing in the console is built from these four pieces:
// `column_widths` resolves the widths, `grid_header` draws the header and the
// drag handles, `grid_row` lays a row out against the same widths, and
// `grid_frame` wraps the result in the scrolling and drag plumbing.
//
// They are separate rather than one `grid(columns, rows)` call because the
// screens genuinely differ: the table list makes each row a button, the backup
// catalogue puts an action in the last cell, and a replication peer that is
// down grows a second line for its error. A single rigid signature would have
// grown an option for each. What has to be shared is the *alignment* — a header
// and its body computing widths separately is a bug waiting for the first
// column somebody drags — and that is what these four guarantee.

/// The gap between two columns, which is also the width of the drag handle.
pub const GRIP: f32 = 5.0;

/// One column of a grid.
pub struct GridColumn {
    /// Stable identity for the remembered width. Not the header text: a header
    /// that gets reworded should not silently lose the width someone set.
    ///
    /// A `Cow` because most grids know their columns at compile time and should
    /// allocate nothing for them, while a result set's come from the server.
    /// This used to be `&'static str`, which meant `Box::leak` for the result
    /// set — in a function the runtime calls on every redraw. Measured at
    /// 0.47 KiB per frame, or about 100 MB an hour with the window open.
    pub key: std::borrow::Cow<'static, str>,
    pub header: String,
    /// Width before anybody drags it.
    pub width: f32,
    pub right: bool,
}

impl GridColumn {
    pub fn new(
        key: impl Into<std::borrow::Cow<'static, str>>,
        header: impl Into<String>,
        width: f32,
    ) -> Self {
        Self { key: key.into(), header: header.into(), width, right: false }
    }

    /// A column of figures, which reads down the page only when right-aligned.
    pub fn right(mut self) -> Self {
        self.right = true;
        self
    }
}

/// Where a grid's widths are stored, and whether a drag is in progress.
pub struct GridLayout<'a> {
    pub widths: &'a std::collections::HashMap<String, f32>,
    /// What the widths are remembered against. Two grids that both have a
    /// `name` column should not share its width, so this is per screen — and
    /// per table, where the columns are the table's own.
    pub scope: String,
    /// Whether a drag is in progress. The outer listener is attached **only**
    /// then: `on_move` fires on every mouse movement over the area, and a
    /// message per pixel of cursor travel over a grid nobody is resizing is a
    /// redraw per pixel.
    pub resizing: bool,
    /// Where the grid is scrolled to and how tall its window is, for deciding
    /// which rows are worth building. `None` renders from the top, which is
    /// what the first frame — before any scroll has been reported — shows
    /// anyway.
    pub viewport: Option<(f32, f32)>,
    /// Emitted as the grid scrolls, when it is virtualised.
    pub on_scroll: Option<fn(iced::widget::scrollable::Viewport) -> Message>,
}

impl<'a> GridLayout<'a> {
    pub fn new(studio: &'a Studio, scope: impl Into<String>) -> Self {
        Self {
            widths: &studio.column_widths,
            scope: scope.into(),
            resizing: studio.resizing.is_some(),
            viewport: None,
            on_scroll: None,
        }
    }

    /// Turns on row virtualisation, tracking the scroll position through
    /// `on_scroll`.
    pub fn virtualised(
        mut self,
        viewport: Option<(f32, f32)>,
        on_scroll: fn(iced::widget::scrollable::Viewport) -> Message,
    ) -> Self {
        self.viewport = viewport;
        self.on_scroll = Some(on_scroll);
        self
    }
}

/// The key a column's width is remembered under.
pub fn width_key(scope: &str, column: &str) -> String {
    format!("{}\u{1}{}", scope, column)
}

/// The width a column takes when nobody has dragged it.
///
/// Estimated from the widest value on the page rather than measured, so a
/// hundred-column result lays out immediately. The floor is deliberately
/// generous: a column narrower than about fifteen characters truncates most
/// real values, and a grid of ellipses is a grid you have to widen before you
/// can read it.
pub fn default_width(header: &str, widest_value: usize) -> f32 {
    let characters = widest_value.max(header.len());
    (characters as f32 * 7.6 + 26.0).clamp(120.0, 560.0)
}

/// The widths to lay a grid out at: what the user dragged to, or the default.
pub fn column_widths(layout: &GridLayout<'_>, columns: &[GridColumn]) -> Vec<f32> {
    columns
        .iter()
        .map(|column| {
            layout
                .widths
                .get(&width_key(&layout.scope, &column.key))
                .copied()
                .unwrap_or(column.width)
        })
        .collect()
}

/// How wide the whole grid is, handles included.
pub fn grid_total(widths: &[f32]) -> f32 {
    widths.iter().sum::<f32>() + GRIP * widths.len() as f32
}

/// The header, with a drag handle after every column.
pub fn grid_header<'a>(
    layout: &GridLayout<'_>,
    columns: &[GridColumn],
    widths: &[f32],
) -> Element<'a, Message> {
    let mut header = row![].spacing(0);
    for (index, column) in columns.iter().enumerate() {
        let width = widths.get(index).copied().unwrap_or(column.width);
        header = header.push(
            container(
                text(column.header.clone())
                    .size(10)
                    .font(theme::MONO)
                    .color(theme::RUST)
                    .align_x(if column.right {
                        iced::alignment::Horizontal::Right
                    } else {
                        iced::alignment::Horizontal::Left
                    })
                    .width(Fill),
            )
            .padding([4, 6])
            .width(Length::Fixed(width)),
        );

        let key = width_key(&layout.scope, &column.key);
        header = header.push(
            iced::widget::mouse_area(
                container(Space::new().width(GRIP).height(Length::Fill)).style(theme::grip),
            )
            .interaction(iced::mouse::Interaction::ResizingHorizontally)
            .on_press(Message::BeginColumnResize(key.clone(), width))
            .on_double_click(Message::ResetColumnWidth(key)),
        );
    }

    container(header)
        .style(theme::header)
        .height(Length::Fixed(24.0))
        .width(Length::Fixed(grid_total(widths)))
        .into()
}

/// One row, laid out against the same widths the header used.
///
/// The spacer after each cell is what keeps a body cell under its header: the
/// handle occupies real width in the header, so a row that did not leave room
/// for it would drift one handle further left with every column.
pub fn grid_row<'a>(
    widths: &[f32],
    cells: Vec<Element<'a, Message>>,
) -> iced::widget::Row<'a, Message> {
    let mut line = row![].spacing(0);
    for (index, cell) in cells.into_iter().enumerate() {
        let width = widths.get(index).copied().unwrap_or(120.0);
        line = line.push(container(cell).width(Length::Fixed(width)));
        line = line.push(Space::new().width(GRIP));
    }
    line
}

/// An ordinary cell of text.
pub fn grid_cell<'a>(
    value: impl Into<String>,
    right: bool,
    colour: iced::Color,
) -> Element<'a, Message> {
    container(
        text(value.into())
            .size(11)
            .font(theme::MONO)
            .color(colour)
            .align_x(if right {
                iced::alignment::Horizontal::Right
            } else {
                iced::alignment::Horizontal::Left
            })
            .width(Fill),
    )
    .padding([3, 6])
    .width(Fill)
    .into()
}

/// Wraps a header and a body in the scrolling and drag plumbing.
pub fn grid_frame<'a>(
    layout: &GridLayout<'_>,
    widths: &[f32],
    header: Element<'a, Message>,
    body: Element<'a, Message>,
) -> Element<'a, Message> {
    let table = scrollable(
        column![header, rule::horizontal(1).style(theme::hairline), body]
            .width(Length::Fixed(grid_total(widths))),
    )
    .direction(scrollable::Direction::Both {
        vertical: scrollable::Scrollbar::default(),
        horizontal: scrollable::Scrollbar::default(),
    })
    .width(Fill)
    .height(Fill)
    .style(theme::scroller);

    if !layout.resizing {
        return table.into();
    }

    // Only while dragging: the handle is five pixels wide and the cursor leaves
    // it immediately, so the movement has to be tracked by something large
    // enough to keep up with it.
    iced::widget::mouse_area(table)
        .on_move(|point| Message::ColumnResizeMove(point.x))
        .on_release(Message::EndColumnResize)
        .on_exit(Message::EndColumnResize)
        .interaction(iced::mouse::Interaction::ResizingHorizontally)
        .into()
}

/// Alternating row background, so a long line stays readable across.
pub fn grid_stripe<'a>(index: usize, line: Element<'a, Message>) -> Element<'a, Message> {
    if index % 2 == 1 {
        container(line).style(theme::zebra).into()
    } else {
        container(line).into()
    }
}

/// Height every body row of a virtualised grid is laid out at.
///
/// Fixed, and it has to be: the window below is computed by dividing a scroll
/// offset by it, so a row that grew would put every row after it in the wrong
/// place. Everything a row can contain — a value, a cell being edited, a
/// delete button — is built to this height.
pub const ROW_HEIGHT: f32 = 24.0;

/// Rows kept above and below the visible window.
///
/// Scrolling produces the offset for a frame that has already been laid out, so
/// a window with no margin shows a band of nothing during a fast drag. Eight
/// rows is about a third of a screen, which is more than a frame of travel at
/// any plausible scroll speed.
const OVERSCAN: usize = 8;

/// Which rows a virtualised grid needs to build.
///
/// Returns the half-open range, and the pixel heights of the spacers that stand
/// in for what is left out — those are what keep the scrollbar the length it
/// would have been.
pub fn visible_rows(
    total: usize,
    offset_y: f32,
    viewport_height: f32,
) -> (std::ops::Range<usize>, f32, f32) {
    if total == 0 {
        return (0..0, 0.0, 0.0);
    }

    let first = ((offset_y / ROW_HEIGHT).floor() as usize).saturating_sub(OVERSCAN);
    let visible = (viewport_height / ROW_HEIGHT).ceil() as usize + OVERSCAN * 2 + 1;
    let last = (first + visible).min(total);
    let first = first.min(last);

    (
        first..last,
        first as f32 * ROW_HEIGHT,
        (total - last) as f32 * ROW_HEIGHT,
    )
}

/// Whether a grid of this size is worth virtualising.
///
/// Below the threshold the spacers and the arithmetic cost more than the rows
/// they save, and every row being present keeps the simple case simple.
pub fn should_virtualise(total: usize) -> bool {
    total > 300
}

/// A result set, whose columns are whatever the server returned.
///
/// The other grids know their columns at compile time; this one derives them —
/// including the default width, from the widest value on the page.
pub fn result_grid<'a>(
    grid: &'a Grid,
    layout: GridLayout<'a>,
    numeric: impl Fn(usize) -> bool + 'a,
    cell: impl Fn(usize, usize, Option<&'a str>) -> Element<'a, Message> + 'a,
    trailing: Option<Box<dyn Fn(usize) -> Element<'a, Message> + 'a>>,
) -> Element<'a, Message> {
    let columns: Vec<GridColumn> = grid
        .columns
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let widest = grid
                .rows
                .iter()
                .filter_map(|row| row.get(index))
                .map(|value| value.as_deref().map_or(4, str::len))
                .max()
                .unwrap_or(0);
            let mut column = GridColumn::new(name.clone(), name.clone(), default_width(name, widest));
            column.right = numeric(index);
            column
        })
        .collect();

    let widths = column_widths(&layout, &columns);
    let mut header = grid_header(&layout, &columns, &widths);
    let mut total = grid_total(&widths);

    if trailing.is_some() {
        total += 70.0;
        header = container(
            row![header, Space::new().width(Length::Fixed(70.0))]
        )
        .style(theme::header)
        .into();
    }

    // Only the rows that can be seen. A five-thousand-row page is twelve
    // thousand cells a frame otherwise, which measured at 20.7 ms — past the
    // frame budget before anything is drawn.
    let virtualise = should_virtualise(grid.rows.len());
    let (window, above, below) = if virtualise {
        let (offset, height) = layout.viewport.unwrap_or((0.0, 900.0));
        visible_rows(grid.rows.len(), offset, height)
    } else {
        (0..grid.rows.len(), 0.0, 0.0)
    };

    let mut body = column![].spacing(0);
    if above > 0.0 {
        // Stands in for the rows above, so the scrollbar is the length the
        // whole result deserves rather than the length of what is built.
        body = body.push(Space::new().height(Length::Fixed(above)));
    }
    for row_index in window.clone() {
        let values = &grid.rows[row_index];
        let cells: Vec<Element<'_, Message>> = values
            .iter()
            .enumerate()
            .map(|(column_index, value)| cell(row_index, column_index, value.as_deref()))
            .collect();
        let mut line = grid_row(&widths, cells);
        if let Some(trailing) = &trailing {
            line = line.push(container(trailing(row_index)).width(Length::Fixed(70.0)));
        }
        let row_element = container(line)
            .width(Length::Fixed(total))
            .height(Length::Fixed(ROW_HEIGHT));
        body = body.push(grid_stripe(row_index, row_element.into()));
    }
    if below > 0.0 {
        body = body.push(Space::new().height(Length::Fixed(below)));
    }

    grid_frame_with_total(&layout, total, header, body.into())
}

/// [`grid_frame`], for a grid whose total width is not just its columns.
pub fn grid_frame_with_total<'a>(
    layout: &GridLayout<'_>,
    total: f32,
    header: Element<'a, Message>,
    body: Element<'a, Message>,
) -> Element<'a, Message> {
    let mut table = scrollable(
        column![header, rule::horizontal(1).style(theme::hairline), body]
            .width(Length::Fixed(total)),
    )
    .direction(scrollable::Direction::Both {
        vertical: scrollable::Scrollbar::default(),
        horizontal: scrollable::Scrollbar::default(),
    })
    .width(Fill)
    .height(Fill)
    .style(theme::scroller);

    if let Some(on_scroll) = layout.on_scroll {
        table = table.on_scroll(on_scroll);
    }

    if !layout.resizing {
        return table.into();
    }

    iced::widget::mouse_area(table)
        .on_move(|point| Message::ColumnResizeMove(point.x))
        .on_release(Message::EndColumnResize)
        .on_exit(Message::EndColumnResize)
        .interaction(iced::mouse::Interaction::ResizingHorizontally)
        .into()
}

/// A `(metric, value, detail)` list, as a resizable grid.
///
/// Four screens show one of these — the overview, a table's statistics, the
/// replication counters, whatever a server says about backups — and all four
/// had the same fixed 200-to-280px metric column, which truncates a data
/// directory path and a long setting name alike. One implementation, and the
/// columns move.
/// Returns an element that borrows nothing: every cell owns its text, so a
/// caller may build the slice on the spot rather than having to keep one alive
/// for as long as the screen is drawn.
pub fn metric_grid(
    studio: &Studio,
    scope: impl Into<String>,
    stats: &[Stat],
) -> Element<'static, Message> {
    let layout = GridLayout::new(studio, scope);
    let specs = vec![
        GridColumn::new("metric", "METRIC", 250.0),
        GridColumn::new("value", "VALUE", 220.0),
        GridColumn::new("detail", "", 460.0),
    ];
    let widths = column_widths(&layout, &specs);
    let total = grid_total(&widths);

    let mut body = column![].spacing(0);
    for (index, stat) in stats.iter().enumerate() {
        // A boolean is the one value worth colouring: on a status page it is
        // usually the answer, and reading it should not need the label.
        let colour = match stat.value.trim() {
            "true" | "on" | "t" => theme::OK,
            "false" | "off" | "f" => theme::TEXT_DIM,
            _ => theme::TEXT,
        };
        let line = grid_row(
            &widths,
            vec![
                grid_cell(stat.metric.clone(), false, theme::TEXT_DIM),
                grid_cell(format_metric(&stat.metric, &stat.value), false, colour),
                grid_cell(stat.detail.clone(), false, theme::TEXT_FAINT),
            ],
        );
        body = body.push(grid_stripe(
            index,
            container(line).width(Length::Fixed(total)).into(),
        ));
    }

    grid_frame(
        &layout,
        &widths,
        grid_header(&layout, &specs, &widths),
        body.into(),
    )
}

/// A plain cell: the value, or a marked `NULL`.
///
/// The marker is dim and italic-adjacent rather than the literal text `NULL`,
/// because a `TEXT` column can hold the four characters `NULL` and the two must
/// not look the same.
pub fn plain_cell(value: Option<&str>, numeric: bool) -> Element<'_, Message> {
    let content: Element<'_, Message> = match value {
        None => text("∅").size(12).font(theme::MONO).color(theme::TEXT_FAINT).into(),
        Some(text_value) => text(elide(text_value, 220))
            .size(12)
            .font(theme::MONO)
            .color(theme::TEXT)
            .align_x(if numeric {
                iced::alignment::Horizontal::Right
            } else {
                iced::alignment::Horizontal::Left
            })
            .width(Fill)
            .into(),
    };
    container(content).padding([3, 6]).width(Fill).into()
}

/// Shortens a value that would otherwise be a page of text in one cell.
/// Shortens a value that would otherwise be a page of text in one cell.
///
/// Borrows when there is nothing to do, which is the overwhelmingly common
/// case: this runs for every cell of every grid on every redraw, and the
/// unconditional `replace` it used to start with allocated a `String` per cell
/// per frame whether or not the value contained a newline.
pub fn elide(value: &str, limit: usize) -> std::borrow::Cow<'_, str> {
    let needs_flattening = value.as_bytes().iter().any(|b| *b == b'\n' || *b == b'\r');

    if !needs_flattening {
        // `len` first: it bounds `chars().count()` from above, so a short value
        // never pays for the scan.
        if value.len() <= limit {
            return std::borrow::Cow::Borrowed(value);
        }
        if value.chars().count() <= limit {
            return std::borrow::Cow::Borrowed(value);
        }
        let head: String = value.chars().take(limit - 1).collect();
        return std::borrow::Cow::Owned(format!("{}…", head));
    }

    let flattened = value.replace(['\n', '\r'], "⏎");
    if flattened.chars().count() <= limit {
        return std::borrow::Cow::Owned(flattened);
    }
    let head: String = flattened.chars().take(limit - 1).collect();
    std::borrow::Cow::Owned(format!("{}…", head))
}

/// Wraps a destructive statement in a confirmation.
pub fn confirm(title: &str, body: String, statement: String, refresh: Refresh) -> Message {
    Message::Confirm {
        title: title.to_string(),
        body,
        statement,
        refresh,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn columns() -> Vec<GridColumn> {
        vec![
            GridColumn::new("name", "NAME", 200.0),
            GridColumn::new("size", "SIZE", 90.0).right(),
        ]
    }

    fn layout<'a>(widths: &'a HashMap<String, f32>, scope: &str) -> GridLayout<'a> {
        GridLayout {
            widths,
            scope: scope.to_string(),
            resizing: false,
            viewport: None,
            on_scroll: None,
        }
    }

    #[test]
    fn a_column_falls_back_to_its_default() {
        let widths = HashMap::new();
        assert_eq!(column_widths(&layout(&widths, "tables"), &columns()), vec![200.0, 90.0]);
    }

    #[test]
    fn a_dragged_width_wins() {
        let mut widths = HashMap::new();
        widths.insert(width_key("tables", "name"), 340.0);
        assert_eq!(column_widths(&layout(&widths, "tables"), &columns()), vec![340.0, 90.0]);
    }

    /// Two grids both have a `name` column, and they are not the same column.
    /// Without the scope, widening one would widen the other.
    #[test]
    fn widths_do_not_leak_between_grids() {
        let mut widths = HashMap::new();
        widths.insert(width_key("tables", "name"), 340.0);
        assert_eq!(
            column_widths(&layout(&widths, "replication"), &columns()),
            vec![200.0, 90.0]
        );
    }

    /// The header spends real width on its handles. A body that did not leave
    /// room for them would drift one handle further left with every column —
    /// the failure this whole factoring exists to prevent.
    #[test]
    fn the_total_accounts_for_every_handle() {
        assert_eq!(grid_total(&[200.0, 90.0]), 200.0 + 90.0 + GRIP * 2.0);
        assert_eq!(grid_total(&[]), 0.0);
    }

    /// A column narrower than about fifteen characters truncates most real
    /// values, and one wider than the screen helps nobody.
    #[test]
    fn a_default_width_is_bounded() {
        assert_eq!(default_width("id", 1), 120.0, "short content still gets a usable column");
        assert_eq!(default_width("x", 400), 560.0, "one enormous value does not take the screen");
        let middling = default_width("email", 30);
        assert!(middling > 120.0 && middling < 560.0, "{middling}");
    }

    /// The header is what is measured when nobody has dragged, so a long
    /// header must not be clipped by a short column of values.
    #[test]
    fn a_long_header_widens_its_own_column() {
        assert!(default_width("RECLAIMABLE_BYTES", 2) > default_width("N", 2));
    }

    // --- virtualisation

    /// The spacers stand in for what is not built, so the scrollbar is the
    /// length the whole result deserves. If they and the window disagree, the
    /// grid scrolls to the wrong rows.
    #[test]
    fn the_window_and_its_spacers_account_for_every_row() {
        for (total, offset, height) in [
            (5000usize, 0.0f32, 900.0f32),
            (5000, 4800.0, 900.0),
            (5000, 119_000.0, 900.0),
            (301, 0.0, 900.0),
            (1, 0.0, 900.0),
        ] {
            let (window, above, below) = visible_rows(total, offset, height);
            assert_eq!(
                above / ROW_HEIGHT + window.len() as f32 + below / ROW_HEIGHT,
                total as f32,
                "total {total} offset {offset} lost rows"
            );
            assert!(window.end <= total, "window ran past the end: {window:?}");
        }
    }

    /// Scrolled to the top, the window starts at the top.
    #[test]
    fn the_first_screen_starts_at_the_first_row() {
        let (window, above, _) = visible_rows(5000, 0.0, 900.0);
        assert_eq!(window.start, 0);
        assert_eq!(above, 0.0);
        assert!(window.end > 30, "a 900px window holds more than 30 rows: {window:?}");
    }

    /// Scrolled somewhere in the middle, the window covers what is on screen
    /// plus a margin — a window with no margin shows a band of nothing while
    /// the scroll offset catches up.
    #[test]
    fn the_window_covers_the_screen_with_margin() {
        let offset = 2400.0;
        let (window, _, _) = visible_rows(5000, offset, 900.0);
        let first_visible = (offset / ROW_HEIGHT) as usize;
        let last_visible = ((offset + 900.0) / ROW_HEIGHT) as usize;
        assert!(window.start < first_visible, "no margin above: {window:?}");
        assert!(window.end > last_visible, "no margin below: {window:?}");
    }

    /// An offset past the end must not produce a backwards range.
    #[test]
    fn scrolling_past_the_end_is_not_a_panic() {
        let (window, above, below) = visible_rows(10, 100_000.0, 900.0);
        assert!(window.start <= window.end, "{window:?}");
        assert!(above >= 0.0 && below >= 0.0);
    }

    #[test]
    fn an_empty_grid_has_an_empty_window() {
        let (window, above, below) = visible_rows(0, 0.0, 900.0);
        assert!(window.is_empty());
        assert_eq!((above, below), (0.0, 0.0));
    }

    /// Below the threshold, everything is built — the spacers and the
    /// arithmetic would cost more than the rows they save.
    #[test]
    fn small_grids_are_not_virtualised() {
        assert!(!should_virtualise(0));
        assert!(!should_virtualise(200));
        assert!(should_virtualise(5000));
    }
}
