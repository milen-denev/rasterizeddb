//! Storage: which directories this database writes into, what is in each of
//! them, and how much room is left.
//!
//! # The two servers
//!
//! The page renders both with the same columns because the question is the same
//! — which directory holds this table's data, and how much room is left there —
//! but who *decides* is not, and the page says which it is looking at rather
//! than leaving the buttons there and failing:
//!
//! - **RasterizedDB has a storage pool.** A placement policy picks a directory
//!   per `CREATE TABLE`, backup locations receive a mirrored copy of every
//!   write, and a rebalance moves tables afterwards. All of it is manageable
//!   from here.
//! - **PostgreSQL has tablespaces.** A table goes where the statement or
//!   `default_tablespace` says, nothing mirrors, and nothing moves on its own.
//!   The panel is read-only, and it says so.
//!
//! # What counts as a fault
//!
//! A draining location is not a broken one and a backup is doing its job, so
//! [`LocationHealth`] separates the four states and only the one that needs
//! attention gets a warning colour. The two banners are for the conditions an
//! operator would want to be interrupted about: a primary below the free-space
//! floor, and a table that is not reaching a backup the pool has.

use iced::widget::{Space, column, container, pick_list, row, scrollable, text};
use iced::{Element, Fill, Length};

use crate::app::{Message, Studio};
use crate::dialect;
use crate::model::*;
use crate::theme;

use super::{framed, primary_button, section_label, small_button};

/// What `ROLE` accepts, in the order the picker offers them.
const ROLES: [&str; 2] = ["primary", "backup"];

/// What `POLICY` accepts. Ordered by how much they do rather than
/// alphabetically: an operator reading down the list is choosing how much
/// autonomy to hand over.
const POLICIES: [&str; 3] = ["primary", "round-robin", "free-space"];

fn policy_help(policy: &str) -> &'static str {
    match policy {
        "round-robin" => "each new table goes to whichever directory holds the fewest",
        "free-space" => "each new table goes to whichever directory has the most room, weighted",
        _ => "every new table goes to the first directory; placement is manual",
    }
}

pub fn view(studio: &Studio) -> Element<'_, Message> {
    let flavor = studio.flavor();
    let manageable = dialect::storage_is_manageable(flavor);

    let Some(snapshot) = &studio.storage else {
        return framed(
            "STORAGE",
            Some(small_button("RELOAD", Some(Message::ReloadStorage))),
            container(text("Loading…").size(13).color(theme::TEXT_FAINT))
                .center_x(Fill)
                .center_y(Fill)
                .into(),
        );
    };

    if !snapshot.supported {
        return framed(
            "STORAGE",
            Some(small_button("RELOAD", Some(Message::ReloadStorage))),
            unsupported(flavor),
        );
    }

    let mut body = column![
        health(snapshot, manageable),
        summary(snapshot, flavor),
        policy_controls(studio, snapshot, manageable),
        locations(studio, snapshot, flavor, manageable),
    ]
    .spacing(10);

    if manageable {
        body = body.push(add_location(studio));
        body = body.push(move_table(studio, snapshot));
    }
    if !studio.storage_report.is_empty() {
        body = body.push(report(studio));
    }
    body = body.push(placements(studio, snapshot, flavor));

    framed(
        "STORAGE",
        Some(small_button("RELOAD", Some(Message::ReloadStorage))),
        scrollable(body).height(Fill).style(theme::scroller).into(),
    )
}

/// Same trap as the backup and replication pages, with one extra way to fall
/// into it: here the server may be perfectly current and simply built without
/// the feature, which is not a version problem and must not be reported as one.
fn unsupported(flavor: Flavor) -> Element<'static, Message> {
    container(
        column![
            text("This server does not report storage locations").size(15).color(theme::TEXT),
            container(Space::new().height(2)).width(72).style(theme::accent_underline),
            text(match flavor {
                Flavor::Rasterized =>
                    "It answered SHOW STORAGE STATS as if it were a runtime parameter, which is \
                     what a server without the storage_pools feature does — and what a build \
                     from before the feature existed does. The database is fine; it simply keeps \
                     everything in one directory. Rebuild the server with --features \
                     storage_pools to place tables across several.",
                Flavor::Postgres =>
                    "It would not answer for pg_tablespace. That usually means the connected role \
                     cannot read the catalogue, rather than that there are no tablespaces.",
            })
            .size(12)
            .color(theme::TEXT_DIM),
        ]
        .spacing(10)
        .max_width(720),
    )
    .padding(14)
    .into()
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

fn health(snapshot: &StorageSnapshot, manageable: bool) -> Element<'static, Message> {
    let mut warnings = column![].spacing(6);
    let mut any = false;

    let tight = snapshot.tight();
    if !tight.is_empty() {
        any = true;
        let floor = bytes(snapshot.min_free_bytes());
        warnings = warnings.push(banner(
            theme::WARN,
            "A DIRECTORY IS RUNNING OUT OF ROOM",
            format!(
                "{} — less than {} free. New tables will not be placed there, and the tables \
                 already on it have nowhere to grow. A rebalance moves the largest of them off.",
                tight
                    .iter()
                    .map(|l| format!("{} ({} free)", l.path, bytes(l.available_bytes)))
                    .collect::<Vec<_>>()
                    .join(", "),
                floor
            ),
        ));
    }

    // Only when the pool actually has a backup: a database that was never asked
    // for one is not missing anything.
    let unmirrored = snapshot.unmirrored();
    if !unmirrored.is_empty() {
        any = true;
        let names: Vec<&str> = unmirrored.iter().map(|p| p.table.as_str()).take(8).collect();
        warnings = warnings.push(banner(
            theme::WARN,
            "SOME TABLES ARE NOT REACHING THE BACKUP",
            format!(
                "{}{} are being written without a backup copy. Whether a file is mirrored is \
                 settled when it is opened, so a backup location added while the server was \
                 running does not cover tables that were already open. Restart the server and \
                 they will be copied over and kept in step.",
                names.join(", "),
                if unmirrored.len() > names.len() {
                    format!(" and {} more", unmirrored.len() - names.len())
                } else {
                    String::new()
                },
            ),
        ));
    }

    if !any && manageable && !snapshot.is_pooled() {
        // Not a fault, and deliberately not coloured as one. It is the state
        // every database starts in, and the page would otherwise look like a
        // control panel for something that is not switched on.
        warnings = warnings.push(banner(
            theme::TEXT_DIM,
            "ONE DIRECTORY",
            "Everything this database owns is in its own directory. Add another below to spread \
             tables across disks, or add one as a backup to keep a mirrored copy of every write."
                .to_string(),
        ));
        any = true;
    }

    if !any {
        return Space::new().into();
    }
    warnings.into()
}

fn banner(colour: iced::Color, heading: &'static str, body: String) -> Element<'static, Message> {
    let style = if colour == theme::BAD { theme::error_box } else { theme::tile };
    container(
        column![
            text(heading).size(11).font(theme::MONO).color(colour),
            text(body).size(11).color(theme::TEXT_DIM),
        ]
        .spacing(3),
    )
    .padding(9)
    .width(Fill)
    .style(style)
    .into()
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------

fn summary(snapshot: &StorageSnapshot, flavor: Flavor) -> Element<'static, Message> {
    // Taken from the server rather than summed here. Two directories on one
    // filesystem report the same bytes, and only the server can tell that they
    // are the same filesystem — adding the column up would claim twice the room
    // the machine has.
    let free = snapshot.number("free_bytes");
    let known = free.is_some_and(|bytes| bytes > 0);

    let backups = snapshot.backups().count();
    let tables: u64 = snapshot
        .number("tables")
        .unwrap_or_else(|| snapshot.placements.len() as u64);

    column![
        row![
            super::tile(
                "POLICY",
                snapshot.policy().to_string(),
                Some(
                    match flavor {
                        Flavor::Rasterized => policy_help(snapshot.policy()),
                        Flavor::Postgres => "default_tablespace decides",
                    }
                    .to_string()
                ),
            ),
            super::tile(
                "DIRECTORIES",
                format!("{} / {}", snapshot.primaries().filter(|l| l.enabled).count(), snapshot.locations.len()),
                Some("taking tables, of all".to_string()),
            ),
            super::tile(
                "BACKUP COPIES",
                if backups == 0 { "none".to_string() } else { count(backups as u64) },
                Some(
                    match flavor {
                        Flavor::Rasterized => "directories mirroring every write",
                        Flavor::Postgres => "PostgreSQL does not mirror a tablespace",
                    }
                    .to_string(),
                ),
            ),
            super::tile("TABLES", count(tables), Some("across every directory".to_string())),
            super::tile(
                "FREE",
                if known { bytes(free.unwrap_or(0)) } else { "—".to_string() },
                Some(
                    if known {
                        "across the directories that take tables".to_string()
                    } else {
                        "the server does not report free space".to_string()
                    },
                ),
            ),
        ]
        .spacing(10),
        row![
            labelled("DATABASE DIRECTORY", snapshot.metric("root").unwrap_or("—").to_string()),
            labelled(
                "FLOOR",
                if snapshot.min_free_bytes() == 0 {
                    "none".to_string()
                } else {
                    bytes(snapshot.min_free_bytes())
                },
            ),
            labelled("NEXT TABLE GOES TO", snapshot.next_placement().unwrap_or("—").to_string()),
        ]
        .spacing(24),
    ]
    .spacing(10)
    .into()
}

fn labelled(label: &'static str, value: String) -> Element<'static, Message> {
    column![
        text(label).size(10).font(theme::MONO).color(theme::TEXT_FAINT),
        text(value).size(12).font(theme::MONO),
    ]
    .spacing(2)
    .into()
}

// ---------------------------------------------------------------------------
// Policy
// ---------------------------------------------------------------------------

fn policy_controls<'a>(
    studio: &'a Studio,
    snapshot: &'a StorageSnapshot,
    manageable: bool,
) -> Element<'a, Message> {
    if !manageable {
        return container(
            text(
                "PostgreSQL places a table where the statement or default_tablespace says, and \
                 never moves one on its own. This page reports; it changes nothing.",
            )
            .size(11)
            .color(theme::TEXT_DIM),
        )
        .padding(8)
        .width(Fill)
        .style(theme::header)
        .into();
    }

    let current = POLICIES
        .iter()
        .find(|p| **p == snapshot.policy())
        .copied()
        .unwrap_or(POLICIES[0]);

    container(
        row![
            column![
                text("Placement policy").size(11).font(theme::MONO).color(theme::TEXT_DIM),
                pick_list(POLICIES, Some(current), Message::SetStoragePolicy)
                    .padding([6, 8])
                    .text_size(13)
                    .width(Length::Fixed(180.0)),
                text(policy_help(snapshot.policy())).size(10).color(theme::TEXT_FAINT),
            ]
            .spacing(4),
            column![
                super::field(
                    "Free-space floor",
                    &studio.storage_min_free,
                    Message::StorageMinFree,
                ),
                text("e.g. 512MB, 2GiB").size(10).color(theme::TEXT_FAINT),
            ]
            .spacing(4)
            .width(Length::Fixed(170.0)),
            column![
                Space::new().height(15),
                small_button("SET FLOOR", Some(Message::SetStorageMinFree)),
            ],
            Space::new().width(Fill),
            column![
                Space::new().height(15),
                row![
                    small_button("PLAN REBALANCE", Some(Message::Rebalance(true))),
                    small_button("REBALANCE", Some(Message::Rebalance(false))),
                ]
                .spacing(4),
            ],
            // Said here rather than in a tooltip: changing the policy is the
            // moment someone expects tables to move, and they do not.
            text(
                "Changing the policy moves nothing. It decides where the next table is created; \
                 a rebalance is what moves the ones that already exist.",
            )
            .size(10)
            .color(theme::TEXT_FAINT)
            .width(Length::Fixed(260.0)),
        ]
        .spacing(8)
        .align_y(iced::alignment::Vertical::Bottom),
    )
    .padding(8)
    .width(Fill)
    .style(theme::header)
    .into()
}

// ---------------------------------------------------------------------------
// Locations
// ---------------------------------------------------------------------------

fn locations<'a>(
    studio: &'a Studio,
    snapshot: &'a StorageSnapshot,
    flavor: Flavor,
    manageable: bool,
) -> Element<'a, Message> {
    if snapshot.locations.is_empty() {
        return container(
            text(match flavor {
                Flavor::Rasterized => "No storage locations. That should not happen: a database \
                                       always has at least its own directory.",
                Flavor::Postgres => "No tablespaces are visible to this role.",
            })
            .size(12)
            .color(theme::TEXT_FAINT),
        )
        .padding(12)
        .width(Fill)
        .style(theme::panel)
        .into();
    }

    let floor = snapshot.min_free_bytes();
    let layout = super::GridLayout::new(studio, "storage:locations");
    let mut columns = vec![
        super::GridColumn::new("location", "DIRECTORY", 300.0),
        super::GridColumn::new("state", "STATE", 100.0),
        super::GridColumn::new("role", "ROLE", 84.0),
        super::GridColumn::new("weight", "WEIGHT", 76.0).right(),
        super::GridColumn::new("tables", "TABLES", 76.0).right(),
        super::GridColumn::new("free", "FREE", 96.0).right(),
        super::GridColumn::new("total", "TOTAL", 96.0).right(),
        super::GridColumn::new("used", "USED", 68.0).right(),
    ];
    if manageable {
        columns.push(super::GridColumn::new("actions", "", 330.0));
    }
    let widths = super::column_widths(&layout, &columns);
    let total = super::grid_total(&widths);

    let mut body = column![].spacing(0);
    for (index, location) in snapshot.locations.iter().enumerate() {
        let health = location.health(floor);
        let colour = match health {
            LocationHealth::Healthy => theme::OK,
            LocationHealth::Full => theme::WARN,
            LocationHealth::Draining => theme::TEXT_DIM,
            LocationHealth::Backup => theme::TEXT_DIM,
        };

        let actions: Element<'a, Message> = if manageable {
            container(location_actions(location)).padding([2, 6]).into()
        } else {
            Space::new().into()
        };

        let used = match location.used_fraction() {
            Some(fraction) => format!("{}%", (fraction * 100.0).round() as u64),
            None => "—".to_string(),
        };

        let mut cells = vec![
            super::grid_cell(
                if location.is_root {
                    format!("{}  ·  database directory", location.path)
                } else {
                    location.path.clone()
                },
                false,
                theme::TEXT,
            ),
            container(super::pill(health.label(), colour)).padding([3, 6]).into(),
            super::grid_cell(location.role.clone(), false, theme::TEXT_DIM),
            super::grid_cell(count(location.weight), true, theme::TEXT_DIM),
            super::grid_cell(count(location.tables), true, theme::TEXT_DIM),
            super::grid_cell(
                if location.space_known() { bytes(location.available_bytes) } else { "—".into() },
                true,
                if health == LocationHealth::Full { theme::WARN } else { theme::TEXT_DIM },
            ),
            super::grid_cell(
                if location.space_known() { bytes(location.total_bytes) } else { "—".into() },
                true,
                theme::TEXT_FAINT,
            ),
            super::grid_cell(used, true, theme::TEXT_FAINT),
        ];
        if manageable {
            cells.push(actions);
        }

        body = body.push(super::grid_stripe(
            index,
            container(super::grid_row(&widths, cells))
                .width(Length::Fixed(total))
                .into(),
        ));
    }

    container(
        column![
            section_label(match flavor {
                Flavor::Rasterized => "LOCATIONS · WHERE THIS DATABASE MAY WRITE",
                Flavor::Postgres => "TABLESPACES · WHERE THIS SERVER MAY WRITE",
            }),
            super::grid_frame(
                &layout,
                &widths,
                super::grid_header(&layout, &columns, &widths),
                body.into(),
            ),
        ]
        .spacing(2),
    )
    .width(Fill)
    .height(Length::Fixed(56.0 + snapshot.locations.len() as f32 * 30.0))
    .style(theme::panel)
    .into()
}

fn location_actions(location: &StorageLocation) -> Element<'static, Message> {
    // The database's own directory is not optional: it holds the catalogue and
    // the pool file, so none of these apply to it. Showing them greyed would be
    // more consistent and less honest — there is no state in which they work.
    if location.is_root {
        return text("always in the pool").size(10).color(theme::TEXT_FAINT).into();
    }

    let path = location.path.clone();
    let mut controls = row![
        small_button(
            if location.enabled { "STOP USING" } else { "USE AGAIN" },
            Some(Message::EnableStorageLocation(path.clone(), !location.enabled)),
        ),
        small_button(
            if location.is_backup() { "MAKE PRIMARY" } else { "MAKE BACKUP" },
            Some(Message::SetStorageRole(
                path.clone(),
                if location.is_backup() { "primary" } else { "backup" },
            )),
        ),
    ]
    .spacing(4);

    // Weight only matters to a primary under the free-space policy, and a
    // backup has no share to weight.
    if !location.is_backup() {
        controls = controls.push(small_button(
            "−",
            (location.weight > 1).then(|| {
                Message::SetStorageWeight(path.clone(), location.weight.saturating_sub(1) as u32)
            }),
        ));
        controls = controls.push(small_button(
            "+",
            Some(Message::SetStorageWeight(path.clone(), (location.weight + 1) as u32)),
        ));
    }

    controls = controls.push(
        iced::widget::button(text("REMOVE").size(11).font(theme::MONO))
            .padding([5, 10])
            .style(theme::destructive)
            .on_press(Message::DropStorageLocation(path)),
    );

    container(controls).into()
}

// ---------------------------------------------------------------------------
// Adding a location
// ---------------------------------------------------------------------------

fn add_location(studio: &Studio) -> Element<'_, Message> {
    container(
        column![
            section_label("ADD A DIRECTORY"),
            row![
                container(super::field("Path", &studio.storage_path, Message::StoragePath))
                    .width(Length::Fixed(380.0)),
                column![
                    text("Role").size(11).font(theme::MONO).color(theme::TEXT_DIM),
                    pick_list(ROLES, Some(studio.storage_role), Message::StorageRole)
                        .padding([6, 8])
                        .text_size(13)
                        .width(Length::Fixed(130.0)),
                ]
                .spacing(4),
                container(super::field(
                    "Weight",
                    &studio.storage_weight,
                    Message::StorageWeight
                ))
                .width(Length::Fixed(90.0)),
                column![
                    Space::new().height(15),
                    primary_button("ADD", Some(Message::AddStorageLocation)),
                ],
                Space::new().width(Fill),
                text(
                    "An absolute path on the server, not on this machine. It is created if it is \
                     not there, and refused if the server cannot write to it or if it overlaps a \
                     directory already in the pool.",
                )
                .size(10)
                .color(theme::TEXT_FAINT)
                .width(Length::Fixed(300.0)),
            ]
            .spacing(8)
            .align_y(iced::alignment::Vertical::Bottom),
            text(match studio.storage_role {
                "backup" =>
                    "A backup directory receives a copy of every write, including the catalogue, \
                     so it opens as a database of its own rather than as an archive. It never \
                     holds a table's only copy.",
                _ =>
                    "A primary directory holds tables. Which one a new table lands in is what the \
                     placement policy decides; weight raises this one's share under free-space.",
            })
            .size(10)
            .color(theme::TEXT_FAINT),
        ]
        .spacing(6),
    )
    .padding(10)
    .width(Fill)
    .style(theme::panel)
    .into()
}

// ---------------------------------------------------------------------------
// Moving one table
// ---------------------------------------------------------------------------

fn move_table<'a>(studio: &'a Studio, snapshot: &'a StorageSnapshot) -> Element<'a, Message> {
    // Only offered when there is somewhere to move to. A picker with one entry
    // is a control that cannot do anything.
    let destinations: Vec<String> = snapshot
        .primaries()
        .map(|location| location.path.clone())
        .collect();
    if destinations.len() < 2 {
        return Space::new().into();
    }

    let tables: Vec<String> = snapshot.placements.iter().map(|p| p.table.clone()).collect();
    if tables.is_empty() {
        return Space::new().into();
    }

    container(
        column![
            section_label("MOVE A TABLE"),
            row![
                column![
                    text("Table").size(11).font(theme::MONO).color(theme::TEXT_DIM),
                    pick_list(tables, studio.storage_move_table.clone(), |table| {
                        Message::StorageMoveTable(Some(table))
                    })
                    .padding([6, 8])
                    .text_size(13)
                    .width(Length::Fixed(220.0)),
                ]
                .spacing(4),
                column![
                    text("To").size(11).font(theme::MONO).color(theme::TEXT_DIM),
                    pick_list(destinations, studio.storage_move_to.clone(), Message::StorageMoveTo)
                        .padding([6, 8])
                        .text_size(13)
                        .width(Length::Fixed(340.0)),
                ]
                .spacing(4),
                column![
                    Space::new().height(15),
                    small_button(
                        "MOVE",
                        (studio.storage_move_table.is_some() && studio.storage_move_to.is_some())
                            .then_some(Message::RunStorageMove),
                    ),
                ],
                Space::new().width(Fill),
                text(
                    "The table's files are copied, verified, and only then released from where \
                     they were. It is briefly unavailable while that happens.",
                )
                .size(10)
                .color(theme::TEXT_FAINT)
                .width(Length::Fixed(280.0)),
            ]
            .spacing(8)
            .align_y(iced::alignment::Vertical::Bottom),
        ]
        .spacing(6),
    )
    .padding(10)
    .width(Fill)
    .style(theme::panel)
    .into()
}

// ---------------------------------------------------------------------------
// The rebalance plan
// ---------------------------------------------------------------------------

fn report(studio: &Studio) -> Element<'_, Message> {
    let mut lines = column![].spacing(2);
    for line in &studio.storage_report {
        lines = lines.push(
            text(line.clone())
                .size(11)
                .font(theme::MONO)
                .color(if line.starts_with("stopped") { theme::BAD } else { theme::TEXT_DIM }),
        );
    }

    container(
        column![
            row![
                section_label("REBALANCE PLAN"),
                Space::new().width(Fill),
                small_button("DISMISS", Some(Message::DismissStorageReport)),
            ]
            .align_y(iced::alignment::Vertical::Center),
            container(scrollable(lines).style(theme::scroller))
                .max_height(200.0)
                .padding([4, 10]),
        ]
        .spacing(4),
    )
    .width(Fill)
    .style(theme::panel)
    .into()
}

// ---------------------------------------------------------------------------
// Placements
// ---------------------------------------------------------------------------

fn placements<'a>(
    studio: &'a Studio,
    snapshot: &'a StorageSnapshot,
    flavor: Flavor,
) -> Element<'a, Message> {
    if snapshot.placements.is_empty() {
        return Space::new().into();
    }

    let mirroring = snapshot.backups().next().is_some();
    let layout = super::GridLayout::new(studio, "storage:placements");
    let mut columns = vec![
        super::GridColumn::new("table_name", "TABLE", 220.0),
        super::GridColumn::new("location", "DIRECTORY", 420.0),
    ];
    if mirroring {
        columns.push(super::GridColumn::new("mirrored", "BACKUP COPY", 120.0));
    }
    let widths = super::column_widths(&layout, &columns);
    let total = super::grid_total(&widths);

    let mut body = column![].spacing(0);
    for (index, placement) in snapshot.placements.iter().enumerate() {
        let mut cells = vec![
            super::grid_cell(placement.table.clone(), false, theme::TEXT),
            super::grid_cell(placement.location.clone(), false, theme::TEXT_DIM),
        ];
        if mirroring {
            cells.push(
                container(if placement.mirrored {
                    super::pill("MIRRORED", theme::OK)
                } else {
                    super::pill("NOT YET", theme::WARN)
                })
                .padding([3, 6])
                .into(),
            );
        }
        body = body.push(super::grid_stripe(
            index,
            container(super::grid_row(&widths, cells))
                .width(Length::Fixed(total))
                .into(),
        ));
    }

    container(
        column![
            section_label(match flavor {
                Flavor::Rasterized => "TABLES · WHICH DIRECTORY HOLDS EACH",
                Flavor::Postgres => "TABLES · WHICH TABLESPACE HOLDS EACH",
            }),
            super::grid_frame(
                &layout,
                &widths,
                super::grid_header(&layout, &columns, &widths),
                body.into(),
            ),
        ]
        .spacing(2),
    )
    .width(Fill)
    .height(Length::Fixed(
        // Capped: a database with a thousand tables should not make this panel
        // a thousand rows tall inside a page that already scrolls.
        56.0 + (snapshot.placements.len().min(14) as f32) * 30.0,
    ))
    .style(theme::panel)
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{Studio, Tab};

    /// Builds every branch of this page against a real `Studio`.
    ///
    /// The unit tests elsewhere check the figures; this checks that the page
    /// *renders*. A grid whose header declares eight columns and whose rows
    /// supply nine, a picker handed a selection that is not in its list — those
    /// are runtime faults in a screen nobody looks at until something has
    /// already gone wrong, which is the worst time to find them.
    fn render(snapshot: Option<StorageSnapshot>, report: Vec<String>) {
        let (mut studio, _) = Studio::new(None);
        studio.tab = Tab::Storage;
        studio.storage = snapshot;
        studio.storage_report = report;
        // Dropped immediately: constructing the tree is the whole assertion,
        // and iced has no way to render one without a window.
        let _ = view(&studio);
    }

    fn location(path: &str, role: &str, enabled: bool, root: bool) -> StorageLocation {
        StorageLocation {
            path: path.into(),
            role: role.into(),
            weight: 2,
            enabled,
            is_root: root,
            tables: 3,
            available_bytes: 4096,
            total_bytes: 8192,
        }
    }

    fn full() -> StorageSnapshot {
        StorageSnapshot {
            stats: vec![
                Stat { metric: "policy".into(), value: "free-space".into(), detail: String::new() },
                Stat {
                    metric: "min_free_bytes".into(),
                    value: "1048576".into(),
                    detail: String::new(),
                },
                Stat { metric: "root".into(), value: "/root".into(), detail: String::new() },
                Stat { metric: "tables".into(), value: "3".into(), detail: String::new() },
                Stat { metric: "free_bytes".into(), value: "8192".into(), detail: String::new() },
                Stat {
                    metric: "next_placement".into(),
                    value: "/mnt/b".into(),
                    detail: String::new(),
                },
            ],
            supported: true,
            locations: vec![
                location("/root", "primary", true, true),
                location("/mnt/b", "primary", true, false),
                location("/mnt/c", "primary", false, false),
                location("/mnt/backup", "backup", true, false),
            ],
            placements: vec![
                TablePlacement {
                    table: "alpha".into(),
                    location: "/root".into(),
                    mirrored: true,
                },
                TablePlacement {
                    table: "beta".into(),
                    location: "/mnt/b".into(),
                    mirrored: false,
                },
            ],
        }
    }

    #[test]
    fn the_page_renders_before_anything_has_loaded() {
        render(None, Vec::new());
    }

    #[test]
    fn the_page_renders_a_server_that_does_not_have_the_feature() {
        render(
            Some(StorageSnapshot {
                stats: Vec::new(),
                supported: false,
                locations: Vec::new(),
                placements: Vec::new(),
            }),
            Vec::new(),
        );
    }

    /// The ordinary case, and the one every existing database is in: one
    /// directory, no backups, nothing to move.
    #[test]
    fn the_page_renders_a_database_with_one_directory() {
        let mut snapshot = full();
        snapshot.locations.truncate(1);
        snapshot.placements.retain(|p| p.location == "/root");
        render(Some(snapshot), Vec::new());
    }

    #[test]
    fn the_page_renders_a_pool_with_every_kind_of_location() {
        render(Some(full()), Vec::new());
    }

    /// Both banners at once, plus a plan on screen — the busiest the page gets.
    #[test]
    fn the_page_renders_every_warning_at_once() {
        let mut snapshot = full();
        for location in snapshot.locations.iter_mut() {
            location.available_bytes = 1;
        }
        render(
            Some(snapshot),
            vec![
                "2 table(s), 4.4 KiB would move:".to_string(),
                "alpha (3.5 KiB) /root -> /mnt/b: its location is below the free-space floor"
                    .to_string(),
                "stopped at beta: something went wrong".to_string(),
            ],
        );
    }

    /// A server that reports no free space at all — every PostgreSQL
    /// connection, and any platform without `statvfs`.
    #[test]
    fn the_page_renders_when_free_space_is_unknown() {
        let mut snapshot = full();
        snapshot.stats.retain(|stat| stat.metric != "free_bytes");
        for location in snapshot.locations.iter_mut() {
            location.available_bytes = 0;
            location.total_bytes = 0;
        }
        render(Some(snapshot), Vec::new());
    }

    /// A placement naming a directory the pool no longer lists. The page has to
    /// draw it rather than index into a list that does not contain it.
    #[test]
    fn the_page_renders_a_table_placed_somewhere_it_does_not_list() {
        let mut snapshot = full();
        snapshot.placements.push(TablePlacement {
            table: "orphan".into(),
            location: "/mnt/gone".into(),
            mirrored: false,
        });
        render(Some(snapshot), Vec::new());
    }
}
