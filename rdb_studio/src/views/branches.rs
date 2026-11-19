//! Branches: databases made as copies of another, which can be refreshed,
//! swapped in, compared with and merged back into it (RasterizedDB).
//!
//! The page is the SQL surface with buttons: every action shows the statement
//! it sends, and the destructive ones go through the confirmation dialog.
//! A diff is what a merge would do, so MERGE is offered beside it rather
//! than blind.

use iced::widget::{Space, column, container, row, scrollable, text};
use iced::{Element, Fill, Length};

use crate::app::{Message, Studio};
use crate::dialect;
use crate::model::*;
use crate::theme;

use super::{framed, section_label, small_button};

pub fn view(studio: &Studio) -> Element<'_, Message> {
    let reload = Some(small_button("RELOAD", Some(Message::ReloadBranches)));
    let Some(snapshot) = &studio.branches else {
        return framed(
            "BRANCHES",
            reload,
            container(text("Loading…").size(13).color(theme::TEXT_FAINT))
                .center_x(Fill)
                .center_y(Fill)
                .into(),
        );
    };
    if !snapshot.supported {
        return framed("BRANCHES", reload, unsupported());
    }
    let body = column![
        create_form(studio),
        branches(studio, snapshot),
        diff_panel(studio),
    ]
    .spacing(10);
    framed(
        "BRANCHES",
        reload,
        scrollable(body).height(Fill).style(theme::scroller).into(),
    )
}

fn unsupported() -> Element<'static, Message> {
    container(
        column![
            text("This server does not have branches")
                .size(15)
                .color(theme::TEXT),
            container(Space::new().height(2))
                .width(72)
                .style(theme::accent_underline),
            text(
                "SHOW BRANCHES was refused. Branches are a RasterizedDB feature (db_branching); a \
                 PostgreSQL server, or a RasterizedDB build without it, has none."
            )
            .size(12)
            .color(theme::TEXT_DIM),
        ]
        .spacing(10)
        .max_width(720),
    )
    .padding(14)
    .into()
}

fn choice<'a>(label: &'static str, selected: bool, message: Message) -> Element<'a, Message> {
    iced::widget::button(text(label).size(11).font(theme::MONO))
        .padding([5, 10])
        .style(if selected {
            theme::primary
        } else {
            theme::secondary
        })
        .on_press(message)
        .into()
}

fn create_form(studio: &Studio) -> Element<'_, Message> {
    let form = &studio.branch_form;
    let statement = dialect::create_branch_sql(form);
    let mut points = row![].spacing(4);
    for kind in BranchPointKind::ALL {
        points = points.push(choice(
            kind.label(),
            form.point == kind,
            Message::BranchPoint(kind),
        ));
    }
    let value: Element<'_, Message> = match form.point {
        BranchPointKind::Now => Space::new().width(0).into(),
        kind => super::field(
            match kind {
                BranchPointKind::Timestamp => "Timestamp (2026-11-19 10:00:00+00)",
                BranchPointKind::Lsn => "LSN",
                _ => "Snapshot id",
            },
            &form.value,
            |v| Message::BranchField(BranchField::Value, v),
        ),
    };
    let preview = match &statement {
        Ok(sql) => text(sql.clone())
            .size(11)
            .font(theme::MONO)
            .color(theme::TEXT_DIM),
        Err(why) => text(why.clone()).size(11).color(theme::TEXT_FAINT),
    };
    container(
        column![
            section_label("NEW BRANCH"),
            row![
                super::field("Name", &form.name, |v| Message::BranchField(
                    BranchField::Name,
                    v
                )),
                super::field(
                    format!("From (empty: {})", studio.current_database()),
                    &form.source,
                    |v| Message::BranchField(BranchField::Source, v)
                ),
                column![
                    text("Point")
                        .size(11)
                        .font(theme::MONO)
                        .color(theme::TEXT_DIM),
                    points
                ]
                .spacing(4),
                value,
                column![
                    Space::new().height(15),
                    super::toggle("Keep a merge base", form.keep_base, Message::BranchKeepBase),
                ],
                column![
                    Space::new().height(15),
                    super::primary_button(
                        "CREATE BRANCH",
                        statement.is_ok().then_some(Message::CreateBranch)
                    ),
                ],
            ]
            .spacing(8)
            .align_y(iced::alignment::Vertical::Bottom),
            preview,
            text(
                "A branch is a full database: connect to it by name. On a filesystem that cannot \
                 share blocks (ext4, NTFS) it is a full copy, and its merge base another; a past \
                 point is restored from the S3 backup."
            )
            .size(10)
            .color(theme::TEXT_FAINT),
        ]
        .spacing(6),
    )
    .padding(8)
    .width(Fill)
    .style(theme::header)
    .into()
}

fn branches<'a>(studio: &'a Studio, snapshot: &'a BranchesSnapshot) -> Element<'a, Message> {
    if snapshot.branches.is_empty() {
        return container(
            text("No branches. Make one above, or run CREATE BRANCH name FROM database.")
                .size(12)
                .color(theme::TEXT_FAINT),
        )
        .padding(12)
        .width(Fill)
        .style(theme::panel)
        .into();
    }
    let layout = super::GridLayout::new(studio, "branches");
    // The actions second, beside the name: at the far right they fell off
    // any window narrower than the whole grid.
    let columns = vec![
        super::GridColumn::new("branch", "BRANCH", 140.0),
        super::GridColumn::new("actions", "", 330.0).unsortable(),
        super::GridColumn::new("parent", "PARENT", 110.0),
        super::GridColumn::new("point", "POINT", 140.0),
        super::GridColumn::new("method", "METHOD", 70.0),
        super::GridColumn::new("created", "CREATED", 160.0),
        super::GridColumn::new("merged", "MERGED", 160.0),
        super::GridColumn::new("size", "SIZE", 80.0).right(),
        super::GridColumn::new("base", "BASE", 80.0).right(),
        super::GridColumn::new("sessions", "SESSIONS", 70.0).right(),
    ];
    let widths = super::column_widths(&layout, &columns);
    let total = super::grid_total(&widths);
    let mut list: Vec<&BranchInfo> = snapshot.branches.iter().collect();
    super::sort_rows(&layout, &mut list, |b, column| match column {
        "parent" => super::SortKey::text(&b.parent),
        "point" => super::SortKey::text(&b.point),
        "method" => super::SortKey::text(&b.method),
        "created" => super::SortKey::text(&b.created_at),
        "merged" => super::SortKey::text(&b.merged_at),
        "size" => super::SortKey::number(b.size as f64),
        "base" => super::SortKey::number(b.base as f64),
        "sessions" => super::SortKey::number(b.sessions as f64),
        _ => super::SortKey::text(&b.name),
    });

    let rows = list.len();
    let mut body = column![].spacing(0);
    for (index, b) in list.into_iter().enumerate() {
        let selected = studio
            .branch_diff
            .as_ref()
            .is_some_and(|(n, _)| n == &b.name);
        let actions = row![
            small_button(
                "DIFF",
                b.mergeable().then(|| Message::DiffBranch(b.name.clone()))
            ),
            small_button(
                "MERGE",
                b.mergeable().then(|| Message::MergeBranch(b.name.clone()))
            ),
            small_button(
                "RESET",
                b.parent_exists
                    .then(|| Message::ResetBranch(b.name.clone()))
            ),
            small_button(
                "PROMOTE",
                b.parent_exists
                    .then(|| Message::PromoteBranch(b.name.clone()))
            ),
            iced::widget::button(text("DROP").size(11).font(theme::MONO))
                .padding([5, 10])
                .style(theme::destructive)
                .on_press(Message::DropBranch(b.name.clone())),
        ]
        .spacing(4);
        let line = super::grid_row(
            &widths,
            vec![
                super::grid_cell(
                    b.name.clone(),
                    false,
                    if selected {
                        theme::RUST_BRIGHT
                    } else {
                        theme::TEXT
                    },
                ),
                container(actions).padding([2, 6]).into(),
                super::grid_cell(
                    if b.parent_exists {
                        b.parent.clone()
                    } else {
                        format!("{} (dropped)", b.parent)
                    },
                    false,
                    if b.parent_exists {
                        theme::TEXT_DIM
                    } else {
                        theme::WARN
                    },
                ),
                super::grid_cell(b.point.clone(), false, theme::TEXT_DIM),
                super::grid_cell(b.method.clone(), false, theme::TEXT_FAINT),
                super::grid_cell(b.created_at.clone(), false, theme::TEXT_DIM),
                super::grid_cell(
                    if b.merged_at.is_empty() {
                        "—".to_string()
                    } else {
                        b.merged_at.clone()
                    },
                    false,
                    theme::TEXT_DIM,
                ),
                super::grid_cell(bytes(b.size), true, theme::TEXT_DIM),
                super::grid_cell(
                    if b.base > 0 {
                        bytes(b.base)
                    } else {
                        "none".to_string()
                    },
                    true,
                    if b.base > 0 {
                        theme::TEXT_DIM
                    } else {
                        theme::TEXT_FAINT
                    },
                ),
                super::grid_cell(count(b.sessions), true, theme::TEXT_DIM),
            ],
        );
        body = body.push(super::grid_stripe(
            index,
            container(line).width(Length::Fixed(total)).into(),
        ));
    }
    container(
        column![
            section_label("BRANCHES"),
            // Sized to its rows: a `Fill` grid inside the page's scrollable
            // is squeezed to a row and a half, and the button rows are
            // taller than text ones.
            container(super::grid_frame(
                &layout,
                &widths,
                super::grid_header(&layout, &columns, &widths),
                body.into(),
            ))
            .height(Length::Fixed(44.0 + rows as f32 * 38.0)),
        ]
        .spacing(2),
    )
    .width(Fill)
    .style(theme::panel)
    .into()
}

fn diff_panel(studio: &Studio) -> Element<'_, Message> {
    let Some((name, diff)) = &studio.branch_diff else {
        return Space::new().height(0).into();
    };
    let mut policies = row![].spacing(4);
    for policy in MergePolicy::ALL {
        policies = policies.push(choice(
            policy.label(),
            studio.merge_policy == policy,
            Message::MergePolicyChosen(policy),
        ));
    }
    let statement = dialect::merge_branch_sql(name, studio.merge_policy, studio.merge_allow_drop);
    let content: Element<'_, Message> = match diff {
        None => text("Comparing…").size(12).color(theme::TEXT_FAINT).into(),
        Some(Err(e)) => text(e.clone()).size(12).color(theme::BAD).into(),
        Some(Ok(rows)) if rows.is_empty() => {
            text("Nothing to merge: the branch has not changed since its base.")
                .size(12)
                .color(theme::TEXT_FAINT)
                .into()
        }
        Some(Ok(rows)) => {
            let mut list = column![].spacing(2);
            for r in rows {
                let colour = if r.conflicts > 0 {
                    theme::BAD
                } else {
                    theme::TEXT
                };
                list = list.push(
                    row![
                        text(r.table.clone())
                            .size(12)
                            .font(theme::MONO)
                            .color(colour)
                            .width(Length::Fixed(180.0)),
                        text(r.change.to_uppercase())
                            .size(11)
                            .font(theme::MONO)
                            .color(theme::TEXT_DIM)
                            .width(Length::Fixed(70.0)),
                        text(if r.change == "data" {
                            format!("+{} ~{} -{}", r.inserts, r.updates, r.deletes)
                        } else {
                            String::new()
                        })
                        .size(12)
                        .font(theme::MONO)
                        .width(Length::Fixed(140.0)),
                        text(if r.conflicts > 0 {
                            format!("{} conflict(s)", r.conflicts)
                        } else {
                            String::new()
                        })
                        .size(11)
                        .color(theme::BAD)
                        .width(Length::Fixed(110.0)),
                        text(super::elide(&r.detail, 160).into_owned())
                            .size(11)
                            .color(theme::TEXT_DIM),
                    ]
                    .spacing(8),
                );
            }
            list.into()
        }
    };
    container(
        column![
            section_label(format!("WHAT MERGING {} WOULD DO", name.to_uppercase())),
            content,
            row![
                column![
                    text("On conflict")
                        .size(11)
                        .font(theme::MONO)
                        .color(theme::TEXT_DIM),
                    policies
                ]
                .spacing(4),
                column![
                    Space::new().height(15),
                    super::toggle(
                        "Allow drops",
                        studio.merge_allow_drop,
                        Message::MergeAllowDrop
                    ),
                ],
                column![
                    Space::new().height(15),
                    super::primary_button("MERGE", Some(Message::MergeBranch(name.clone()))),
                ],
                Space::new().width(Fill),
            ]
            .spacing(8)
            .align_y(iced::alignment::Vertical::Bottom),
            text(statement)
                .size(11)
                .font(theme::MONO)
                .color(theme::TEXT_DIM),
        ]
        .spacing(8),
    )
    .padding(8)
    .width(Fill)
    .style(theme::panel)
    .into()
}

/// Which text box of the new-branch form changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchField {
    Name,
    Source,
    Value,
}
