//! Who may connect: the server's roles.
//!
//! What either server does here: create a role, set its password, drop it,
//! and see what each may do (its attributes) and whose member it is.
//! Privileges are granted with GRANT in the query editor. A role's password
//! hash is never read: a console has no use for it.

use iced::widget::{Space, column, container, row, text};
use iced::{Center, Element, Fill, Length};

use crate::app::{Message, ModalKind, Refresh, Studio};
use crate::model::Flavor;
use crate::theme;

use super::{confirm, framed, small_button};

pub fn view(studio: &Studio) -> Element<'_, Message> {
    let layout = super::GridLayout::new(studio, "roles");
    let specs = vec![
        super::GridColumn::new("name", "ROLE", 240.0),
        super::GridColumn::new("flags", "", 330.0).unsortable(),
        super::GridColumn::new("member_of", "MEMBER OF", 200.0).unsortable(),
        super::GridColumn::new("actions", "", 190.0).unsortable(),
    ];
    let widths = super::column_widths(&layout, &specs);
    let current = studio.form.user.as_str();

    let mut roles: Vec<_> = studio.roles.iter().collect();
    super::sort_rows(&layout, &mut roles, |role, _| super::SortKey::text(&role.name));

    let mut lines = column![].spacing(0);
    for (index, role) in roles.into_iter().enumerate() {
        let mut flags = row![].spacing(4);
        if role.superuser {
            flags = flags.push(super::pill("SUPERUSER", theme::RUST));
        }
        flags = flags.push(if role.can_login {
            super::pill("LOGIN", theme::TEXT_DIM)
        } else {
            super::pill("NO LOGIN", theme::TEXT_FAINT)
        });
        if role.create_role && !role.superuser {
            flags = flags.push(super::pill("CREATEROLE", theme::TEXT_DIM));
        }
        if role.create_db && !role.superuser {
            flags = flags.push(super::pill("CREATEDB", theme::TEXT_DIM));
        }
        if role.bypass_rls && !role.superuser {
            flags = flags.push(super::pill("BYPASSRLS", theme::RUST));
        }
        if role.conn_limit >= 0 {
            flags = flags.push(super::pill(&format!("≤{} CONN", role.conn_limit), theme::TEXT_FAINT));
        }
        if role.valid_until.is_some() {
            flags = flags.push(super::pill("EXPIRES", theme::TEXT_FAINT));
        }
        if role.name == current {
            flags = flags.push(super::pill("YOU", theme::OK));
        }
        let drop = iced::widget::button(text("DROP").size(10).font(theme::MONO))
            .padding([2, 6])
            .style(theme::destructive);
        // Dropping the role this console is logged in as would end the
        // console's own access; the server may allow it, the button does not.
        let drop = if role.name == current {
            drop
        } else {
            drop.on_press(confirm(
                "Drop role",
                format!(
                    "This removes the role `{}`. Its connections are not closed, but it cannot \
                     log in again.",
                    role.name
                ),
                format!("DROP ROLE {}", crate::dialect::quote_ident(&role.name)),
                Refresh::Roles,
            ))
        };
        let line = super::grid_row(
            &widths,
            vec![
                super::grid_cell(role.name.clone(), false, theme::TEXT),
                container(flags).padding([3, 6]).into(),
                super::grid_cell(role.member_of.join(", "), false, theme::TEXT_DIM),
                container(
                    row![
                        small_button("PASSWORD", Some(Message::RolePassword(role.name.clone()))),
                        drop,
                    ]
                    .spacing(4),
                )
                .padding([2, 6])
                .into(),
            ],
        );
        lines = lines.push(super::grid_stripe(
            index,
            container(line)
                .width(Length::Fixed(super::grid_total(&widths)))
                .into(),
        ));
    }
    let grid = super::grid_frame(
        &layout,
        &widths,
        super::grid_header(&layout, &specs, &widths),
        lines.into(),
    );

    let toolbar = row![
        text(format!(
            "{} role{}",
            studio.roles.len(),
            if studio.roles.len() == 1 { "" } else { "s" }
        ))
        .size(11)
        .font(theme::MONO)
        .color(theme::TEXT_FAINT),
        Space::new().width(Fill),
        small_button("NEW ROLE", Some(Message::OpenModal(ModalKind::CreateRole))),
        small_button("RELOAD", Some(Message::SelectTab(crate::app::Tab::Roles))),
    ]
    .spacing(8)
    .align_y(Center);

    let note = match studio.flavor() {
        Flavor::Rasterized | Flavor::Postgres => {
            "Privileges on tables, columns, sequences and functions, role membership, default \
             privileges and row-level security policies are set with GRANT, ALTER DEFAULT \
             PRIVILEGES and CREATE POLICY in the query editor; this page creates, re-passwords and \
             drops roles, and shows what each may do."
        }
    };

    framed(
        "ROLES",
        None,
        column![
            toolbar,
            text(note).size(11).color(theme::TEXT_DIM),
            grid,
        ]
        .spacing(8)
        .into(),
    )
}
