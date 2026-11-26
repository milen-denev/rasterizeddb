//! Who may connect, and what each role may reach.
//!
//! The grid lists every role with its attributes, memberships and how many
//! tables it was granted. MANAGE opens one role: its attributes (login,
//! superuser, create database/role, bypass RLS, connection limit, expiry,
//! rename), its memberships both ways, its table privileges (vaults and
//! ledger tables among them), the default privileges it receives, and the
//! row-level security policies that apply to it. Every change shows the
//! statement it sends; revoking, dropping and widening (superuser, bypass
//! RLS, enabling RLS on a table) go through the confirmation dialog. A
//! role's password hash is never read.

use iced::widget::{Space, column, container, pick_list, row, scrollable, text};
use iced::{Center, Element, Fill, Length};

use crate::app::{Message, ModalKind, Refresh, Studio};
use crate::dialect;
use crate::model::{Flavor, RoleInfo};
use crate::theme;

use super::{confirm, framed, pill, section_label, small_button};

pub fn view(studio: &Studio) -> Element<'_, Message> {
    let toolbar = row![
        text(format!("{} role{}", studio.roles.len(), if studio.roles.len() == 1 { "" } else { "s" }))
            .size(11)
            .font(theme::MONO)
            .color(theme::TEXT_FAINT),
        small_button("NEW ROLE", Some(Message::OpenModal(ModalKind::CreateRole))),
        small_button("RELOAD", Some(Message::SelectTab(crate::app::Tab::Roles))),
    ]
    .spacing(8)
    .align_y(Center);

    let mut body = column![roles_grid(studio)].spacing(10);
    if let Some(role) = studio.role_selected.as_ref().and_then(|n| studio.roles.iter().find(|r| &r.name == n)) {
        body = body.push(detail(studio, role));
    } else {
        body = body.push(
            text(
                "MANAGE a role to change its attributes and memberships, grant it tables (vaults and ledger tables \
                 included), set the default privileges it gets on new tables, and see or write the row-level \
                 security policies that apply to it.",
            )
            .size(11)
            .color(theme::TEXT_DIM),
        );
    }
    for e in &studio.role_access.errors {
        body = body.push(text(format!("Not shown: {e}")).size(11).color(theme::TEXT_FAINT));
    }
    framed("ROLES", Some(toolbar.into()), scrollable(body.padding(iced::Padding { right: 12.0, ..iced::Padding::ZERO })).height(Fill).style(theme::scroller).into())
}

fn row_button<'a>(label: &str, on: Option<Message>, destructive: bool) -> Element<'a, Message> {
    let mut b = iced::widget::button(text(label.to_string()).size(10).font(theme::MONO))
        .padding([2, 7])
        .style(if destructive { theme::destructive } else { theme::secondary });
    if let Some(m) = on {
        b = b.on_press(m);
    }
    b.into()
}

fn section<'a>(label: impl Into<String>, body: Element<'a, Message>) -> Element<'a, Message> {
    container(column![section_label(label), container(body).padding([2, 8])].spacing(2))
        .padding(iced::Padding { bottom: 8.0, ..iced::Padding::ZERO })
        .width(Fill)
        .style(theme::panel)
        .into()
}

fn input<'a>(studio: &'a Studio, key: &'static str, label: &str, placeholder: &str, width: f32) -> Element<'a, Message> {
    column![
        text(label.to_string()).size(11).font(theme::MONO).color(theme::TEXT_DIM),
        iced::widget::text_input(placeholder, studio.role_inputs.get(key).map(String::as_str).unwrap_or(""))
            .on_input(move |v| Message::RoleInput(key.to_string(), v))
            .padding([5, 8])
            .size(12)
            .font(theme::MONO)
            .style(theme::input)
            .width(Length::Fixed(width)),
    ]
    .spacing(4)
    .into()
}

fn on(studio: &Studio, key: &str) -> bool {
    studio.role_inputs.get(key).is_some_and(|v| v == "true")
}

fn tick<'a>(studio: &Studio, key: String, label: &str) -> Element<'a, Message> {
    let k = key.clone();
    super::toggle(label.to_string(), on(studio, &key), move |v| Message::RoleInput(k.clone(), v.to_string()))
}

fn picker<'a>(studio: &'a Studio, key: &'static str, label: &str, options: Vec<String>, width: f32) -> Element<'a, Message> {
    let chosen = studio.role_inputs.get(key).cloned();
    column![
        text(label.to_string()).size(11).font(theme::MONO).color(theme::TEXT_DIM),
        pick_list(options, chosen, move |v: String| Message::RoleInput(key.to_string(), v))
            .placeholder("choose…")
            .padding([5, 8])
            .text_size(12)
            .width(Length::Fixed(width)),
    ]
    .spacing(4)
    .into()
}

fn lowered<'a>(e: Element<'a, Message>) -> Element<'a, Message> {
    column![Space::new().height(15), e].into()
}

fn preview<'a>(sql: &Result<String, String>) -> Element<'a, Message> {
    match sql {
        Ok(s) => text(s.clone()).size(11).font(theme::MONO).color(theme::TEXT_DIM).into(),
        Err(why) => text(why.clone()).size(11).color(theme::TEXT_FAINT).into(),
    }
}

/// A small sortable grid of `rows` (cells, and a sort text per column).
fn grid<'a>(
    studio: &'a Studio,
    scope: &str,
    specs: Vec<super::GridColumn>,
    mut rows: Vec<(Vec<Element<'a, Message>>, Vec<String>)>,
    row_height: f32,
    max_height: f32,
) -> Element<'a, Message> {
    let layout = super::GridLayout::new(studio, scope.to_string());
    let widths = super::column_widths(&layout, &specs);
    let total = super::grid_total(&widths);
    let keys: Vec<String> = specs.iter().map(|c| c.key.to_string()).collect();
    super::sort_rows(&layout, &mut rows, |(_, sort), column| {
        super::SortKey::text(keys.iter().position(|k| k == column).and_then(|i| sort.get(i)).map_or("", String::as_str))
    });
    let n = rows.len();
    let mut body = column![].spacing(0);
    for (index, (cells, _)) in rows.into_iter().enumerate() {
        let line = super::grid_row(&widths, cells).height(Length::Fixed(row_height)).align_y(Center);
        body = body.push(super::grid_stripe(index, container(line).width(Length::Fixed(total)).into()));
    }
    container(super::grid_frame(&layout, &widths, super::grid_header(&layout, &specs, &widths), body.into()))
        .height(Length::Fixed((44.0 + n as f32 * row_height).min(max_height)))
        .into()
}

fn widget<'a>(e: impl Into<Element<'a, Message>>) -> Element<'a, Message> {
    container(e).padding([2, 6]).height(Fill).align_y(Center).into()
}

fn attribute_pills<'a>(role: &RoleInfo, current: &str) -> iced::widget::Row<'a, Message> {
    let mut flags = row![].spacing(4);
    if role.superuser {
        flags = flags.push(pill("SUPERUSER", theme::RUST));
    }
    flags = flags.push(if role.can_login { pill("LOGIN", theme::TEXT_DIM) } else { pill("NO LOGIN", theme::TEXT_FAINT) });
    if role.create_role && !role.superuser {
        flags = flags.push(pill("CREATEROLE", theme::TEXT_DIM));
    }
    if role.create_db && !role.superuser {
        flags = flags.push(pill("CREATEDB", theme::TEXT_DIM));
    }
    if role.bypass_rls && !role.superuser {
        flags = flags.push(pill("BYPASSRLS", theme::RUST));
    }
    if role.name == current {
        flags = flags.push(pill("YOU", theme::OK));
    }
    flags
}

fn members_of<'a>(studio: &'a Studio, role: &str) -> Vec<&'a str> {
    studio.roles.iter().filter(|r| r.member_of.iter().any(|m| m == role)).map(|r| r.name.as_str()).collect()
}

fn roles_grid(studio: &Studio) -> Element<'_, Message> {
    let current = studio.form.user.as_str();
    let selected = studio.role_selected.as_deref();
    let specs = vec![
        super::GridColumn::new("name", "ROLE", 180.0),
        super::GridColumn::new("actions", "", 220.0).unsortable(),
        super::GridColumn::new("flags", "ATTRIBUTES", 300.0).unsortable(),
        super::GridColumn::new("member_of", "MEMBER OF", 170.0),
        super::GridColumn::new("members", "MEMBERS", 170.0),
        super::GridColumn::new("tables", "TABLES GRANTED", 120.0).right(),
        super::GridColumn::new("owns", "OWNS", 70.0).right(),
        super::GridColumn::new("conn", "CONN LIMIT", 90.0).right(),
        super::GridColumn::new("until", "VALID UNTIL", 190.0),
    ];
    let rows = studio
        .roles
        .iter()
        .map(|role| {
            let granted = studio.role_access.grants.iter().filter(|g| g.grantee == role.name).count();
            let owns = studio.role_access.owned.iter().filter(|(o, _)| *o == role.name).count();
            let members = members_of(studio, &role.name).join(", ");
            let is_selected = selected == Some(role.name.as_str());
            // Dropping the role this console is logged in as would end the
            // console's own access; the server may allow it, the button does not.
            let drop = (role.name != current).then(|| {
                confirm(
                    "Drop role",
                    format!("This removes the role `{}`. Its connections are not closed, but it cannot log in again.", role.name),
                    format!("DROP ROLE {}", dialect::quote_ident(&role.name)),
                    Refresh::Roles,
                )
            });
            let actions = row![
                row_button(if is_selected { "CLOSE" } else { "MANAGE" }, Some(Message::RoleManage((!is_selected).then(|| role.name.clone()))), false),
                row_button("PASSWORD", Some(Message::RolePassword(role.name.clone())), false),
                row_button("DROP", drop, true),
            ]
            .spacing(4);
            let cells = vec![
                super::grid_cell(role.name.clone(), false, if is_selected { theme::RUST_BRIGHT } else { theme::TEXT }),
                widget(actions),
                widget(attribute_pills(role, current)),
                super::grid_cell(if role.member_of.is_empty() { "—".into() } else { role.member_of.join(", ") }, false, theme::TEXT_DIM),
                super::grid_cell(if members.is_empty() { "—".to_string() } else { members.clone() }, false, theme::TEXT_DIM),
                super::grid_cell(granted.to_string(), true, theme::TEXT_DIM),
                super::grid_cell(owns.to_string(), true, theme::TEXT_DIM),
                super::grid_cell(if role.conn_limit >= 0 { role.conn_limit.to_string() } else { "—".into() }, true, theme::TEXT_DIM),
                super::grid_cell(role.valid_until.clone().unwrap_or_else(|| "—".into()), false, theme::TEXT_DIM),
            ];
            let sort = vec![
                role.name.clone(),
                String::new(),
                String::new(),
                role.member_of.join(","),
                members,
                format!("{granted:08}"),
                format!("{owns:08}"),
                format!("{:08}", role.conn_limit + 1),
                role.valid_until.clone().unwrap_or_default(),
            ];
            (cells, sort)
        })
        .collect();
    section("ROLES", grid(studio, "roles", specs, rows, 27.0, 420.0))
}

fn detail<'a>(studio: &'a Studio, role: &'a RoleInfo) -> Element<'a, Message> {
    let name = role.name.as_str();
    let mut col = column![].spacing(10);
    if role.superuser {
        col = col.push(
            row![pill("SUPERUSER", theme::RUST), text("passes every privilege check: the grants and policies below do not limit it.").size(11).color(theme::TEXT_DIM)]
                .spacing(8)
                .align_y(Center),
        );
    } else if role.bypass_rls {
        col = col.push(
            row![pill("BYPASSRLS", theme::RUST), text("row-level security policies do not apply to it; its table grants do.").size(11).color(theme::TEXT_DIM)]
                .spacing(8)
                .align_y(Center),
        );
    }
    col = col.push(attributes(studio, role));
    col = col.push(membership(studio, role));
    col = col.push(table_privileges(studio, name));
    col = col.push(default_privileges(studio, name));
    col = col.push(policies(studio, name));
    container(column![section_label(format!("ROLE {}", name.to_uppercase())), col].spacing(4))
        .padding(8)
        .width(Fill)
        .style(theme::header)
        .into()
}

fn attributes<'a>(studio: &'a Studio, role: &'a RoleInfo) -> Element<'a, Message> {
    let sql = dialect::alter_role_sql(
        role,
        on(studio, "login"),
        on(studio, "superuser"),
        on(studio, "createdb"),
        on(studio, "createrole"),
        on(studio, "bypassrls"),
        studio.role_inputs.get("connlimit").map(String::as_str).unwrap_or(""),
        studio.role_inputs.get("validuntil").map(String::as_str).unwrap_or(""),
    );
    // Widening what a role may do goes through the confirmation.
    let apply = sql.as_ref().ok().map(|s| {
        let widens = (!role.superuser && on(studio, "superuser")) || (!role.bypass_rls && on(studio, "bypassrls"));
        if widens {
            confirm(
                "Widen this role?",
                format!("`{}` will pass privilege checks or row-level security it is held to now.", role.name),
                s.clone(),
                Refresh::Roles,
            )
        } else {
            Message::RoleRun(s.clone())
        }
    });
    let rename = studio.role_inputs.get("rename").map(|s| s.trim().to_string()).filter(|s| !s.is_empty() && *s != role.name);
    let rename_sql = rename.map(|to| format!("ALTER ROLE {} RENAME TO {}", dialect::quote_ident(&role.name), dialect::quote_ident(&to)));
    let note = match studio.flavor() {
        Flavor::Postgres => "Renaming clears an MD5 password; set it again with PASSWORD.",
        Flavor::Rasterized => "A role's grants, memberships and policies follow a rename.",
    };
    section(
        "ATTRIBUTES",
        column![
            row![
                tick(studio, "login".into(), "Can log in"),
                tick(studio, "superuser".into(), "Superuser"),
                tick(studio, "createdb".into(), "Create databases"),
                tick(studio, "createrole".into(), "Create roles"),
                tick(studio, "bypassrls".into(), "Bypass row-level security"),
            ]
            .spacing(16),
            row![
                input(studio, "connlimit", "Connection limit", "empty: no limit", 120.0),
                {
                    let until = input(studio, "validuntil", "Password valid until", "empty: always, or YYYY-MM-DD", 220.0);
                    match dialect::date_problem(studio.role_inputs.get("validuntil").map(String::as_str).unwrap_or("")) {
                        Some(p) => column![until, text(p).size(11).color(theme::BAD)].spacing(2).into(),
                        None => until,
                    }
                },
                lowered(super::primary_button("APPLY", apply)),
                Space::new().width(24),
                input(studio, "rename", "Rename to", "e.g. app_reader", 180.0),
                lowered(small_button(
                    "RENAME",
                    rename_sql.clone().map(|s| confirm("Rename role", format!("`{}` will log in under the new name only.", role.name), s, Refresh::Roles))
                )),
            ]
            .spacing(8)
            .align_y(iced::alignment::Vertical::Top),
            preview(&sql),
            text(note).size(10).color(theme::TEXT_FAINT),
        ]
        .spacing(8)
        .into(),
    )
}

fn membership<'a>(studio: &'a Studio, role: &'a RoleInfo) -> Element<'a, Message> {
    let name = role.name.as_str();
    let q = dialect::quote_ident;
    let mut of = row![text("Member of").size(11).font(theme::MONO).color(theme::TEXT_DIM)].spacing(6).align_y(Center);
    if role.member_of.is_empty() {
        of = of.push(text("no role").size(11).color(theme::TEXT_FAINT));
    }
    for g in &role.member_of {
        of = of.push(pill(g.clone(), theme::TEXT));
        of = of.push(row_button(
            "LEAVE",
            Some(confirm("Revoke membership", format!("`{name}` loses what `{g}` was granted."), format!("REVOKE {} FROM {}", q(g), q(name)), Refresh::Roles)),
            true,
        ));
    }
    let members = members_of(studio, name);
    let mut has = row![text("Members").size(11).font(theme::MONO).color(theme::TEXT_DIM)].spacing(6).align_y(Center);
    if members.is_empty() {
        has = has.push(text("none").size(11).color(theme::TEXT_FAINT));
    }
    for m in &members {
        has = has.push(pill(m.to_string(), theme::TEXT));
        has = has.push(row_button(
            "REMOVE",
            Some(confirm("Revoke membership", format!("`{m}` loses what `{name}` was granted."), format!("REVOKE {} FROM {}", q(name), q(m)), Refresh::Roles)),
            true,
        ));
    }
    let others: Vec<String> = studio.roles.iter().filter(|r| r.name != name).map(|r| r.name.clone()).collect();
    let join = studio.role_inputs.get("join").filter(|g| !role.member_of.contains(g)).map(|g| format!("GRANT {} TO {}", q(g), q(name)));
    let add = studio.role_inputs.get("member").filter(|m| !members.contains(&m.as_str())).map(|m| format!("GRANT {} TO {}", q(name), q(m)));
    section(
        "MEMBERSHIP",
        column![
            of,
            has,
            row![
                picker(studio, "join", "Make it a member of", others.clone(), 200.0),
                lowered(small_button("JOIN", join.map(Message::RoleRun))),
                Space::new().width(24),
                picker(studio, "member", "Add a member", others, 200.0),
                lowered(small_button("ADD MEMBER", add.map(Message::RoleRun))),
            ]
            .spacing(8)
            .align_y(iced::alignment::Vertical::Bottom),
            text("A member has every privilege of the roles it belongs to.").size(10).color(theme::TEXT_FAINT),
        ]
        .spacing(8)
        .into(),
    )
}

fn privilege_ticks<'a>(studio: &Studio, prefix: &str) -> (iced::widget::Row<'a, Message>, Vec<&'static str>) {
    let mut r = row![].spacing(12);
    let mut chosen = Vec::new();
    for p in dialect::TABLE_PRIVILEGES {
        let key = format!("{prefix}{p}");
        if on(studio, &key) {
            chosen.push(p);
        }
        r = r.push(tick(studio, key, p));
    }
    (r, chosen)
}

fn table_privileges<'a>(studio: &'a Studio, name: &'a str) -> Element<'a, Message> {
    let access = &studio.role_access;
    let owned: Vec<&str> = access.owned.iter().filter(|(o, _)| o == name).map(|(_, t)| t.as_str()).collect();
    let mut col = column![].spacing(8);
    col = col.push(
        row![
            text("Owns").size(11).font(theme::MONO).color(theme::TEXT_DIM),
            text(if owned.is_empty() { "nothing".to_string() } else { owned.join(", ") }).size(11).font(theme::MONO).color(if owned.is_empty() { theme::TEXT_FAINT } else { theme::TEXT }),
        ]
        .spacing(8),
    );
    let grants: Vec<_> = access.grants.iter().filter(|g| g.grantee == name).collect();
    if grants.is_empty() {
        col = col.push(text("No table granted to it.").size(11).color(theme::TEXT_FAINT));
    } else {
        let specs = vec![
            super::GridColumn::new("table", "TABLE", 200.0),
            super::GridColumn::new("privileges", "PRIVILEGES (* = may grant on)", 520.0).unsortable(),
            super::GridColumn::new("actions", "", 120.0).unsortable(),
        ];
        let rows = grants
            .iter()
            .map(|g| {
                let mut privs = row![].spacing(4);
                for p in &g.privileges {
                    let star = if g.grantable.contains(p) { "*" } else { "" };
                    privs = privs.push(pill(format!("{p}{star}"), theme::TEXT_DIM));
                }
                let revoke = confirm(
                    "Revoke table privileges",
                    format!("`{name}` loses {} on `{}`.", g.privileges.join(", "), g.table),
                    dialect::revoke_table_sql(&g.table, &g.privileges, name),
                    Refresh::Roles,
                );
                (
                    vec![super::grid_cell(g.table.clone(), false, theme::TEXT), widget(privs), widget(row_button("REVOKE", Some(revoke), true))],
                    vec![g.table.clone(), String::new(), String::new()],
                )
            })
            .collect();
        col = col.push(grid(studio, &format!("role-grants-{name}"), specs, rows, 27.0, 300.0));
    }
    let tables: Vec<String> = studio.tables.iter().map(|t| t.name.clone()).collect();
    let (ticks, chosen) = privilege_ticks(studio, "priv:");
    let table = studio.role_inputs.get("grant_table").cloned().unwrap_or_default();
    let sql = dialect::grant_table_sql(&table, &chosen, name, on(studio, "with_grant"));
    col = col.push(
        column![
            row![
                picker(studio, "grant_table", "Table", tables, 220.0),
                column![text("Privileges").size(11).font(theme::MONO).color(theme::TEXT_DIM), ticks].spacing(6),
            ]
            .spacing(16)
            .align_y(iced::alignment::Vertical::Bottom),
            row![
                tick(studio, "with_grant".into(), "With grant option"),
                super::primary_button("GRANT", sql.as_ref().ok().map(|s| Message::RoleRun(s.clone()))),
            ]
            .spacing(16)
            .align_y(Center),
            preview(&sql),
            text(
                "On a vault, SELECT reads its secrets and uses its keys, INSERT sets, disables, deletes and recovers \
                 them; PURGE and ROTATE KEY are its owner's. On a ledger table, INSERT adds rows; UPDATE only on a \
                 VERSIONED one; DELETE only past retention and with no legal hold.",
            )
            .size(10)
            .color(theme::TEXT_FAINT),
        ]
        .spacing(6),
    );
    section("TABLE PRIVILEGES", col.into())
}

fn default_privileges<'a>(studio: &'a Studio, name: &'a str) -> Element<'a, Message> {
    let current = studio.form.user.as_str();
    let mut col = column![].spacing(6);
    let defaults: Vec<_> = studio.role_access.defaults.iter().filter(|d| d.grantee == name).collect();
    if defaults.is_empty() {
        col = col.push(text("It gets nothing by default on new objects.").size(11).color(theme::TEXT_FAINT));
    }
    for d in defaults {
        let mut r = row![
            text(format!("{} created by {}", d.kind, if d.owner.is_empty() { "anyone" } else { d.owner.as_str() })).size(11).font(theme::MONO).color(theme::TEXT),
        ]
        .spacing(6)
        .align_y(Center);
        for p in &d.privileges {
            r = r.push(pill(p.clone(), theme::TEXT_DIM));
        }
        r = r.push(row_button(
            "REVOKE",
            Some(confirm(
                "Revoke default privileges",
                format!("New {} will no longer give `{name}` {}. Existing grants stay.", d.kind.to_lowercase(), d.privileges.join(", ")),
                dialect::revoke_default_sql(d, current),
                Refresh::Roles,
            )),
            true,
        ));
        col = col.push(r);
    }
    let (ticks, chosen) = privilege_ticks(studio, "dpriv:");
    let sql = dialect::grant_default_sql(&chosen, name);
    col = col.push(
        row![
            column![text(format!("On tables {current} creates from now on")).size(11).font(theme::MONO).color(theme::TEXT_DIM), ticks].spacing(6),
            super::primary_button("GRANT BY DEFAULT", sql.as_ref().ok().map(|s| Message::RoleRun(s.clone()))),
        ]
        .spacing(16)
        .align_y(iced::alignment::Vertical::Bottom),
    );
    col = col.push(preview(&sql));
    section("DEFAULT PRIVILEGES", col.into())
}

fn policies<'a>(studio: &'a Studio, name: &'a str) -> Element<'a, Message> {
    let access = &studio.role_access;
    let q = dialect::quote_ident;
    let mut col = column![].spacing(8);
    let mine: Vec<_> = access.policies.iter().filter(|p| p.roles.iter().any(|r| r == name || r == "public")).collect();
    if mine.is_empty() {
        col = col.push(text("No policy names this role (or PUBLIC).").size(11).color(theme::TEXT_FAINT));
    } else {
        let specs = vec![
            super::GridColumn::new("name", "POLICY", 150.0),
            super::GridColumn::new("table", "TABLE", 150.0),
            super::GridColumn::new("cmd", "FOR", 80.0),
            super::GridColumn::new("kind", "KIND", 110.0),
            super::GridColumn::new("using", "USING (ROWS IT SEES)", 260.0),
            super::GridColumn::new("check", "WITH CHECK (ROWS IT WRITES)", 260.0),
            super::GridColumn::new("rls", "RLS ON TABLE", 110.0).unsortable(),
            super::GridColumn::new("actions", "", 80.0).unsortable(),
        ];
        let rows = mine
            .iter()
            .map(|p| {
                let rls_on = access.rls_tables.contains(&p.table);
                let rls: Element<'a, Message> = if rls_on {
                    widget(pill("ON", theme::OK))
                } else {
                    widget(row_button(
                        "ENABLE",
                        Some(confirm(
                            "Enable row-level security",
                            format!("Every role but the owner, superusers and BYPASSRLS roles then sees only the rows a policy on `{}` lets it.", p.table),
                            format!("ALTER TABLE {} ENABLE ROW LEVEL SECURITY", q(&p.table)),
                            Refresh::Roles,
                        )),
                        false,
                    ))
                };
                let drop = confirm(
                    "Drop policy",
                    format!("The rows `{}` let its roles reach on `{}` are no longer allowed by it.", p.name, p.table),
                    format!("DROP POLICY {} ON {}", q(&p.name), q(&p.table)),
                    Refresh::Roles,
                );
                (
                    vec![
                        super::grid_cell(p.name.clone(), false, theme::TEXT),
                        super::grid_cell(p.table.clone(), false, theme::TEXT_DIM),
                        super::grid_cell(p.command.clone(), false, theme::TEXT_DIM),
                        super::grid_cell(if p.roles.iter().any(|r| r == "public") { "PUBLIC" } else if p.permissive { "permissive" } else { "restrictive" }, false, theme::TEXT_DIM),
                        super::grid_cell(super::elide(&p.using, 60).into_owned(), false, theme::TEXT),
                        super::grid_cell(super::elide(&p.check, 60).into_owned(), false, theme::TEXT),
                        rls,
                        widget(row_button("DROP", Some(drop), true)),
                    ],
                    vec![p.name.clone(), p.table.clone(), p.command.clone(), String::new(), p.using.clone(), p.check.clone(), String::new(), String::new()],
                )
            })
            .collect();
        col = col.push(grid(studio, &format!("role-policies-{name}"), specs, rows, 27.0, 300.0));
    }
    let tables: Vec<String> = studio.tables.iter().map(|t| t.name.clone()).collect();
    let command = studio.role_inputs.get("policy_cmd").cloned().unwrap_or_else(|| "SELECT".to_string());
    let mut commands = row![].spacing(2);
    for c in ["ALL", "SELECT", "INSERT", "UPDATE", "DELETE"] {
        commands = commands.push(
            iced::widget::button(text(c).size(11).font(theme::MONO))
                .padding([5, 10])
                .style(if command == c { theme::primary } else { theme::secondary })
                .on_press(Message::RoleInput("policy_cmd".into(), c.into())),
        );
    }
    let get = |k: &str| studio.role_inputs.get(k).map(String::as_str).unwrap_or("");
    let table = get("policy_table").to_string();
    let sql = dialect::create_policy_sql(get("policy_name"), &table, &command, name, get("policy_using"), get("policy_check"));
    let rls_off = !table.is_empty() && !access.rls_tables.contains(&table);
    col = col.push(
        column![
            row![
                input(studio, "policy_name", "Policy name", "e.g. own_rows", 160.0),
                picker(studio, "policy_table", "On table", tables, 200.0),
                column![text("For").size(11).font(theme::MONO).color(theme::TEXT_DIM), commands].spacing(4),
            ]
            .spacing(8)
            .align_y(iced::alignment::Vertical::Bottom),
            row![
                input(studio, "policy_using", "USING: rows it may see, update, delete", "e.g. owner = current_user", 320.0),
                input(studio, "policy_check", "WITH CHECK: rows it may write", "e.g. amount < 10000", 320.0),
                lowered(super::primary_button("CREATE POLICY", sql.as_ref().ok().map(|s| Message::RoleRun(s.clone())))),
            ]
            .spacing(8)
            .align_y(iced::alignment::Vertical::Bottom),
            preview(&sql),
            text(if rls_off {
                "Row-level security is off on that table: a policy there does nothing until it is enabled (ENABLE in the list above, or ALTER TABLE … ENABLE ROW LEVEL SECURITY)."
            } else {
                "A policy narrows the rows its roles reach on a table with row-level security on; the table's owner, superusers and BYPASSRLS roles are not held to it."
            })
            .size(10)
            .color(if rls_off { theme::WARN } else { theme::TEXT_FAINT }),
        ]
        .spacing(6),
    );
    section("ROW-LEVEL SECURITY", col.into())
}
