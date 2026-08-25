//! The connection screen.
//!
//! It is the first thing the console shows and the only screen that is not a
//! grid, so it carries the identity: the mark, the accent rule, and a plain
//! statement of what the TLS setting will and will not do — which is the one
//! thing on this screen a person can get wrong without finding out.

use iced::widget::{Space, column, container, pick_list, row, rule, scrollable, text};
use iced::{Center, Element, Fill, Length};

use crate::app::{Message, Studio};
use crate::model::{Flavor, TlsMode};
use crate::theme;

use super::{field, primary_button, small_button, toggle};

pub fn view(studio: &Studio) -> Element<'_, Message> {
    // Bound rather than inlined: the widget borrows the string it is given, so
    // a temporary here would not outlive the element that reads it.
    let port = studio.form.port.to_string();

    let form = column![
        heading(),
        row![
            column![
                field("Profile name", &studio.form.name, Message::FormName),
                field("Host", &studio.form.host, Message::FormHost),
                field("Port", &port, Message::FormPort),
            ]
            .spacing(10)
            .width(Fill),
            column![
                field("Database", &studio.form.database, Message::FormDatabase),
                field("User", &studio.form.user, Message::FormUser),
                secret("Password", &studio.form.password),
            ]
            .spacing(10)
            .width(Fill),
        ]
        .spacing(16),
        row![
            labelled(
                "Dialect",
                pick_list(Flavor::ALL, Some(studio.form.flavor), Message::FormFlavor)
                    .padding([6, 8])
                    .text_size(13)
                    .width(Length::Fixed(180.0))
                    .into(),
            ),
            labelled(
                "TLS",
                pick_list(TlsMode::ALL, Some(studio.form.tls), Message::FormTls)
                    .padding([6, 8])
                    .text_size(13)
                    .width(Length::Fixed(140.0))
                    .into(),
            ),
            column![
                Space::new().height(16),
                toggle("Save password", studio.form.save_password, Message::FormSavePassword),
            ],
            Space::new().width(Fill),
        ]
        .spacing(16)
        .align_y(iced::alignment::Vertical::Bottom),
        tls_note(studio.form.tls),
        row![
            primary_button(
                if studio.connecting { "CONNECTING…" } else { "CONNECT" },
                (!studio.connecting).then_some(Message::Connect),
            ),
            small_button("SAVE PROFILE", Some(Message::SaveProfile)),
            Space::new().width(Fill),
        ]
        .spacing(8),
        error(studio),
    ]
    .spacing(16)
    .width(Length::Fixed(680.0));

    let screen = row![
        container(form).padding(26).style(theme::panel),
        saved(studio),
    ]
    .spacing(20)
    .padding(28);

    container(screen).center_x(Fill).center_y(Fill).into()
}

fn heading() -> Element<'static, Message> {
    column![
        row![
            text("rdb").size(30).font(theme::MONO).color(theme::RUST),
            text("_studio").size(30).font(theme::MONO).color(theme::TEXT),
        ],
        container(Space::new().height(3)).width(96).style(theme::accent_underline),
        text("RasterizedDB and PostgreSQL, over one wire protocol.")
            .size(13)
            .color(theme::TEXT_DIM),
    ]
    .spacing(8)
    .into()
}

fn secret<'a>(label: &'a str, value: &'a str) -> Element<'a, Message> {
    column![
        text(label).size(11).font(theme::MONO).color(theme::TEXT_DIM),
        iced::widget::text_input("", value)
            .on_input(Message::FormPassword)
            .secure(true)
            .padding([6, 8])
            .size(13)
            .font(theme::MONO)
            .style(theme::input),
    ]
    .spacing(4)
    .into()
}

fn labelled<'a>(label: &'a str, control: Element<'a, Message>) -> Element<'a, Message> {
    column![
        text(label).size(11).font(theme::MONO).color(theme::TEXT_DIM),
        control,
    ]
    .spacing(4)
    .into()
}

/// What the chosen TLS mode actually does.
///
/// This is here because the honest answer is surprising: `require` encrypts but
/// does not authenticate the server, since RasterizedDB's default certificate
/// is self-signed and verifying it would refuse the server the user just
/// started. A console that said "require" and left the user believing it had
/// verified anything would be the misleading option.
fn tls_note(mode: TlsMode) -> Element<'static, Message> {
    let (colour, note) = match mode {
        TlsMode::Disable => (
            theme::WARN,
            "Cleartext. Everything, the password included, crosses the network readable.",
        ),
        TlsMode::Prefer => (
            theme::TEXT_DIM,
            "Encrypts if the server offers it, otherwise connects in cleartext. \
             The certificate is not verified.",
        ),
        TlsMode::Require => (
            theme::TEXT_DIM,
            "Refuses to connect without encryption. The certificate is still not verified, \
             so this protects against reading the traffic, not against a server \
             impersonating the real one.",
        ),
    };
    container(text(note).size(11).color(colour))
        .padding([6, 8])
        .width(Fill)
        .style(theme::panel_flat)
        .into()
}

fn error(studio: &Studio) -> Element<'_, Message> {
    match &studio.connect_error {
        None => Space::new().height(0).into(),
        Some(message) => container(text(message.clone()).size(12).font(theme::MONO))
            .padding(10)
            .width(Fill)
            .style(theme::error_box)
            .into(),
    }
}

fn saved(studio: &Studio) -> Element<'_, Message> {
    let mut list = column![].spacing(2).width(Fill);

    if studio.profiles.is_empty() {
        list = list.push(
            text("Saved connections appear here.")
                .size(12)
                .color(theme::TEXT_FAINT),
        );
    }

    for (index, profile) in studio.profiles.iter().enumerate() {
        let active = profile.name == studio.form.name;
        let summary = column![
            text(&profile.name).size(13).font(theme::MONO),
            text(format!(
                "{}@{}:{}/{} · {}",
                profile.user, profile.host, profile.port, profile.database, profile.flavor
            ))
            .size(10)
            .color(theme::TEXT_FAINT),
        ]
        .spacing(2);

        list = list.push(
            row![
                iced::widget::button(summary)
                    .width(Fill)
                    .padding([6, 8])
                    .style(theme::tree_item(active))
                    .on_press(Message::LoadProfile(index)),
                small_button("×", Some(Message::DeleteProfile(index))),
            ]
            .spacing(4)
            .align_y(Center),
        );
    }

    container(
        column![
            text("SAVED").size(10).font(theme::MONO).color(theme::TEXT_FAINT),
            rule::horizontal(1).style(theme::hairline),
            scrollable(list).height(Fill).style(theme::scroller),
        ]
        .spacing(8)
        .padding(14),
    )
    .width(Length::Fixed(300.0))
    .height(Length::Fixed(420.0))
    .style(theme::panel)
    .into()
}
