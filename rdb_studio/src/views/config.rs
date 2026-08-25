//! The configuration screen.
//!
//! # Hot reload, and being honest about it
//!
//! Some knobs a running server can change and some it cannot, and the
//! difference is not cosmetic: `batch_size` is read once into a `OnceLock` that
//! every later reader assumes is stable, so there is no safe way to change it
//! mid-flight. The server says which is which — RasterizedDB through
//! `SHOW CONFIG`'s `hot_reload` column, PostgreSQL through `pg_settings.context`
//! — and this screen shows the cold ones with their value, greyed, and the flag
//! or file that actually changes them.
//!
//! The alternative, offering an edit box on everything and relaying the
//! server's refusal, teaches a person that half the screen does not work.

use iced::widget::{Space, column, container, row, rule, text, text_input};
use iced::{Center, Element, Fill, Length};

use crate::app::{Message, Studio};
use crate::model::{ConfigEntry, Flavor};
use crate::theme;

use super::{framed, small_button};

pub fn view(studio: &Studio) -> Element<'_, Message> {
    let needle = studio.config_filter.to_ascii_lowercase();
    let matching: Vec<&ConfigEntry> = studio
        .config
        .iter()
        .filter(|entry| {
            needle.is_empty()
                || entry.qualified().to_ascii_lowercase().contains(&needle)
                || entry.help.to_ascii_lowercase().contains(&needle)
        })
        .collect();

    let hot = matching.iter().filter(|e| e.hot).count();

    let layout = super::GridLayout::new(studio, "config");
    let specs = vec![
        super::GridColumn::new("key", "SETTING", 280.0),
        super::GridColumn::new("value", "VALUE", 280.0),
        super::GridColumn::new("help", "WHAT IT DOES", 520.0),
    ];
    let widths = super::column_widths(&layout, &specs);
    let total = super::grid_total(&widths);

    let mut body = column![].spacing(0);
    let mut current = String::new();
    let mut index = 0usize;

    for entry in &matching {
        if entry.section != current {
            current = entry.section.clone();
            index = 0;
            body = body.push(
                container(
                    text(current.to_ascii_uppercase())
                        .size(10)
                        .font(theme::MONO)
                        .color(theme::TEXT_FAINT),
                )
                .padding([6, 8])
                .width(Length::Fixed(total)),
            );
        }
        body = body.push(super::grid_stripe(
            index,
            container(line(studio, entry, &widths))
                .width(Length::Fixed(total))
                .into(),
        ));
        index += 1;
    }

    let sections = super::grid_frame(
        &layout,
        &widths,
        super::grid_header(&layout, &specs, &widths),
        body.into(),
    );

    let toolbar = row![
        text_input("filter…", &studio.config_filter)
            .on_input(Message::ConfigFilter)
            .padding([5, 8])
            .size(12)
            .font(theme::MONO)
            .style(theme::input)
            .width(Length::Fixed(260.0)),
        text(format!(
            "{} settings · {} changeable while running",
            matching.len(),
            hot
        ))
        .size(11)
        .font(theme::MONO)
        .color(theme::TEXT_FAINT),
        Space::new().width(Fill),
        small_button("RELOAD", Some(Message::SelectTab(crate::app::Tab::Config))),
    ]
    .spacing(10)
    .align_y(Center);

    let body = column![
        toolbar,
        note(studio.flavor()),
        rule::horizontal(1).style(theme::hairline),
        sections,
    ]
    .spacing(8);

    framed("CONFIGURATION", None, body.into())
}

fn note(flavor: Flavor) -> Element<'static, Message> {
    let text_body = match flavor {
        Flavor::Rasterized =>
            "A change is applied by the subsystem that owns it — the filter surface, the \
             replication surface — so it takes effect immediately and is not written to disk. \
             It is gone after a restart unless the matching flag is added too.",
        Flavor::Postgres =>
            "A change is written with ALTER SYSTEM SET and followed by pg_reload_conf(), so it \
             lands in postgresql.auto.conf and survives a restart. Settings whose context is \
             postmaster still need one.",
    };
    container(text(text_body).size(11).color(theme::TEXT_DIM))
        .padding(8)
        .width(Fill)
        .style(theme::panel_flat)
        .into()
}

fn line<'a>(
    studio: &'a Studio,
    entry: &'a ConfigEntry,
    widths: &[f32],
) -> Element<'a, Message> {
    let key = entry.qualified();
    let pending = studio.config_edits.get(&key);
    let dirty = pending.is_some_and(|value| value != &entry.value);

    let control: Element<'_, Message> = if entry.hot {
        let value = pending.cloned().unwrap_or_else(|| entry.value.clone());
        let apply_key = key.clone();
        container(
            row![
                text_input("", &value)
                    .on_input({
                        let key = key.clone();
                        move |v| Message::ConfigEdit(key.clone(), v)
                    })
                    .on_submit(Message::ApplyConfig(key.clone()))
                    .padding([4, 6])
                    .size(12)
                    .font(theme::MONO)
                    .style(theme::input)
                    .width(Fill),
                small_button("APPLY", dirty.then_some(Message::ApplyConfig(apply_key))),
            ]
            .spacing(6)
            .align_y(Center),
        )
        .padding([2, 6])
        .into()
    } else {
        container(
            row![
                text(if entry.value.is_empty() { "—" } else { &entry.value })
                    .size(12)
                    .font(theme::MONO)
                    .color(theme::TEXT_DIM)
                    .width(Fill),
                super::pill("RESTART", theme::TEXT_FAINT),
            ]
            .spacing(6)
            .align_y(Center),
        )
        .padding([4, 6])
        .into()
    };

    super::grid_row(
        widths,
        vec![
            container(
                text(&entry.key)
                    .size(12)
                    .font(theme::MONO)
                    .color(if entry.hot { theme::TEXT } else { theme::TEXT_DIM })
                    .width(Fill),
            )
            .padding([5, 6])
            .into(),
            control,
            container(
                column![
                    text(&entry.help).size(11).color(theme::TEXT_FAINT),
                    text(&entry.set_with).size(10).font(theme::MONO).color(theme::TEXT_FAINT),
                ]
                .spacing(1),
            )
            .padding([2, 6])
            .into(),
        ],
    )
    .into()
}
