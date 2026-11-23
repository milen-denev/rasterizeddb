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
//!
//! A cold setting is editable when the server can *save* it for its next
//! start — PostgreSQL always can, through `ALTER SYSTEM`, and RasterizedDB can
//! from the build whose `SHOW CONFIG` reports `pending`. Its button then says
//! SAVE rather than APPLY, and the row says a restart is waiting until one
//! happens, because a saved value that the running server is not using is
//! exactly the state somebody forgets.
//!
//! # Choices, not spelling
//!
//! A setting with a fixed set of values is a dropdown, so nobody has to know
//! whether this one is spelled `on` or `true`, or that a quorum is written
//! `quorum:2`. A choice that carries a number gets a box for the number
//! beside it. See [`ConfigEntry::choices`] for where the sets come from.

use iced::widget::{Space, column, container, pick_list, row, rule, text, text_input};
use iced::{Center, Element, Fill, Length};

use crate::app::{Message, Studio};
use crate::model::{ConfigChoice, ConfigEntry, Flavor, choice_of};
use crate::theme;

use super::{framed, small_button};

pub fn view(studio: &Studio) -> Element<'_, Message> {
    let needle = studio.config_filter.to_ascii_lowercase();
    let mut matching: Vec<&ConfigEntry> = studio
        .config
        .iter()
        .filter(|entry| {
            needle.is_empty()
                || entry.qualified().to_ascii_lowercase().contains(&needle)
                || entry.help.to_ascii_lowercase().contains(&needle)
        })
        .collect();

    let hot = matching.iter().filter(|e| e.hot).count();
    let saved = matching.iter().filter(|e| e.restart_settable).count();
    let waiting = matching.iter().filter(|e| e.pending_restart).count();

    let layout = super::GridLayout::new(studio, "config");
    let specs = vec![
        super::GridColumn::new("key", "SETTING", 280.0),
        super::GridColumn::new("value", "VALUE", 280.0),
        super::GridColumn::new("help", "WHAT IT DOES", 520.0),
    ];
    let widths = super::column_widths(&layout, &specs);
    let total = super::grid_total(&widths);

    // Sorting flattens the sections. They are an ordering of their own — the
    // settings arrive grouped and each group is announced by a heading — and
    // keeping them while sorting would produce a page of one-row sections in
    // an order nobody asked for. The heading a sorted list loses is the
    // section name, which is the first half of every key in the column beside
    // it, so nothing is actually hidden.
    let grouped = layout.sort.is_none();
    super::sort_rows(&layout, &mut matching, |entry, column| match column {
        "value" => super::SortKey::cell(Some(&entry.value)),
        "help" => super::SortKey::text(&entry.help),
        _ => super::SortKey::text(entry.qualified()),
    });

    let mut body = column![].spacing(0);
    let mut current = String::new();
    let mut index = 0usize;

    for entry in &matching {
        if grouped && entry.section != current {
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
            "{} settings · {} changeable while running · {} changeable at restart{}",
            matching.len(),
            hot,
            saved,
            if waiting > 0 { format!(" · {} waiting for a restart", waiting) } else { String::new() },
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
        note(studio.flavor(), studio.config.iter().any(|e| e.restart_settable)),
        rule::horizontal(1).style(theme::hairline),
        sections,
    ]
    .spacing(8);

    framed("CONFIGURATION", None, body.into())
}

fn note(flavor: Flavor, saves_for_restart: bool) -> Element<'static, Message> {
    let text_body = match (flavor, saves_for_restart) {
        (Flavor::Rasterized, true) =>
            "APPLY changes a setting now, through the subsystem that owns it. SAVE keeps a \
             restart-only setting for the next start; until then the row says a restart is \
             waiting. A flag given on the server's command line wins over a saved value.",
        (Flavor::Rasterized, false) =>
            "A change is applied by the subsystem that owns it — the filter surface, the \
             replication surface — so it takes effect immediately and is not written to disk. \
             It is gone after a restart unless the matching flag is added too. Restart-only \
             settings are set by the flags shown beside them: this server cannot save them.",
        (Flavor::Postgres, _) =>
            "A change is written with ALTER SYSTEM SET and followed by pg_reload_conf(), so it \
             lands in postgresql.auto.conf and survives a restart. A postmaster setting is \
             saved the same way and waits for the restart.",
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

    let control: Element<'_, Message> = if entry.editable() {
        let value = pending.cloned().unwrap_or_else(|| entry.value.clone());
        let editor = value_editor(&key, entry, &value);
        let button = small_button(
            if entry.hot { "APPLY" } else { "SAVE" },
            dirty.then_some(Message::ApplyConfig(key.clone())),
        );
        let mut cell = column![row![editor, button].spacing(6).align_y(Center)].spacing(3);
        if !entry.hot {
            cell = cell.push(restart_line(entry));
        }
        container(cell).padding([2, 6]).into()
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
                    .color(if entry.editable() { theme::TEXT } else { theme::TEXT_DIM })
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

/// The edit for one value: a dropdown when the setting takes a fixed set,
/// with a number box beside a choice that carries one, and a text box
/// otherwise.
fn value_editor<'a>(key: &str, entry: &ConfigEntry, value: &str) -> Element<'a, Message> {
    let choices = entry.choices();
    if choices.is_empty() {
        let key = key.to_string();
        return text_input("", value)
            .on_input({
                let key = key.clone();
                move |v| Message::ConfigEdit(key.clone(), v)
            })
            .on_submit(Message::ApplyConfig(key))
            .padding([4, 6])
            .size(12)
            .font(theme::MONO)
            .style(theme::input)
            .width(Fill)
            .into();
    }

    let current = choice_of(&choices, value);
    let selected: Option<ConfigChoice> = current.as_ref().map(|(at, _)| choices[*at].clone());
    let number = current.as_ref().map(|(_, n)| n.clone()).unwrap_or_default();

    // Switching to a choice that takes a number keeps the number already
    // there, or starts at 1 — the smallest level, the smallest quorum — so
    // the value is complete the moment it is picked.
    let picked_number = if number.is_empty() { "1".to_string() } else { number.clone() };
    let picker = pick_list(choices, selected.clone(), {
        let key = key.to_string();
        move |choice: ConfigChoice| Message::ConfigEdit(key.clone(), choice.value(&picked_number))
    })
    // A value that is none of the choices is shown, not replaced:
    // `vacuum.enabled` reads `per table` until somebody picks one.
    .placeholder(value.to_string())
    .padding([4, 6])
    .text_size(12)
    .width(Fill);

    match selected.filter(|c| c.numeric) {
        None => picker.into(),
        Some(choice) => row![
            picker,
            text_input("n", &number)
                .on_input({
                    let key = key.to_string();
                    move |v| {
                        let digits: String = v.chars().filter(char::is_ascii_digit).collect();
                        Message::ConfigEdit(key.clone(), choice.value(&digits))
                    }
                })
                .on_submit(Message::ApplyConfig(key.to_string()))
                .padding([4, 6])
                .size(12)
                .font(theme::MONO)
                .style(theme::input)
                .width(Length::Fixed(56.0)),
        ]
        .spacing(4)
        .align_y(Center)
        .into(),
    }
}

/// What a restart-only setting is waiting for, under its edit.
fn restart_line(entry: &ConfigEntry) -> Element<'static, Message> {
    let mut line = row![super::pill("RESTART", theme::TEXT_FAINT)].spacing(6).align_y(Center);
    let (message, colour) = if entry.pending_restart {
        (
            if entry.pending.is_empty() {
                "a saved change is waiting for a restart".to_string()
            } else {
                format!("saved: {} — waiting for a restart", entry.pending)
            },
            theme::WARN,
        )
    } else {
        ("takes effect at the next start".to_string(), theme::TEXT_FAINT)
    };
    line = line.push(text(message).size(10).font(theme::MONO).color(colour));
    if entry.source != "command line" {
        return line.into();
    }
    // Its own line: beside the status it is squeezed into a column a word
    // wide, and it is the one note on the row that says the save is moot.
    column![
        line,
        text("set on the server's command line, which wins over a saved value: remove the flag for a save to take effect")
            .size(10)
            .color(theme::WARN)
            .width(Fill),
    ]
    .spacing(2)
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every kind of row: a dropdown, a dropdown with a number, a text box,
    /// a saved restart-only value waiting, and one the server cannot save.
    #[test]
    fn every_kind_of_setting_renders() {
        let (mut studio, _) = Studio::new(None);
        let row = |section: &str, key: &str, value: &str, hot: bool| ConfigEntry {
            section: section.into(),
            key: key.into(),
            value: value.into(),
            hot,
            ..ConfigEntry::default()
        };
        studio.config = vec![
            row("wal", "synchronous_commit", "on", true),
            row("replication", "mode", "quorum:2", true),
            row("replication", "compression", "auto:zstd:1", true),
            row("vacuum", "enabled", "per table", true),
            row("wal", "writer_delay_ms", "200", true),
            ConfigEntry {
                restart_settable: true,
                pending: "32000".into(),
                pending_restart: true,
                source: "command line".into(),
                ..row("server", "batch_size", "10000", false)
            },
            row("cache", "enabled", "false", false),
        ];
        studio.config_edits.insert("replication.mode".into(), "all".into());
        let _ = view(&studio);
    }
}
