//! The key/value store: RasterizedDB's Redis-compatible cache, browsed.
//!
//! Keys are listed by `KV SCAN`, a page at a time, so a store of millions
//! of keys is never read whole; MORE goes on from where the last page
//! stopped. A key's value is read by its type, a long list or sorted set in
//! part. Deleting a key asks first, as dropping a table does.

use iced::widget::{Space, button, column, container, row, scrollable, text, text_input};
use iced::{Center, Element, Fill, Length};

use crate::app::{Message, Refresh, Studio};
use crate::model::{Flavor, count};
use crate::theme;

use super::{confirm, framed, plain_cell, result_grid, small_button};

pub fn view(studio: &Studio) -> Element<'_, Message> {
    if studio.flavor() != Flavor::Rasterized {
        return framed(
            "KEYS",
            None,
            container(
                text("PostgreSQL has no key/value store; this tab browses RasterizedDB's.")
                    .size(13)
                    .color(theme::TEXT_FAINT),
            )
            .center_x(Fill)
            .center_y(Fill)
            .into(),
        );
    }
    let kv = &studio.kv;

    let toolbar = row![
        text("MATCH").size(11).font(theme::MONO).color(theme::TEXT_FAINT),
        text_input("*", &kv.pattern)
            .on_input(Message::KvPattern)
            .on_submit(Message::KvScan(false))
            .padding([4, 6])
            .size(12)
            .font(theme::MONO)
            .style(theme::input)
            .width(Length::Fixed(280.0)),
        small_button("SCAN", Some(Message::KvScan(false))),
        text(format!(
            "{} key{} listed{}{}",
            count(kv.keys.len() as u64),
            if kv.keys.len() == 1 { "" } else { "s" },
            if kv.cursor.is_some() { ", more to scan" } else { "" },
            kv.size.map(|n| format!(" · {} in the store", count(n))).unwrap_or_default(),
        ))
        .size(11)
        .font(theme::MONO)
        .color(theme::TEXT_FAINT),
        Space::new().width(Fill),
    ]
    .spacing(8)
    .align_y(Center);

    let mut list = column![].spacing(1);
    for key in &kv.keys {
        let selected = kv.selected.as_ref().is_some_and(|e| &e.key == key);
        list = list.push(
            button(text(key.clone()).size(12).font(theme::MONO))
                .width(Fill)
                .padding([3, 8])
                .style(theme::tree_item(selected))
                .on_press(Message::KvSelect(key.clone())),
        );
    }
    if kv.cursor.is_some() {
        list = list.push(small_button("MORE", Some(Message::KvScan(true))));
    }
    let keys = container(scrollable(list).style(theme::scroller))
        .width(Length::Fixed(340.0))
        .height(Fill)
        .padding(4)
        .style(theme::panel);

    framed(
        "KEYS",
        None,
        column![toolbar, row![keys, detail(studio)].spacing(10).height(Fill)]
            .spacing(8)
            .into(),
    )
}

fn detail(studio: &Studio) -> Element<'_, Message> {
    let Some(entry) = &studio.kv.selected else {
        return container(text("Select a key.").size(13).color(theme::TEXT_FAINT))
            .center_x(Fill)
            .center_y(Fill)
            .into();
    };
    if entry.kind == "none" {
        return container(
            text(format!("`{}` is gone: it expired or was deleted since the scan.", entry.key))
                .size(12)
                .color(theme::TEXT_DIM),
        )
        .padding(10)
        .into();
    }
    let ttl = match entry.ttl {
        -1 => "no expiry".to_string(),
        t if t < 0 => "expired".to_string(),
        t => format!("expires in {} s", count(t as u64)),
    };
    let header = row![
        text(entry.key.clone()).size(13).font(theme::MONO).color(theme::TEXT),
        super::pill(entry.kind.to_ascii_uppercase(), theme::RUST),
        text(ttl).size(11).font(theme::MONO).color(theme::TEXT_DIM),
        Space::new().width(Fill),
        button(text("DELETE").size(11).font(theme::MONO))
            .padding([5, 10])
            .style(theme::destructive)
            .on_press(confirm(
                "Delete key",
                format!("This deletes `{}` from the key/value store.", entry.key),
                format!("KV DEL {}", crate::dialect::quote_literal(&entry.key)),
                Refresh::Keys,
            )),
    ]
    .spacing(8)
    .align_y(Center);

    let body: Element<'_, Message> = match &entry.value {
        None => text(format!(
            "A {} is not shown here; read it with KV commands in the query editor.",
            entry.kind
        ))
        .size(12)
        .color(theme::TEXT_DIM)
        .into(),
        Some(grid) => {
            let mut body = column![].spacing(4);
            if matches!(entry.kind.as_str(), "list" | "zset")
                && grid.rows.len() >= crate::dialect::KV_SHOWN
            {
                body = body.push(
                    text(format!("the first {} items", crate::dialect::KV_SHOWN))
                        .size(11)
                        .color(theme::WARN),
                );
            }
            body.push(result_grid(
                grid,
                super::GridLayout::new(studio, format!("kv:{}", entry.kind)),
                |_| false,
                |_, _, value| plain_cell(value, false),
                None,
            ))
            .into()
        }
    };

    container(column![header, body].spacing(8))
        .width(Fill)
        .height(Fill)
        .padding(8)
        .style(theme::panel)
        .into()
}
