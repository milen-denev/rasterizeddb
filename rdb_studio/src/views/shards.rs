//! Shards: the cluster map this node serves, who owns how much of the hash
//! space, what sits on the shard this connection reached, and anything a
//! two-phase commit left undecided.
//!
//! # Where the connection landed
//!
//! A cluster is reached two ways and the page says which: through a gateway
//! (`SHOW GATEWAY` answers, and the map comes from its home shard), or on one
//! shard directly. Either way the load and the prepared parts are the
//! reached shard's own, not the cluster's, and the headings say so: a
//! cluster-wide total read off one shard would be the number that looks
//! right.
//!
//! # What counts as a fault
//!
//! Two things need attention: blocks of the hash space with no owner (a key
//! there has nowhere to go), and a two-phase part left prepared (it holds
//! its locks until `rdb-shard resolve` finishes it). A gateway draining is
//! a decision, shown but not coloured as an outage.

use iced::widget::{Space, column, container, row, scrollable, text};
use iced::{Element, Fill, Length};

use crate::app::{Message, Studio};
use crate::model::*;
use crate::theme;

use super::{framed, section_label, small_button};

pub fn view(studio: &Studio) -> Element<'_, Message> {
    let reload = || Some(small_button("RELOAD", Some(Message::ReloadShards)));
    let Some(snapshot) = &studio.shards else {
        return framed(
            "SHARDS",
            reload(),
            container(text("Loading…").size(13).color(theme::TEXT_FAINT)).center_x(Fill).center_y(Fill).into(),
        );
    };
    if !snapshot.supported && !snapshot.through_gateway() {
        return framed("SHARDS", reload(), message(UNSUPPORTED_TITLE, unsupported_body(studio.flavor())));
    }
    if snapshot.shards.is_empty() {
        return framed("SHARDS", reload(), message("This node is not part of a cluster", NOT_A_CLUSTER.to_string()));
    }

    let mut body = column![health(snapshot), summary(snapshot), shard_grid(studio, snapshot)].spacing(10);
    body = body.push(load(studio, snapshot));
    if !snapshot.prepared.is_empty() {
        body = body.push(prepared(snapshot));
    }
    if snapshot.through_gateway() {
        body = body.push(gateway(studio, snapshot));
    }
    framed("SHARDS", reload(), scrollable(body).height(Fill).style(theme::scroller).into())
}

const UNSUPPORTED_TITLE: &str = "This server does not report a cluster map";

fn unsupported_body(flavor: Flavor) -> String {
    match flavor {
        Flavor::Postgres => "PostgreSQL has no shard map to read: this page is for a RasterizedDB cluster, reached \
                             through its gateway or on one of its shards."
            .to_string(),
        Flavor::Rasterized => "It did not answer SHOW SHARDS with a map, which is what a build from before \
                               sharding, or one built without it, does. Upgrade the server, or connect to a gateway."
            .to_string(),
    }
}

const NOT_A_CLUSTER: &str = "SHOW SHARDS answered with no shards: this server runs on its own. A cluster is shards \
     started with --role shard --cluster-map <file> --shard-id <n>, and a gateway (rasterizeddb_gateway) in front \
     of them that routes each statement by its key. `rdb-shard` makes the map, applies DDL to every shard, and \
     moves and splits ranges.";

fn message(title: &str, body: String) -> Element<'static, Message> {
    container(
        column![
            text(title.to_string()).size(15).color(theme::TEXT),
            container(Space::new().height(2)).width(72).style(theme::accent_underline),
            text(body).size(12).color(theme::TEXT_DIM),
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

fn health(snapshot: &ShardsSnapshot) -> Element<'static, Message> {
    let mut warnings = column![].spacing(6);
    let mut any = false;

    let covered = snapshot.covered_blocks();
    if covered < ShardsSnapshot::BLOCKS {
        any = true;
        warnings = warnings.push(banner(
            theme::BAD,
            "PART OF THE HASH SPACE HAS NO OWNER",
            format!(
                "The map's shards own {} of the {} blocks. A key that hashes into the rest has no shard: \
                 a write there is refused. Fix the map with rdb-shard and publish it.",
                count(covered),
                count(ShardsSnapshot::BLOCKS)
            ),
        ));
    }
    if !snapshot.prepared.is_empty() {
        any = true;
        warnings = warnings.push(banner(
            theme::WARN,
            "TWO-PHASE PARTS ARE WAITING FOR A DECISION",
            format!(
                "{} transaction part(s) on this shard are prepared and undecided. Each holds its locks until \
                 it is committed or rolled back: `rdb-shard resolve` reads the anchor shard's decision and \
                 finishes it. A gateway that lost its link mid-commit leaves these behind.",
                snapshot.prepared.len()
            ),
        ));
    }
    if snapshot.gateway_value("draining") == Some("true") {
        any = true;
        warnings = warnings.push(banner(
            theme::TEXT_DIM,
            "THE GATEWAY IS DRAINING",
            "It finishes the sessions it has and takes no new ones.".to_string(),
        ));
    }
    if !any {
        return Space::new().height(0).into();
    }
    warnings.into()
}

fn banner(colour: iced::Color, heading: &'static str, body: String) -> Element<'static, Message> {
    let style = if colour == theme::BAD { theme::error_box } else { theme::tile };
    container(column![text(heading).size(11).font(theme::MONO).color(colour), text(body).size(11).color(theme::TEXT_DIM)].spacing(3))
        .padding(9)
        .width(Fill)
        .style(style)
        .into()
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------

fn summary(snapshot: &ShardsSnapshot) -> Element<'static, Message> {
    let (reached, how) = match (snapshot.through_gateway(), snapshot.shards.iter().find(|s| s.this)) {
        (true, _) => ("GATEWAY".to_string(), format!("{}, routes by key", super::elide(snapshot.gateway_value("gateway").unwrap_or(""), 28))),
        (false, Some(s)) => (format!("SHARD {}", s.id), "one shard directly".to_string()),
        (false, None) => ("—".to_string(), String::new()),
    };
    let covered = snapshot.covered_blocks();
    let standbys: usize = snapshot.shards.iter().map(|s| s.standbys.len()).sum();
    column![
        row![
            super::tile("SHARDS", count(snapshot.shards.len() as u64), Some(format!("{standbys} standby(s)"))),
            super::tile(
                "MAP EPOCH",
                snapshot.map_epoch.map_or("—".to_string(), |e| e.to_string()),
                Some("bumped by every published map".to_string()),
            ),
            super::tile("CONNECTED TO", reached, Some(how)),
            super::tile("HASH SPACE OWNED", percent(covered, ShardsSnapshot::BLOCKS), Some("blocks with an owner".to_string())),
            super::tile("PREPARED PARTS", count(snapshot.prepared.len() as u64), Some("undecided two-phase".to_string())),
        ]
        .spacing(10),
        row![
            labelled("CLUSTER", if snapshot.cluster_id.is_empty() { "—".into() } else { snapshot.cluster_id.clone() }),
            labelled("MAP DIGEST", if snapshot.map_digest.is_empty() { "—".into() } else { snapshot.map_digest.clone() }),
            labelled("HOME SHARD", snapshot.gateway_value("home_shard").unwrap_or("—").to_string()),
            labelled("SCHEMA EPOCH", snapshot.gateway_value("schema_epoch").unwrap_or("—").to_string()),
        ]
        .spacing(24),
    ]
    .spacing(10)
    .into()
}

fn labelled(label: &'static str, v: String) -> Element<'static, Message> {
    column![text(label).size(10).font(theme::MONO).color(theme::TEXT_FAINT), text(v).size(12).font(theme::MONO)].spacing(2).into()
}

/// `part` of `whole` as a percentage, to two places below 1%.
fn percent(part: u64, whole: u64) -> String {
    if whole == 0 {
        return "—".to_string();
    }
    let p = part as f64 * 100.0 / whole as f64;
    if p > 0.0 && p < 1.0 { format!("{p:.2}%") } else { format!("{p:.1}%") }
}

// ---------------------------------------------------------------------------
// The map
// ---------------------------------------------------------------------------

/// A bar of `share` (0..=1) of `width` pixels.
fn share_bar(share: f32, width: f32) -> Element<'static, Message> {
    let filled = (share.clamp(0.0, 1.0) * width).max(if share > 0.0 { 1.0 } else { 0.0 });
    container(
        row![
            container(Space::new().height(6)).width(Length::Fixed(filled)).style(theme::accent_underline),
            Space::new().width(Length::Fixed(width - filled)),
        ]
        .align_y(iced::alignment::Vertical::Center),
    )
    .padding([8, 6])
    .into()
}

fn shard_grid<'a>(studio: &'a Studio, snapshot: &'a ShardsSnapshot) -> Element<'a, Message> {
    let layout = super::GridLayout::new(studio, "shards");
    let columns = vec![
        super::GridColumn::new("id", "SHARD", 70.0).right(),
        super::GridColumn::new("address", "ADDRESS", 180.0),
        super::GridColumn::new("this", "", 90.0),
        super::GridColumn::new("share", "SHARE", 80.0).right(),
        super::GridColumn::new("bar", "", 130.0).unsortable(),
        super::GridColumn::new("ranges", "RANGES", 80.0).right(),
        super::GridColumn::new("standbys", "STANDBYS", 200.0),
    ];
    let widths = super::column_widths(&layout, &columns);
    let total = super::grid_total(&widths);

    let mut shards: Vec<&ShardInfo> = snapshot.shards.iter().collect();
    super::sort_rows(&layout, &mut shards, |s, column| match column {
        "address" => super::SortKey::text(&s.address),
        "this" => super::SortKey::number(if s.this { 1.0 } else { 0.0 }),
        "share" | "blocks" => super::SortKey::number(s.blocks as f64),
        "ranges" => super::SortKey::number(s.ranges as f64),
        "standbys" => super::SortKey::number(s.standbys.len() as f64),
        _ => super::SortKey::number(s.id as f64),
    });
    // The biggest owner fills the bar: shares of an even map then read as
    // equal bars, not as thin slivers of 2^48.
    let widest = snapshot.shards.iter().map(|s| s.blocks).max().unwrap_or(1).max(1);
    let bar_width = widths.get(4).copied().unwrap_or(150.0) - 12.0;

    let mut body = column![].spacing(0);
    for (index, s) in shards.into_iter().enumerate() {
        let reached: Element<'a, Message> = if s.this {
            container(super::pill(if snapshot.through_gateway() { "HOME" } else { "REACHED" }, theme::OK)).padding([3, 6]).into()
        } else {
            Space::new().into()
        };
        let line = super::grid_row(
            &widths,
            vec![
                super::grid_cell(s.id.to_string(), true, theme::TEXT),
                super::grid_cell(s.address.clone(), false, theme::TEXT_DIM),
                reached,
                super::grid_cell(
                    percent(s.blocks, ShardsSnapshot::BLOCKS),
                    true,
                    if s.blocks == 0 { theme::WARN } else { theme::TEXT },
                ),
                share_bar(s.blocks as f32 / widest as f32, bar_width.max(10.0)),
                super::grid_cell(count(s.ranges), true, theme::TEXT_DIM),
                super::grid_cell(
                    if s.standbys.is_empty() { "none".to_string() } else { s.standbys.join(", ") },
                    false,
                    if s.standbys.is_empty() { theme::TEXT_FAINT } else { theme::TEXT_DIM },
                ),
            ],
        );
        body = body.push(super::grid_stripe(index, container(line).width(Length::Fixed(total)).into()));
    }

    container(
        column![
            section_label("THE MAP · WHO OWNS WHICH PART OF THE HASH SPACE"),
            container(super::grid_frame(&layout, &widths, super::grid_header(&layout, &columns, &widths), body.into()))
                .height(Length::Fixed(grid_height(snapshot.shards.len()))),
        ]
        .spacing(2),
    )
    .width(Fill)
    .style(theme::panel)
    .into()
}

/// A grid of `rows` on a page that scrolls as a whole: tall enough for
/// every row and a horizontal scrollbar under them, up to 16 rows.
fn grid_height(rows: usize) -> f32 {
    40.0 + rows.min(16) as f32 * 24.0 + 14.0
}

// ---------------------------------------------------------------------------
// Load on the reached shard
// ---------------------------------------------------------------------------

fn load<'a>(studio: &'a Studio, snapshot: &'a ShardsSnapshot) -> Element<'a, Message> {
    let heading = match (snapshot.through_gateway(), snapshot.shards.iter().find(|s| s.this)) {
        (true, Some(s)) => format!("TABLES ON SHARD {} · THE GATEWAY'S HOME, NOT THE CLUSTER", s.id),
        (_, Some(s)) => format!("TABLES ON SHARD {} · THIS SHARD ONLY", s.id),
        _ => "TABLES ON THE REACHED SHARD".to_string(),
    };
    if snapshot.load.is_empty() {
        return container(
            column![
                section_label(heading),
                text("No partitioned table holds rows on this shard yet.").size(12).color(theme::TEXT_FAINT),
            ]
            .spacing(6),
        )
        .padding(8)
        .width(Fill)
        .style(theme::panel)
        .into();
    }
    let layout = super::GridLayout::new(studio, "shards:load");
    let columns = vec![
        super::GridColumn::new("table", "TABLE", 240.0),
        super::GridColumn::new("partitions", "PARTITIONS", 110.0).right(),
        super::GridColumn::new("rows", "ROW SLOTS", 130.0).right(),
        super::GridColumn::new("bytes", "SIZE", 120.0).right(),
    ];
    let widths = super::column_widths(&layout, &columns);
    let total = super::grid_total(&widths);
    let mut tables: Vec<&TableLoad> = snapshot.load.iter().collect();
    super::sort_rows(&layout, &mut tables, |t, column| match column {
        "partitions" => super::SortKey::number(t.partitions as f64),
        "rows" => super::SortKey::number(t.row_slots as f64),
        "bytes" => super::SortKey::number(t.bytes as f64),
        _ => super::SortKey::text(&t.table),
    });
    let mut body = column![].spacing(0);
    for (index, t) in tables.into_iter().enumerate() {
        let line = super::grid_row(
            &widths,
            vec![
                super::grid_cell(t.table.clone(), false, theme::TEXT),
                super::grid_cell(count(t.partitions), true, theme::TEXT_DIM),
                super::grid_cell(count(t.row_slots), true, theme::TEXT_DIM),
                super::grid_cell(bytes(t.bytes), true, theme::TEXT),
            ],
        );
        body = body.push(super::grid_stripe(index, container(line).width(Length::Fixed(total)).into()));
    }
    container(
        column![
            section_label(heading),
            container(super::grid_frame(&layout, &widths, super::grid_header(&layout, &columns, &widths), body.into()))
                .height(Length::Fixed(grid_height(snapshot.load.len()))),
        ]
        .spacing(2),
    )
    .width(Fill)
    .style(theme::panel)
    .into()
}

// ---------------------------------------------------------------------------
// Prepared parts
// ---------------------------------------------------------------------------

fn prepared(snapshot: &ShardsSnapshot) -> Element<'static, Message> {
    let mut list = column![section_label("PREPARED TWO-PHASE PARTS ON THIS SHARD")].spacing(4);
    for gid in &snapshot.prepared {
        list = list.push(text(gid.clone()).size(11).font(theme::MONO).color(theme::WARN));
    }
    list = list.push(
        text("Finish them with `rdb-shard resolve --map <file>`: it asks the anchor shard what was decided.")
            .size(10)
            .color(theme::TEXT_FAINT),
    );
    container(list).padding(8).width(Fill).style(theme::panel).into()
}

// ---------------------------------------------------------------------------
// The gateway
// ---------------------------------------------------------------------------

/// The gateway's rows the summary did not promote: its settings, then its
/// counters, as it reports them.
fn gateway(studio: &Studio, snapshot: &ShardsSnapshot) -> Element<'static, Message> {
    let rest: Vec<Stat> = snapshot
        .gateway
        .iter()
        .filter(|(k, _)| !GATEWAY_PROMOTED.contains(&k.as_str()))
        .map(|(k, v)| Stat { metric: k.clone(), value: v.clone(), detail: String::new() })
        .collect();
    let height = 26.0 + rest.len() as f32 * 19.0;
    column![
        section_label(format!("GATEWAY {} · SETTINGS AND COUNTERS", snapshot.gateway_value("gateway").unwrap_or(""))),
        container(super::metric_grid(studio, "shards:gateway", &rest)).height(Length::Fixed(height)),
    ]
    .spacing(2)
    .into()
}

/// Gateway rows the tiles and the identity row already show.
const GATEWAY_PROMOTED: &[&str] = &["gateway", "cluster_id", "map_epoch", "map_digest", "shards", "home_shard", "schema_epoch"];

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(id: u64, blocks: u64) -> ShardInfo {
        ShardInfo { id, this: id == 0, address: format!("10.0.0.{id}:7000"), standbys: Vec::new(), ranges: 1, blocks }
    }

    #[test]
    fn an_even_map_owns_the_whole_hash_space() {
        let third = ShardsSnapshot::BLOCKS / 3;
        let snapshot = ShardsSnapshot {
            supported: true,
            shards: vec![shard(0, third), shard(1, third), shard(2, ShardsSnapshot::BLOCKS - 2 * third)],
            ..Default::default()
        };
        assert_eq!(snapshot.covered_blocks(), ShardsSnapshot::BLOCKS);
        assert_eq!(percent(third, ShardsSnapshot::BLOCKS), "33.3%");
        assert_eq!(percent(1, 1000), "0.10%");
        assert_eq!(percent(0, 0), "—");
    }

    #[test]
    fn the_gateway_is_known_by_its_rows() {
        let mut snapshot = ShardsSnapshot::default();
        assert!(!snapshot.through_gateway());
        snapshot.gateway = vec![("gateway".into(), "gw-1".into()), ("draining".into(), "false".into())];
        assert!(snapshot.through_gateway());
        assert_eq!(snapshot.gateway_value("gateway"), Some("gw-1"));
    }

    /// The page renders every state without panicking: a cluster through a
    /// gateway with a hole in the map and a prepared part, and one shard
    /// directly with no tables.
    #[test]
    fn every_state_renders() {
        let (mut studio, _) = Studio::new(None);
        for snapshot in [
            None,
            Some(ShardsSnapshot::default()),
            Some(ShardsSnapshot { supported: true, ..Default::default() }),
            Some(ShardsSnapshot {
                supported: true,
                shards: vec![shard(0, 10), shard(1, 0)],
                map_epoch: Some(3),
                gateway: vec![("gateway".into(), "gw".into()), ("draining".into(), "true".into()), ("routed".into(), "12".into())],
                load: vec![TableLoad { table: "orders".into(), partitions: 2, row_slots: 40, bytes: 4096 }],
                prepared: vec!["rdb2pc:gw:1".into()],
                ..Default::default()
            }),
        ] {
            studio.shards = snapshot;
            let _ = view(&studio);
        }
    }
}
