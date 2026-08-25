//! The schema map: tables as boxes, relationships as lines between the exact
//! columns they join.
//!
//! # The layout
//!
//! Foreign keys make the schema a directed graph, and almost always a shallow
//! one: a handful of roots that nothing references, then everything hanging off
//! them. So the layout is a layered one — a table's column is one past the
//! deepest table it references — which puts the parents on the left and the
//! children to their right, and makes every edge point the same way.
//!
//! A force-directed layout would look more organic and be worse here. It gives
//! a different picture every time it runs, so a schema you have learned the
//! shape of rearranges itself on the next refresh; and it has no notion of
//! direction, so `orders → users` and `users → orders` draw identically. The
//! layered layout is deterministic, which means the map is *memorable*.
//!
//! Cycles are what the layering has to survive. A table that references itself,
//! or two that reference each other, would recurse forever, so depth is
//! computed with an explicit visited set and a cycle simply stops descending.

use std::collections::{HashMap, HashSet};

use iced::mouse;
use iced::widget::canvas::{self, Frame, Geometry, Path, Stroke, Text};
use iced::widget::{Space, column, container, row, text};
use iced::{Color, Element, Fill, Point, Rectangle, Renderer, Size, Theme, Vector};

use crate::app::{Message, Studio};
use crate::model::{Relationship, SchemaGraph, TableNode};
use crate::theme;

/// Horizontal distance between layers, and vertical distance between siblings.
const COLUMN_GAP: f32 = 300.0;
const ROW_GAP: f32 = 28.0;
const MARGIN: f32 = 40.0;

pub fn view(studio: &Studio) -> Element<'_, Message> {
    if studio.schema.tables.is_empty() {
        return super::framed(
            "SCHEMA MAP",
            Some(super::small_button("RELOAD", Some(Message::SchemaRelayout))),
            container(
                text("Nothing to draw — this database has no tables.")
                    .size(13)
                    .color(theme::TEXT_FAINT),
            )
            .center_x(Fill)
            .center_y(Fill)
            .into(),
        );
    }

    let controls = row![
        super::small_button("−", Some(Message::SchemaZoom(0.8))),
        text(format!("{:.0}%", studio.schema_zoom * 100.0))
            .size(11)
            .font(theme::MONO)
            .color(theme::TEXT_DIM),
        super::small_button("+", Some(Message::SchemaZoom(1.25))),
        super::small_button("RESET", Some(Message::SchemaRelayout)),
    ]
    .spacing(6)
    .align_y(iced::Center);

    let map = Map {
        graph: &studio.schema,
        pan: studio.schema_pan,
        zoom: studio.schema_zoom,
        cache: &studio.schema_cache,
    };

    let legend = row![
        swatch(theme::RUST, "foreign key"),
        swatch(theme::WARN, "JSON column → shadow table"),
        Space::new().width(Fill),
        text(format!(
            "{} tables · {} relationships",
            studio.schema.tables.len(),
            studio.schema.edges.len()
        ))
        .size(11)
        .font(theme::MONO)
        .color(theme::TEXT_FAINT),
    ]
    .spacing(16)
    .align_y(iced::Center);

    super::framed(
        "SCHEMA MAP",
        Some(controls.into()),
        column![
            iced::widget::Canvas::new(map).width(Fill).height(Fill),
            legend,
        ]
        .spacing(8)
        .into(),
    )
}

fn swatch(colour: Color, label: &str) -> Element<'_, Message> {
    row![
        container(Space::new().width(18).height(2)).style(move |_: &Theme| {
            container::Style {
                background: Some(iced::Background::Color(colour)),
                ..container::Style::default()
            }
        }),
        text(label).size(11).color(theme::TEXT_FAINT),
    ]
    .spacing(6)
    .align_y(iced::Center)
    .into()
}

/// Places every node, in-place.
///
/// Called once when the graph loads rather than every frame: the positions are
/// part of the model so that a later version can let the user drag a node and
/// have it stay where it was put.
pub fn layout(graph: &mut SchemaGraph) {
    let depths = depths(graph);

    // Group by depth, then stack each column, tallest column centred against
    // the others so the picture does not sit against the top edge.
    let mut columns: HashMap<usize, Vec<usize>> = HashMap::new();
    for (index, node) in graph.tables.iter().enumerate() {
        columns.entry(*depths.get(&node.name).unwrap_or(&0)).or_default().push(index);
    }

    let mut heights: HashMap<usize, f32> = HashMap::new();
    for (depth, nodes) in &columns {
        let total: f32 = nodes
            .iter()
            .map(|index| graph.tables[*index].height() + ROW_GAP)
            .sum();
        heights.insert(*depth, total);
    }
    let tallest = heights.values().copied().fold(0.0_f32, f32::max);

    for (depth, nodes) in columns {
        let column_height = heights[&depth];
        let mut y = MARGIN + (tallest - column_height) / 2.0;
        // Sorted by name so two runs over the same schema place the same table
        // in the same slot; a hash map's order would not.
        let mut nodes = nodes;
        nodes.sort_by(|a, b| graph.tables[*a].name.cmp(&graph.tables[*b].name));
        for index in nodes {
            let node = &mut graph.tables[index];
            node.x = MARGIN + depth as f32 * COLUMN_GAP;
            node.y = y;
            y += node.height() + ROW_GAP;
        }
    }
}

/// How many references deep each table is.
///
/// A table's depth is one more than the deepest table it references. The
/// visited set is what makes a cycle terminate: revisiting a table already on
/// the current path returns 0 rather than recursing, so `a → b → a` settles
/// instead of overflowing the stack.
fn depths(graph: &SchemaGraph) -> HashMap<String, usize> {
    let mut parents: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in &graph.edges {
        parents
            .entry(edge.from_table.as_str())
            .or_default()
            .push(edge.to_table.as_str());
    }

    fn depth_of<'a>(
        table: &'a str,
        parents: &HashMap<&'a str, Vec<&'a str>>,
        memo: &mut HashMap<String, usize>,
        path: &mut HashSet<String>,
    ) -> usize {
        if let Some(known) = memo.get(table) {
            return *known;
        }
        if !path.insert(table.to_string()) {
            return 0;
        }
        let depth = parents
            .get(table)
            .map(|references| {
                references
                    .iter()
                    .map(|parent| depth_of(parent, parents, memo, path) + 1)
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        path.remove(table);
        memo.insert(table.to_string(), depth);
        depth
    }

    let mut memo = HashMap::new();
    let mut path = HashSet::new();
    for node in &graph.tables {
        depth_of(&node.name, &parents, &mut memo, &mut path);
    }
    // A table nothing references and that references nothing still needs an
    // entry, or it lands at depth 0 by accident rather than by decision — which
    // is the right place, but the map has to say so.
    for node in &graph.tables {
        memo.entry(node.name.clone()).or_insert(0);
    }
    memo
}

struct Map<'a> {
    graph: &'a SchemaGraph,
    pan: Vector,
    zoom: f32,
    /// Tessellated geometry, kept until something it depends on changes.
    ///
    /// Without it the whole map — every box, every label, every curve — is
    /// re-tessellated on every redraw, including redraws caused by something
    /// else entirely on another part of the screen. A thirty-table schema is a
    /// lot of text to lay out sixty times a second for no reason.
    cache: &'a canvas::Cache,
}

impl canvas::Program<Message> for Map<'_> {
    type State = Interaction;

    fn update(
        &self,
        state: &mut Self::State,
        event: &canvas::Event,
        bounds: Rectangle,
        cursor: mouse::Cursor,
    ) -> Option<canvas::Action<Message>> {
        let Some(position) = cursor.position_in(bounds) else {
            // A drag that leaves the canvas must end, or the map keeps panning
            // against a cursor that is somewhere else entirely.
            state.dragging = None;
            return None;
        };

        match event {
            canvas::Event::Mouse(mouse::Event::ButtonPressed(mouse::Button::Left)) => {
                state.dragging = Some(position);
                Some(canvas::Action::request_redraw().and_capture())
            }
            canvas::Event::Mouse(mouse::Event::ButtonReleased(mouse::Button::Left)) => {
                state.dragging = None;
                None
            }
            canvas::Event::Mouse(mouse::Event::CursorMoved { .. }) => {
                let from = state.dragging?;
                state.dragging = Some(position);
                Some(canvas::Action::publish(Message::SchemaPan(Vector::new(
                    position.x - from.x,
                    position.y - from.y,
                ))))
            }
            canvas::Event::Mouse(mouse::Event::WheelScrolled { delta }) => {
                let lines = match delta {
                    mouse::ScrollDelta::Lines { y, .. } => *y,
                    mouse::ScrollDelta::Pixels { y, .. } => y / 40.0,
                };
                if lines == 0.0 {
                    return None;
                }
                Some(canvas::Action::publish(Message::SchemaZoom(
                    1.0 + lines.clamp(-1.0, 1.0) * 0.15,
                ))
                .and_capture())
            }
            _ => None,
        }
    }

    fn draw(
        &self,
        _state: &Self::State,
        renderer: &Renderer,
        _theme: &Theme,
        bounds: Rectangle,
        _cursor: mouse::Cursor,
    ) -> Vec<Geometry> {
        let geometry = self.cache.draw(renderer, bounds.size(), |frame| {
            frame.fill_rectangle(Point::ORIGIN, bounds.size(), theme::BG);

            frame.translate(self.pan);
            frame.scale(self.zoom);

            let positions: HashMap<&str, &TableNode> = self
                .graph
                .tables
                .iter()
                .map(|node| (node.name.as_str(), node))
                .collect();

            for edge in &self.graph.edges {
                draw_edge(frame, edge, &positions);
            }
            for node in &self.graph.tables {
                draw_node(frame, node);
            }
        });

        vec![geometry]
    }

    fn mouse_interaction(
        &self,
        state: &Self::State,
        bounds: Rectangle,
        cursor: mouse::Cursor,
    ) -> mouse::Interaction {
        if state.dragging.is_some() {
            mouse::Interaction::Grabbing
        } else if cursor.is_over(bounds) {
            mouse::Interaction::Grab
        } else {
            mouse::Interaction::default()
        }
    }
}

#[derive(Default)]
pub struct Interaction {
    dragging: Option<Point>,
}

fn draw_node(frame: &mut Frame, node: &TableNode) {
    let origin = Point::new(node.x, node.y);
    let size = Size::new(TableNode::WIDTH, node.height());

    frame.fill_rectangle(origin, size, theme::PANEL);
    frame.stroke(
        &Path::rectangle(origin, size),
        Stroke::default().with_color(theme::LINE_BRIGHT).with_width(1.0),
    );
    // The header carries the accent, which is what makes a table findable in a
    // map of thirty of them.
    frame.fill_rectangle(origin, Size::new(TableNode::WIDTH, TableNode::HEADER), theme::RUST_DIM);
    frame.fill_rectangle(origin, Size::new(3.0, TableNode::HEADER), theme::RUST);

    frame.fill_text(Text {
        content: node.name.clone(),
        position: Point::new(node.x + 10.0, node.y + 7.0),
        color: theme::TEXT,
        size: 12.0.into(),
        font: theme::MONO,
        ..Text::default()
    });
    frame.fill_text(Text {
        content: crate::model::count(node.rows),
        position: Point::new(node.x + TableNode::WIDTH - 8.0, node.y + 8.0),
        color: theme::TEXT_DIM,
        size: 10.0.into(),
        font: theme::MONO,
        align_x: iced::alignment::Horizontal::Right.into(),
        ..Text::default()
    });

    let mut y = node.y + TableNode::HEADER + 4.0;
    for column in node.columns.iter().take(TableNode::MAX_COLUMNS) {
        let colour = if column.primary_key {
            theme::RUST
        } else if column.unique {
            theme::TEXT
        } else {
            theme::TEXT_DIM
        };
        frame.fill_text(Text {
            content: format!(
                "{}{}",
                if column.primary_key { "◆ " } else { "  " },
                column.name
            ),
            position: Point::new(node.x + 8.0, y),
            color: colour,
            size: 10.0.into(),
            font: theme::MONO,
            ..Text::default()
        });
        frame.fill_text(Text {
            content: short_type(&column.data_type),
            position: Point::new(node.x + TableNode::WIDTH - 8.0, y),
            color: theme::TEXT_FAINT,
            size: 9.0.into(),
            font: theme::MONO,
            align_x: iced::alignment::Horizontal::Right.into(),
            ..Text::default()
        });
        y += TableNode::ROW;
    }

    if node.columns.len() > TableNode::MAX_COLUMNS {
        frame.fill_text(Text {
            content: format!("  +{} more", node.columns.len() - TableNode::MAX_COLUMNS),
            position: Point::new(node.x + 8.0, y),
            color: theme::TEXT_FAINT,
            size: 9.0.into(),
            font: theme::MONO,
            ..Text::default()
        });
    }
}

/// Where on a node's edge a relationship to that column attaches.
///
/// Anchoring at the column rather than at the box is the whole reason to draw
/// this instead of listing the constraints: a table with four foreign keys into
/// the same parent draws four distinguishable lines.
fn anchor(node: &TableNode, column: &str, right_side: bool) -> Point {
    let index = node
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(column))
        .unwrap_or(0)
        .min(TableNode::MAX_COLUMNS.saturating_sub(1));
    Point::new(
        if right_side { node.x + TableNode::WIDTH } else { node.x },
        node.y + TableNode::HEADER + 4.0 + index as f32 * TableNode::ROW + 5.0,
    )
}

fn draw_edge(frame: &mut Frame, edge: &Relationship, nodes: &HashMap<&str, &TableNode>) {
    let (Some(from), Some(to)) = (
        nodes.get(edge.from_table.as_str()),
        nodes.get(edge.to_table.as_str()),
    ) else {
        return;
    };

    // Leave from whichever side faces the target, so a child placed to the left
    // of its parent does not draw a line back through its own box.
    let leaving_right = to.x >= from.x;
    let start = anchor(from, &edge.from_column, leaving_right);
    let end = anchor(to, &edge.to_column, !leaving_right);

    let colour = if edge.kind == "json" { theme::WARN } else { theme::RUST };
    let reach = ((end.x - start.x).abs() * 0.45).clamp(30.0, 140.0);
    let direction = if leaving_right { 1.0 } else { -1.0 };

    let path = Path::new(|builder| {
        builder.move_to(start);
        builder.bezier_curve_to(
            Point::new(start.x + reach * direction, start.y),
            Point::new(end.x - reach * direction, end.y),
            end,
        );
    });
    frame.stroke(
        &path,
        Stroke::default().with_color(colour).with_width(1.4),
    );

    // A dot at the parent end, which is what says which way the reference
    // points without needing an arrowhead that would be three pixels wide.
    frame.fill(&Path::circle(end, 3.0), colour);
}

/// A type name short enough for a 200px node.
fn short_type(data_type: &str) -> String {
    let upper = data_type.to_ascii_uppercase();
    let head = upper.split(['(', ' ']).next().unwrap_or(&upper);
    let short = match head {
        "CHARACTER" | "TEXT" | "VARCHAR" => "text",
        "BIGINT" | "INT8" => "i64",
        "INTEGER" | "INT" | "INT4" => "i32",
        "SMALLINT" | "INT2" => "i16",
        "UBIGINT" => "u64",
        "BOOLEAN" | "BOOL" => "bool",
        "TIMESTAMPTZ" | "TIMESTAMP" => "time",
        "NUMERIC" | "DECIMAL" => "num",
        "DOUBLE" | "REAL" | "FLOAT8" => "f64",
        "UUID" => "uuid",
        "JSONB" | "JSON" => "json",
        other => return other.to_ascii_lowercase(),
    };
    short.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ColumnInfo;

    fn node(name: &str) -> TableNode {
        TableNode {
            name: name.to_string(),
            columns: vec![ColumnInfo {
                position: 1,
                name: "id".into(),
                data_type: "BIGINT".into(),
                nullable: false,
                unique: true,
                primary_key: true,
                default: String::new(),
                indexed: true,
                json_table: String::new(),
            }],
            rows: 0,
            x: 0.0,
            y: 0.0,
        }
    }

    fn edge(from: &str, to: &str) -> Relationship {
        Relationship {
            kind: "fk".into(),
            from_table: from.into(),
            from_column: "id".into(),
            to_table: to.into(),
            to_column: "id".into(),
            constraint: String::new(),
            on_delete: String::new(),
        }
    }

    /// A self-reference and a two-table cycle are both ordinary schemas, and
    /// both would recurse forever without the visited set.
    #[test]
    fn a_cycle_terminates() {
        let mut graph = SchemaGraph {
            tables: vec![node("a"), node("b"), node("tree")],
            edges: vec![edge("a", "b"), edge("b", "a"), edge("tree", "tree")],
        };
        layout(&mut graph);
        assert_eq!(graph.tables.len(), 3, "every table still has a place");
    }

    /// A child is placed to the right of what it references, which is the whole
    /// point of the layering.
    #[test]
    fn a_child_sits_right_of_its_parent() {
        let mut graph = SchemaGraph {
            tables: vec![node("users"), node("orders"), node("order_lines")],
            edges: vec![edge("orders", "users"), edge("order_lines", "orders")],
        };
        layout(&mut graph);

        let x = |name: &str| graph.tables.iter().find(|n| n.name == name).unwrap().x;
        assert!(x("users") < x("orders"));
        assert!(x("orders") < x("order_lines"));
    }

    /// The same schema must draw the same picture twice, or a refresh
    /// rearranges a map the user has learned.
    #[test]
    fn the_layout_is_deterministic() {
        let build = || SchemaGraph {
            tables: vec![node("c"), node("a"), node("b")],
            edges: vec![edge("b", "a"), edge("c", "a")],
        };
        let (mut first, mut second) = (build(), build());
        layout(&mut first);
        layout(&mut second);
        assert_eq!(first.tables, second.tables);
    }

    #[test]
    fn an_anchor_tracks_the_column_it_names() {
        let mut table = node("t");
        table.columns.push(ColumnInfo {
            position: 2,
            name: "owner_id".into(),
            data_type: "BIGINT".into(),
            nullable: true,
            unique: false,
            primary_key: false,
            default: String::new(),
            indexed: false,
            json_table: String::new(),
        });
        let first = anchor(&table, "id", true);
        let second = anchor(&table, "owner_id", true);
        assert!(second.y > first.y, "a later column anchors lower");
        assert_eq!(first.x, TableNode::WIDTH, "the right side is the right edge");
        assert_eq!(anchor(&table, "id", false).x, 0.0);
    }
}
