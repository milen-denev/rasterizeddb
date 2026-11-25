//! The application icon, drawn from the logo's own polygons.
//!
//! # Why draw it rather than load a PNG
//!
//! iced takes a window icon as raw RGBA. Decoding a PNG would need the `image`
//! feature (and its decoder crates) for one picture, and shipping raw RGBA would
//! put an opaque blob in the tree. The mark is a handful of straight-edged
//! polygons, so it is drawn here directly, at whatever size is asked, with 4×4
//! samples per pixel for smooth edges. `branding/tools/make_icons.py` draws the
//! same polygons for the Windows `.ico` and the PNGs; change both together.
//!
//! The mark: an angular database (a hexagon cap over three disks coloured as
//! the storage tiers, memory, disk and S3) with two darker copies of its
//! silhouette behind it, the shards.

/// The id a Linux desktop matches against `rdb_studio.desktop` to find the
/// icon and name for the window (Wayland ignores the window icon itself).
pub const APP_ID: &str = "rdb_studio";

/// The edge of the icon handed to the window system, in pixels.
pub const SIZE: u32 = 256;

const BG: [u8; 3] = [0x14, 0x11, 0x10];
const CAP: [u8; 3] = [0xFF, 0x7A, 0x3D];
const DISKS: [[u8; 3]; 3] = [[0xF7, 0x4C, 0x00], [0xCE, 0x42, 0x2B], [0x5B, 0x6E, 0x86]];
const ECHO_NEAR: [u8; 3] = [0x7A, 0x2A, 0x1D];
const ECHO_FAR: [u8; 3] = [0x4E, 0x20, 0x16];

/// The drawing is laid out on a 176×176 canvas, the mark centred in it.
const CANVAS: f32 = 176.0;
const X0: f32 = 10.0;
/// The width of the dark gap that separates each layer from the one behind.
const STROKE: f32 = 5.0;

const CAP_PTS: [(f32, f32); 6] = [(0., 13.), (18., 0.), (82., 0.), (100., 13.), (82., 26.), (18., 26.)];
const SILHOUETTE: [(f32, f32); 8] =
    [(0., 13.), (18., 0.), (82., 0.), (100., 13.), (100., 121.), (82., 134.), (18., 134.), (0., 121.)];

fn disk(i: usize) -> [(f32, f32); 8] {
    let t = 19.0 + 36.0 * i as f32;
    let b = t + 30.0;
    [(0., t), (18., t + 13.), (82., t + 13.), (100., t), (100., b), (82., b + 13.), (18., b + 13.), (0., b)]
}

/// One layer of the drawing, painted in order: its fill, then (if any) a
/// background-coloured stroke along its outline, as the SVG paints it.
struct Layer {
    points: Vec<(f32, f32)>,
    fill: [u8; 3],
    stroke: bool,
}

fn layer(points: &[(f32, f32)], dx: f32, dy: f32, fill: [u8; 3], stroke: bool) -> Layer {
    Layer { points: points.iter().map(|&(x, y)| (x + dx + X0, y + dy)).collect(), fill, stroke }
}

fn layers() -> Vec<Layer> {
    let mut l = vec![
        layer(&SILHOUETTE, 50.0, 6.0, ECHO_FAR, true),
        layer(&SILHOUETTE, 28.0, 21.0, ECHO_NEAR, true),
        layer(&SILHOUETTE, 6.0, 36.0, BG, true),
    ];
    for (i, colour) in DISKS.iter().enumerate() {
        l.push(layer(&disk(i), 6.0, 36.0, *colour, false));
    }
    l.push(layer(&CAP_PTS, 6.0, 36.0, CAP, false));
    l
}

/// Even-odd point-in-polygon test.
fn inside(points: &[(f32, f32)], x: f32, y: f32) -> bool {
    let mut hit = false;
    let mut j = points.len() - 1;
    for i in 0..points.len() {
        let (xi, yi) = points[i];
        let (xj, yj) = points[j];
        if (yi > y) != (yj > y) && x < (xj - xi) * (y - yi) / (yj - yi) + xi {
            hit = !hit;
        }
        j = i;
    }
    hit
}

/// Distance from a point to the polygon's outline.
fn edge_distance(points: &[(f32, f32)], x: f32, y: f32) -> f32 {
    let mut best = f32::INFINITY;
    let mut j = points.len() - 1;
    for i in 0..points.len() {
        let (ax, ay) = points[j];
        let (bx, by) = points[i];
        let (dx, dy) = (bx - ax, by - ay);
        let len2 = dx * dx + dy * dy;
        let t = if len2 == 0.0 { 0.0 } else { (((x - ax) * dx + (y - ay) * dy) / len2).clamp(0.0, 1.0) };
        let (px, py) = (ax + t * dx - x, ay + t * dy - y);
        best = best.min((px * px + py * py).sqrt());
        j = i;
    }
    best
}

/// The colour at one point of the canvas, or `None` where it is transparent.
fn sample(layers: &[Layer], x: f32, y: f32) -> Option<[u8; 3]> {
    let mut colour = None;
    for l in layers {
        if inside(&l.points, x, y) {
            colour = Some(l.fill);
        }
        if l.stroke && edge_distance(&l.points, x, y) <= STROKE / 2.0 {
            colour = Some(BG);
        }
    }
    colour
}

/// The icon as straight (not premultiplied) RGBA, `size` × `size`.
pub fn rgba(size: u32) -> Vec<u8> {
    const N: u32 = 4; // samples per pixel along each axis
    let layers = layers();
    let scale = CANVAS / size as f32;
    let mut out = Vec::with_capacity((size * size * 4) as usize);
    for py in 0..size {
        for px in 0..size {
            let (mut r, mut g, mut b, mut covered) = (0u32, 0u32, 0u32, 0u32);
            for sy in 0..N {
                for sx in 0..N {
                    let x = (px as f32 + (sx as f32 + 0.5) / N as f32) * scale;
                    let y = (py as f32 + (sy as f32 + 0.5) / N as f32) * scale;
                    if let Some([cr, cg, cb]) = sample(&layers, x, y) {
                        r += cr as u32;
                        g += cg as u32;
                        b += cb as u32;
                        covered += 1;
                    }
                }
            }
            if covered == 0 {
                out.extend_from_slice(&[0, 0, 0, 0]);
            } else {
                out.extend_from_slice(&[
                    (r / covered) as u8,
                    (g / covered) as u8,
                    (b / covered) as u8,
                    (covered * 255 / (N * N)) as u8,
                ]);
            }
        }
    }
    out
}

/// The window icon, for the title bar and the taskbar.
pub fn window_icon() -> Option<iced::window::Icon> {
    iced::window::icon::from_rgba(rgba(SIZE), SIZE, SIZE).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_icon_has_the_size_it_claims_and_is_partly_transparent() {
        let px = rgba(64);
        assert_eq!(px.len(), 64 * 64 * 4);
        let alpha: Vec<u8> = px.chunks(4).map(|p| p[3]).collect();
        assert!(alpha.iter().any(|&a| a == 0), "the corners are transparent");
        assert!(alpha.iter().any(|&a| a == 255), "the mark itself is opaque");
    }

    #[test]
    fn the_front_disks_carry_the_tier_colours() {
        let layers = layers();
        // The middle of each front disk, in canvas coordinates.
        for (i, colour) in DISKS.iter().enumerate() {
            let y = 36.0 + 19.0 + 36.0 * i as f32 + 15.0 + 6.5;
            assert_eq!(sample(&layers, X0 + 6.0 + 50.0, y), Some(*colour), "disk {i}");
        }
        assert_eq!(sample(&layers, X0 + 6.0 + 50.0, 36.0 + 13.0), Some(CAP));
    }
}
