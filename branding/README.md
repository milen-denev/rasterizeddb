# Branding

The RasterizedDB mark: an angular database (a hexagon cap over three disks
coloured as the storage tiers, memory `#F74C00`, disk `#CE422B` and S3
`#5B6E86`) with two darker copies of its silhouette behind it, the shards.

| File | Use |
|---|---|
| `rasterizeddb.svg` | The mark on a transparent background |
| `rasterizeddb-tile.svg` | The mark on the console's background, `#141110` |
| `rasterizeddb.ico` | Windows executables (16 to 256 px); copied into `rasterizeddb_core/assets/` and `rdb_studio/assets/`, which their `build.rs` embed (a published crate holds only its own files) |
| `rasterizeddb-{64,128,256,512}.png` | Raster sizes, for places that take no SVG |

`tools/make_icons.py` builds the `.ico` and PNGs from the polygons;
`rdb_studio/src/icon.rs` draws the same polygons for the studio's window icon.
Change both together.
