"""Builds branding/rasterizeddb.ico and the PNG sizes from the logo's polygons.

Run from the repository root:  python3 branding/tools/make_icons.py
The geometry is the website's "tiers" mark: an angular database (hexagon cap,
three disks coloured memory / disk / S3) with two shard echoes behind it.
rdb_studio/src/icon.rs draws the same polygons for the window icon.
"""
from PIL import Image, ImageDraw

BG = (0x14, 0x11, 0x10)
FILL = {"cap": (0xFF, 0x7A, 0x3D), "d0": (0xF7, 0x4C, 0x00), "d1": (0xCE, 0x42, 0x2B), "d2": (0x5B, 0x6E, 0x86),
        "e1": (0x7A, 0x2A, 0x1D), "e2": (0x4E, 0x20, 0x16)}
W, H, X0 = 176, 176, 10          # a square canvas; the 156-wide mark centred in it

def poly(pts, dx, dy):
    return [(x + dx + X0, y + dy) for x, y in pts]

CAP = [(0, 13), (18, 0), (82, 0), (100, 13), (82, 26), (18, 26)]
SIL = [(0, 13), (18, 0), (82, 0), (100, 13), (100, 121), (82, 134), (18, 134), (0, 121)]
def disk(i):
    t = 19 + 36 * i; b = t + 30
    return [(0, t), (18, t + 13), (82, t + 13), (100, t), (100, b), (82, b + 13), (18, b + 13), (0, b)]

def draw(px):
    s = px * 4 / W
    im = Image.new("RGBA", (px * 4, px * 4), (0, 0, 0, 0))
    d = ImageDraw.Draw(im)
    sc = lambda pts: [(x * s, y * s) for x, y in pts]
    stroke = max(1, round(5 * s))
    for off, col in (((50, 6), "e2"), ((28, 21), "e1")):
        d.polygon(sc(poly(SIL, *off)), fill=FILL[col] + (255,), outline=BG + (255,), width=stroke)
    d.polygon(sc(poly(SIL, 6, 36)), fill=BG + (255,), outline=BG + (255,), width=stroke)
    for i in range(3):
        d.polygon(sc(poly(disk(i), 6, 36)), fill=FILL[f"d{i}"] + (255,))
    d.polygon(sc(poly(CAP, 6, 36)), fill=FILL["cap"] + (255,))
    return im.resize((px, px), Image.LANCZOS)

sizes = [16, 24, 32, 48, 64, 128, 256]
images = [draw(p) for p in sizes]
images[-1].save("branding/rasterizeddb.ico", sizes=[(p, p) for p in sizes], append_images=images[:-1])
for p in (64, 128, 256, 512):
    draw(p).save(f"branding/rasterizeddb-{p}.png")
print("ok")
