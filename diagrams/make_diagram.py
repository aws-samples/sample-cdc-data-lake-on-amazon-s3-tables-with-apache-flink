#!/usr/bin/env python3
"""Generate architecture.drawio + architecture.png from ONE layout table.

Source of truth for the zero-etl-s3-tables-flink-cdc blog architecture diagram.
- .drawio: standard draw.io XML with official AWS icon PNGs embedded (base64),
  fully editable in draw.io / app.diagrams.net.
- .png: rendered with PIL from the same coordinates -> perfectly straight
  arrows, 96px icons, tight canvas.

Icons come from the official AWS Architecture Icons deck that awsdac caches
locally (ppt/media/*.png). Re-run after editing NODES/EDGES below.
"""
import base64
import glob
import os

from PIL import Image, ImageDraw, ImageFont

HERE = os.path.dirname(os.path.abspath(__file__))
MEDIA = glob.glob(os.path.expanduser(
    "~/.cache/awsdac/*-AWS-Architecture-Icons-Deck_For-Light-BG_*.pptx/ppt/media"))[0]

ICON = 96  # px
ROW_Y = 340  # vertical center of the flow row

# name: (icon file, center-x, label lines)
NODES = {
    "SourceDB": ("image612.png", 220, ["MySQL / PostgreSQL", "(binlog / WAL)"]),
    "MSF":      ("image200.png", 700, ["Managed Service for Apache Flink", "(Flink CDC 3.6)"]),
    "S3Tables": ("image1660.png", 1030, ["Amazon S3 Tables", "(Apache Iceberg)"]),
    "GlueDC":   ("image206.png", 1360, ["AWS Glue Data Catalog", "(s3tablescatalog)"]),
    "Athena":   ("image274.png", 1720, ["Athena / Redshift", "Spark / Trino"]),
}

# (source, target, label, dashed)
EDGES = [
    ("SourceDB", "MSF", "CDC changelog", False),
    ("MSF", "S3Tables", "Iceberg REST (SigV4)", False),
    ("S3Tables", "GlueDC", "analytics integration", True),
    ("GlueDC", "Athena", "SQL query", False),
]

# name: (label, header icon, x0, y0, x1, y1, stroke rgb, dashed)
BOXES = {
    "SelfManaged": ("Self-managed", "image51.png", 40, 190, 400, 490, (125, 137, 152), False),
    "AWSCloud": ("AWS Cloud", "image43.png", 460, 80, 1940, 600, (35, 47, 62), False),
    "Region": ("Region", "image45.png", 500, 140, 1900, 560, (0, 164, 166), True),
    "VPC": ("VPC", "image18.png", 540, 200, 860, 500, (105, 59, 197), False),
}
W, H = 1980, 640


def _icon(path):
    return Image.open(os.path.join(MEDIA, path)).convert("RGBA")


def _b64(path):
    with open(os.path.join(MEDIA, path), "rb") as f:
        return base64.b64encode(f.read()).decode()


def _font(size, bold=False):
    for cand in ("/System/Library/Fonts/Helvetica.ttc",
                 "/Library/Fonts/Arial.ttf"):
        if os.path.exists(cand):
            return ImageFont.truetype(cand, size, index=1 if bold else 0)
    return ImageFont.load_default()


def render_png(out):
    img = Image.new("RGB", (W, H), "white")
    d = ImageDraw.Draw(img)
    f_box = _font(24, bold=True)
    f_lbl = _font(19)
    f_edge = _font(17)

    def dashed_rect(x0, y0, x1, y1, color, width=2, dash=8):
        for (a, b, horiz) in ((y0, y0, True), (y1, y1, True)):
            x = x0
            while x < x1:
                d.line([(x, a), (min(x + dash, x1), b)], fill=color, width=width)
                x += dash * 2
        for x in (x0, x1):
            y = y0
            while y < y1:
                d.line([(x, y), (x, min(y + dash, y1))], fill=color, width=width)
                y += dash * 2

    for label, hicon, x0, y0, x1, y1, rgb, dashed in BOXES.values():
        if dashed:
            dashed_rect(x0, y0, x1, y1, rgb)
        else:
            d.rectangle([x0, y0, x1, y1], outline=rgb, width=2)
        hi = _icon(hicon).resize((36, 36))
        img.paste(hi, (x0, y0), hi)
        d.text((x0 + 44, y0 + 6), label, fill=(0, 0, 0), font=f_box)

    half = ICON // 2
    for icon, cx, lines in NODES.values():
        ic = _icon(icon).resize((ICON, ICON))
        img.paste(ic, (cx - half, ROW_Y - half), ic)
        ty = ROW_Y + half + 8
        for line in lines:
            tw = d.textlength(line, font=f_lbl)
            d.text((cx - tw / 2, ty), line, fill=(0, 0, 0), font=f_lbl)
            ty += 24

    for src, dst, label, dashed in EDGES:
        x0 = NODES[src][1] + half + 6
        x1 = NODES[dst][1] - half - 6
        y = ROW_Y
        if dashed:
            x = x0
            while x < x1 - 10:
                d.line([(x, y), (min(x + 8, x1 - 10), y)], fill=(80, 80, 80), width=3)
                x += 16
        else:
            d.line([(x0, y), (x1 - 10, y)], fill=(80, 80, 80), width=3)
        d.polygon([(x1, y), (x1 - 14, y - 7), (x1 - 14, y + 7)], fill=(80, 80, 80))
        tw = d.textlength(label, font=f_edge)
        d.text(((x0 + x1) / 2 - tw / 2, y - 30), label, fill=(60, 60, 60), font=f_edge)

    img.save(out)
    print(f"wrote {out} ({W}x{H})")


def render_drawio(out):
    cells = []
    cid = [2]

    def add(xml):
        cells.append(xml)

    def nid():
        cid[0] += 1
        return f"n{cid[0]}"

    box_style = ("rounded=0;fillColor=none;strokeColor=#{rgb};verticalAlign=top;"
                 "align=left;spacingLeft=8;fontSize=16;fontStyle=1;html=1;{dash}")
    for label, _hi, x0, y0, x1, y1, rgb, dashed in BOXES.values():
        style = box_style.format(rgb="%02x%02x%02x" % rgb,
                                 dash="dashed=1;" if dashed else "")
        add(f'<mxCell id="{nid()}" value="{label}" style="{style}" vertex="1" parent="1">'
            f'<mxGeometry x="{x0}" y="{y0}" width="{x1-x0}" height="{y1-y0}" as="geometry"/></mxCell>')

    node_ids = {}
    for name, (icon, cx, lines) in NODES.items():
        i = nid()
        node_ids[name] = i
        label = "&lt;br&gt;".join(lines)
        style = (f"shape=image;image=data:image/png,{_b64(icon)};"
                 "verticalLabelPosition=bottom;verticalAlign=top;fontSize=15;html=1;imageAspect=1;")
        add(f'<mxCell id="{i}" value="{label}" style="{style}" vertex="1" parent="1">'
            f'<mxGeometry x="{cx-ICON//2}" y="{ROW_Y-ICON//2}" width="{ICON}" height="{ICON}" as="geometry"/></mxCell>')

    for src, dst, label, dashed in EDGES:
        style = ("endArrow=open;endSize=10;html=1;fontSize=13;strokeWidth=2;"
                 "exitX=1;exitY=0.5;entryX=0;entryY=0.5;"
                 + ("dashed=1;" if dashed else ""))
        add(f'<mxCell id="{nid()}" value="{label}" style="{style}" edge="1" parent="1" '
            f'source="{node_ids[src]}" target="{node_ids[dst]}">'
            f'<mxGeometry relative="1" as="geometry"/></mxCell>')

    xml = ('<mxfile host="app.diagrams.net">'
           '<diagram id="arch" name="architecture">'
           f'<mxGraphModel dx="1000" dy="600" grid="1" gridSize="10" page="1" '
           f'pageWidth="{W}" pageHeight="{H}"><root>'
           '<mxCell id="0"/><mxCell id="1" parent="0"/>'
           + "".join(cells) + "</root></mxGraphModel></diagram></mxfile>")
    with open(out, "w", encoding="utf-8") as f:
        f.write(xml)
    print(f"wrote {out} ({os.path.getsize(out)//1024} KB)")


if __name__ == "__main__":
    render_png(os.path.join(HERE, "architecture.png"))
    render_drawio(os.path.join(HERE, "architecture.drawio"))
