#!/usr/bin/env python3
"""Generate illustrative PNG diagrams for SATIF documentation.

This script programmatically creates two 400x400 PNG images:
1. inputs-to-sdif.png  – illustrates converting heterogeneous input files into a single SDIF database.
2. sdif-to-outputs.png – illustrates transforming an SDIF database into various output files.

The files are written under docs/static/img/ so they can be consumed directly by the Docusaurus site.

Run the script from the repository root:
    python scripts/generate_png_diagrams.py
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

from PIL import Image, ImageDraw, ImageFont  # type: ignore

IMG_SIZE = (400, 400)
BG_COLOR = (255, 255, 255, 0)  # Transparent background
SATIF_BLUE = (59, 130, 246, 255)
SATIF_BLUE_DARK = (29, 78, 216, 255)
SATIF_BLUE_LIGHT = (147, 197, 253, 255)
TEXT_COLOR = (255, 255, 255, 255)
LINE_COLOR = SATIF_BLUE
LINE_WIDTH = 5

# Where to write generated images
OUTPUT_DIR = Path("docs/static/img")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_FONT = ImageFont.load_default()


def _draw_file_boxes(
    draw: ImageDraw.ImageDraw,
    boxes: List[Tuple[int, int, str]],
    colors: List[Tuple[int, int, int, int]],
) -> None:
    """Helper to draw labelled rectangles representing files."""
    for (x, y, label), color in zip(boxes, colors):
        draw.rectangle([x, y, x + 100, y + 40], fill=color, outline=None)
        text_x = x + 20
        text_y = y + 12
        draw.text((text_x, text_y), label, fill=TEXT_COLOR, font=DEFAULT_FONT)


def _draw_arrow(
    draw: ImageDraw.ImageDraw, start: Tuple[int, int], end: Tuple[int, int]
) -> None:
    """Draw a right-pointing arrow from start to end."""
    draw.line([start, end], fill=LINE_COLOR, width=LINE_WIDTH)
    # Arrowhead
    arrow_head = [
        (end[0], end[1]),
        (end[0] - 10, end[1] - 10),
        (end[0] - 10, end[1] + 10),
    ]
    draw.polygon(arrow_head, fill=LINE_COLOR)


def _draw_sdif_cylinder(draw: ImageDraw.ImageDraw, top_left: Tuple[int, int]) -> None:
    """Draw a simple cylinder shape labelled SDIF."""
    x, y = top_left
    width, height = 80, 90
    # Top ellipse
    draw.ellipse([x, y, x + width, y + 20], fill=SATIF_BLUE_DARK)
    # Body
    draw.rectangle([x, y + 10, x + width, y + height - 10], fill=SATIF_BLUE_DARK)
    # Bottom ellipse
    draw.ellipse([x, y + height - 20, x + width, y + height], fill=SATIF_BLUE_DARK)
    # Label (centered)
    try:
        # Pillow ≥ 10 provides textbbox; gives (x0,y0,x1,y1)
        bbox = draw.textbbox((0, 0), "SDIF", font=DEFAULT_FONT)
        text_w, text_h = bbox[2] - bbox[0], bbox[3] - bbox[1]
    except AttributeError:
        # Older Pillow versions
        text_w, text_h = draw.textsize("SDIF", font=DEFAULT_FONT)  # type: ignore[attr-defined]

    text_x = x + (width - text_w) / 2
    text_y = y + (height - text_h) / 2
    draw.text((text_x, text_y), "SDIF", fill=TEXT_COLOR, font=DEFAULT_FONT)


def create_inputs_to_sdif(path: Path) -> None:
    """Create the first diagram: inputs → SDIF."""
    img = Image.new("RGBA", IMG_SIZE, BG_COLOR)
    draw = ImageDraw.Draw(img)

    # Input files on the left
    file_boxes = [(40, 90, "CSV"), (40, 160, "XLSX"), (40, 230, "PDF")]
    box_colors = [SATIF_BLUE, SATIF_BLUE_LIGHT, SATIF_BLUE_DARK]
    _draw_file_boxes(draw, file_boxes, box_colors)

    # Arrow to SDIF
    _draw_arrow(draw, (150, 180), (250, 180))

    # SDIF cylinder on the right
    _draw_sdif_cylinder(draw, (270, 135))

    img.save(path)


def create_sdif_to_outputs(path: Path) -> None:
    """Create the second diagram: SDIF → outputs."""
    img = Image.new("RGBA", IMG_SIZE, BG_COLOR)
    draw = ImageDraw.Draw(img)

    # SDIF on the left
    _draw_sdif_cylinder(draw, (50, 135))

    # Arrow to outputs
    _draw_arrow(draw, (150, 180), (250, 180))

    # Output files on the right
    output_boxes = [(280, 90, "JSON"), (280, 160, "XML"), (280, 230, "CSV")]
    box_colors = [SATIF_BLUE, SATIF_BLUE_LIGHT, SATIF_BLUE_DARK]
    _draw_file_boxes(draw, output_boxes, box_colors)

    img.save(path)


def main() -> None:
    create_inputs_to_sdif(OUTPUT_DIR / "inputs-to-sdif.png")
    create_sdif_to_outputs(OUTPUT_DIR / "sdif-to-outputs.png")
    print(f"Generated diagrams in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
