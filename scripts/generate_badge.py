#!/usr/bin/env python3
"""
Generates an SVG badge for test results.
Takes three arguments: passed, failed, and skipped test counts.
Outputs tests.svg in the current directory.

Created on Mon Jun 29 16:24:55 2026
@author: shane
"""

import sys

passed = int(sys.argv[1])
failed = int(sys.argv[2])
skipped = int(sys.argv[3])


def text_width(text: str) -> float:
    """Calculate approximate pixel width for a given text string."""
    return sum(7.5 if c.isupper() else 6 for c in text) + 12


blocks = [{"text": "tests", "color": "#555"}]
blocks.append({"text": f"{passed} pass", "color": "#4c1"})
blocks.append({"text": f"{failed} fail", "color": "#e05d44"})
blocks.append({"text": f"{skipped} skip", "color": "#9f9f9f"})

total_width = sum(text_width(b["text"]) for b in blocks)
svg = f"""<svg xmlns="http://www.w3.org/2000/svg"
  width="{total_width}" height="20">
  <linearGradient id="b" x2="0" y2="100%">
    <stop offset="0" stop-color="#bbb" stop-opacity=".1"/>
    <stop offset="1" stop-opacity=".1"/>
  </linearGradient>
  <mask id="a">
    <rect width="{total_width}" height="20" rx="3" fill="#fff"/>
  </mask>
  <g mask="url(#a)">\n"""

x = 0
for b in blocks:
    w = text_width(b["text"])
    svg += f'<rect x="{x}" width="{w}" height="20" fill="{b["color"]}"/>'
    x += w

svg += f'<rect width="{total_width}" height="20" fill="url(#b)"/></g>'
svg += (
    '<g fill="#fff" text-anchor="middle" '
    'font-family="DejaVu Sans,Verdana,Geneva,sans-serif" font-size="11">'
)

x = 0
for b in blocks:
    w = text_width(b["text"])
    cx = x + w / 2
    svg += f'<text x="{cx}" y="15" fill="#010101" '
    svg += f'fill-opacity=".3">{b["text"]}</text>'
    svg += f'<text x="{cx}" y="14">{b["text"]}</text>'
    x += w

svg += "</g></svg>"

with open("tests.svg", "w", encoding="utf-8") as f:
    f.write(svg)
