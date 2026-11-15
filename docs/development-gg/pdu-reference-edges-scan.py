#!/usr/bin/env python3
"""Inventory event-id-shaped strings in a directory of PDU JSONL files.

Usage:
    python3 docs/development-gg/pdu-reference-edges-scan.py ../dags/merged

Each line is treated as an event when it has a string ``type``. The scan looks
under ``prev_events``, ``auth_events``, ``redacts``, ``content`` and
``unsigned`` and groups matches by event type and normalised JSON path, with
array indexes collapsed to ``[]``. This is the scan behind
``2026-11-14-pdu-reference-edges.md``; it is an inventory of where event IDs
appear, not a check that any reference stays inside its room.
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter
from pathlib import Path

EVENT_ID = re.compile(r"^\$[^\s]+$")
ROOTS = ("prev_events", "auth_events", "redacts", "content", "unsigned")


def walk(value, path, hits):
    if isinstance(value, dict):
        for key, child in value.items():
            walk(child, f"{path}.{key}" if path else key, hits)
    elif isinstance(value, list):
        for child in value:
            walk(child, f"{path}[]", hits)
    elif isinstance(value, str) and EVENT_ID.match(value):
        hits.append(path)


def event_paths(event, hits):
    for key in ROOTS:
        if key in event:
            walk(event[key], key, hits)


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <merged-corpus-dir>", file=sys.stderr)
        return 2

    corpus = Path(sys.argv[1])
    if not corpus.is_dir():
        print(f"not a directory: {corpus}", file=sys.stderr)
        return 2

    counts = Counter()
    files = 0
    events = 0
    for path in sorted(corpus.glob("*.jsonl")):
        if path.name.endswith(".rezzy-meta.jsonl"):
            continue
        files += 1
        with path.open(encoding="utf-8", errors="replace") as source:
            for line in source:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(event, dict) or not isinstance(event.get("type"), str):
                    continue
                events += 1
                hits = []
                event_paths(event, hits)
                for hit in hits:
                    counts[(event["type"], hit)] += 1

    for (event_type, path), count in counts.most_common():
        print(f"{count:8d}  {event_type:45s}  {path}")
    print(f"\n{files} files, {events} events, {sum(counts.values())} matches", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
