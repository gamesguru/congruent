#!/usr/bin/env python3
"""Sort JSONL files by origin_server_ts and compact to one object per line."""

import json
import sys
from pathlib import Path


def sort_jsonl(path: Path) -> None:
    events = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            events.append(json.loads(line))

    events.sort(key=lambda e: e.get("origin_server_ts", 0))

    with open(path, "w") as f:
        for event in events:
            f.write(json.dumps(event, separators=(",", ":")) + "\n")

    print(f"Sorted {len(events)} events in {path.name}")


if __name__ == "__main__":
    for arg in sys.argv[1:]:
        sort_jsonl(Path(arg))
