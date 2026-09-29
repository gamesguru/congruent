#!/usr/bin/env python3
"""Convert rezzy fixtures to conduwuit Pdu JSONL format.

Adds missing fields (hashes, signatures, depth) that conduwuit's Pdu requires.
Processes all JSON fixture files from rezzy and outputs one JSONL per test case.
"""

import json
import sys
from pathlib import Path

rezzy_FIXTURES = Path("/run/media/shane/shane4tb-ent/repos/rezzy/res/ruma_upstream")
OUTPUT_DIR = Path(
    "/run/media/shane/shane4tb-ent/repos/continuwuity/case-study-state-res/fixtures"
)

# Test cases: name -> list of fixture files (in order)
TEST_CASES = {
    "ban-vs-power-levels": [
        "bootstrap-public-chat.json",
        "ban-vs-power-levels-alice.json",
        "ban-vs-power-levels-bob.json",
    ],
    "topic-vs-power-levels": [
        "bootstrap-public-chat.json",
        "topic-vs-power-levels-alice.json",
        "topic-vs-power-levels-bob.json",
    ],
    "concurrent-joins": [
        "bootstrap-public-chat.json",
        "concurrent-joins-charlie.json",
        "concurrent-joins-ella.json",
    ],
    "join-rules-vs-join": [
        "bootstrap-public-chat.json",
        "join-rules-vs-join-common.json",
        "join-rules-vs-join-alice.json",
        "join-rules-vs-join-ella.json",
    ],
    "topic-vs-ban": [
        "bootstrap-public-chat.json",
        "topic-vs-ban-common.json",
        "topic-vs-ban-alice.json",
        "topic-vs-ban-bob.json",
    ],
    "power-levels-admin-vs-mod": [
        "bootstrap-public-chat.json",
        "power-levels-admin-vs-mod-alice.json",
        "power-levels-admin-vs-mod-bob.json",
    ],
    "bootstrap-public-chat": [
        "bootstrap-public-chat.json",
    ],
    "bootstrap-private-chat": [
        "bootstrap-private-chat.json",
    ],
}


def enrich_event(ev: dict, idx: int) -> dict:
    """Add fields required by conduwuit's Pdu that rezzy fixtures lack."""
    enriched = dict(ev)
    if "depth" not in enriched:
        enriched["depth"] = idx + 1
    if "hashes" not in enriched:
        enriched["hashes"] = {"sha256": "dummy"}
    if "signatures" not in enriched:
        enriched["signatures"] = {}
    return enriched


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for test_name, fixture_files in TEST_CASES.items():
        events = []
        for fname in fixture_files:
            fpath = rezzy_FIXTURES / fname
            if not fpath.exists():
                print(f"  SKIP {fname} (not found)", file=sys.stderr)
                continue
            with open(fpath) as f:
                data = json.load(f)
            if isinstance(data, list):
                events.extend(data)
            elif isinstance(data, dict) and "events" in data:
                events.extend(data["events"])

        # Deduplicate by event_id
        seen = set()
        unique = []
        for ev in events:
            eid = ev["event_id"]
            if eid not in seen:
                seen.add(eid)
                unique.append(ev)

        # Enrich and write JSONL
        outpath = OUTPUT_DIR / f"{test_name}.jsonl"
        with open(outpath, "w") as f:
            for i, ev in enumerate(unique):
                enriched = enrich_event(ev, i)
                f.write(json.dumps(enriched, separators=(",", ":")) + "\n")

        print(f"{test_name}: {len(unique)} events -> {outpath.name}")


if __name__ == "__main__":
    main()
