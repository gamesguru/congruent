#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path


def load_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return path.read_text().splitlines()


def test_name(line: str) -> str:
    try:
        return json.loads(line).get("Test", "") or ""
    except Exception:
        return ""


def merge_results(main_lines: list[str], patch_lines: list[str]) -> list[str]:
    patch_by_test: dict[str, str] = {}
    patch_order: list[str] = []
    for line in patch_lines:
        name = test_name(line)
        if not name:
            continue
        if name not in patch_by_test:
            patch_order.append(name)
        patch_by_test[name] = line

    main_present = {name for line in main_lines if (name := test_name(line))}
    merged: list[str] = []
    emitted: set[str] = set()

    for line in main_lines:
        name = test_name(line)
        if not name or name not in patch_by_test:
            merged.append(line)
            continue

        if name in emitted:
            continue

        merged.append(patch_by_test[name])
        emitted.add(name)

    for name in patch_order:
        if name not in main_present:
            merged.append(patch_by_test[name])

    return merged


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Merge partial Complement JSONL results into the main results file."
    )
    parser.add_argument("main_results")
    parser.add_argument("partial_results")
    parser.add_argument("output")
    args = parser.parse_args()

    main_path = Path(args.main_results)
    patch_path = Path(args.partial_results)
    output_path = Path(args.output)

    merged = merge_results(load_lines(main_path), load_lines(patch_path))
    output_path.write_text("\n".join(merged) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
