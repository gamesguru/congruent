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


def normalize(line: str) -> str:
    """Reduce a result record to just its `Action` and `Test` keys.

    The raw `go test -json` stream carries transient fields (e.g. `Package`,
    `Elapsed`) that are noise in the merged results file and would otherwise
    churn on every re-run. Keep only the stable, meaningful keys.
    """
    try:
        obj = json.loads(line)
    except Exception:
        return line
    return json.dumps({"Action": obj.get("Action"), "Test": obj.get("Test")}, separators=(",", ":"))


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

    main_last_index: dict[str, int] = {}
    for index, line in enumerate(main_lines):
        name = test_name(line)
        if name:
            main_last_index[name] = index

    main_present = set(main_last_index)

    def result_record(line: str) -> str:
        # Normalize legacy/raw records (which may carry `Package`, `Elapsed`,
        # etc.) down to just `Action` and `Test`.
        return normalize(line) if test_name(line) else line

    merged: list[str] = []

    for index, line in enumerate(main_lines):
        name = test_name(line)
        if not name:
            merged.append(line)
            continue

        if main_last_index.get(name) != index:
            continue

        merged.append(result_record(patch_by_test.get(name, line)))

    for name in patch_order:
        if name not in main_present:
            merged.append(result_record(patch_by_test[name]))

    return sorted(merged, key=lambda line: (test_name(line) == "", test_name(line), line))


def sort_jsonl(lines: list[str]) -> list[str]:
    objs = []
    for line in lines:
        if not line.strip():
            continue
        objs.append(json.loads(line))
    objs.sort(key=lambda obj: obj.get("Test", ""))
    return [json.dumps(obj, separators=(",", ":")) for obj in objs]


def dedupe_jsonl(lines: list[str]) -> list[str]:
    by_test: dict[str, str] = {}
    order: list[str] = []
    passthrough: list[str] = []

    for line in lines:
        name = test_name(line)
        if not name:
            passthrough.append(line)
            continue
        if name not in by_test:
            order.append(name)
        by_test[name] = line

    deduped = passthrough[:]
    deduped.extend(by_test[name] for name in order)
    return deduped


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Merge partial Complement JSONL results into the main results file."
    )
    parser.add_argument("main_results", nargs="?")
    parser.add_argument("partial_results", nargs="?")
    parser.add_argument("output", nargs="?")
    parser.add_argument(
        "--sort-in-place",
        action="store_true",
        help="Sort a JSONL results file by .Test and rewrite it in place.",
    )
    parser.add_argument(
        "--dedupe-in-place",
        action="store_true",
        help="Collapse duplicate .Test entries in a JSONL results file and rewrite it in place.",
    )
    args = parser.parse_args()

    if args.sort_in_place:
        if not args.main_results or args.partial_results or args.output:
            raise SystemExit("--sort-in-place takes exactly one path argument")
        path = Path(args.main_results)
        lines = load_lines(path)
        path.write_text("\n".join(sort_jsonl(lines)) + "\n")
        return 0

    if args.dedupe_in_place:
        if not args.main_results or args.partial_results or args.output:
            raise SystemExit("--dedupe-in-place takes exactly one path argument")
        path = Path(args.main_results)
        lines = load_lines(path)
        path.write_text("\n".join(dedupe_jsonl(lines)) + "\n")
        return 0

    if not args.main_results or not args.partial_results or not args.output:
        raise SystemExit("merge mode requires three positional arguments")

    main_path = Path(args.main_results)
    patch_path = Path(args.partial_results)
    output_path = Path(args.output)

    merged = merge_results(load_lines(main_path), load_lines(patch_path))
    output_path.write_text("\n".join(merged) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
