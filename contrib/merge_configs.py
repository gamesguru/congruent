#!/usr/bin/env python3
"""
Merge two continuwuity TOML config files.

Usage:
    ./merge_configs.py <config_a> <config_b> [-e <example_config>]

Operations:
    1. Union bypassed_signature_events (ordered, deduped) -> both files
    2. Compare against example config for missing keys -> prints report
    3. Report value differences between the two configs
"""

import argparse
import re
from collections import OrderedDict

# Keys that are always instance-specific and should not be diffed.
INSTANCE_KEYS = frozenset(("server_name", "support_email", "registration_token"))


def extract_event_ids(content):
    """Extract event IDs from bypassed_signature_events, preserving order."""
    match = re.search(r"bypassed_signature_events\s*=\s*\[(.*?)\]", content, re.DOTALL)
    if not match:
        return []
    return re.findall(r'"(\$[^"]+)"', match.group(1))


def ordered_dedup(lst):
    """Deduplicate list preserving order."""
    seen = set()
    return [x for x in lst if x not in seen and not seen.add(x)]


def ordered_union(list_a, list_b):
    """Merge two lists preserving order from A, appending new from B."""
    seen = set()
    result = []
    for item in list_a:
        if item not in seen:
            seen.add(item)
            result.append(item)
    for item in list_b:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def replace_event_ids(content, merged_ids):
    """Replace the bypassed_signature_events block in content string."""
    lines = [f'    "{eid}",' for eid in merged_ids]
    new_block = "bypassed_signature_events = [\n" + "\n".join(lines) + "\n]"
    return re.sub(
        r"bypassed_signature_events\s*=\s*\[.*?\]",
        new_block,
        content,
        flags=re.DOTALL,
    )


def extract_keys(content):
    """Extract config keys from a TOML file.

    Returns OrderedDict of key -> (value, line_number, is_commented).
    Handles both ``key = value`` and ``#key = value``.
    """
    keys = OrderedDict()
    for i, line in enumerate(content.splitlines(), 1):
        match = re.match(r"^(#?)([a-z][a-z0-9_]*)\s*=\s*(.*)", line)
        if match:
            commented = match.group(1) == "#"
            key = match.group(2)
            value = match.group(3).strip()
            if key not in keys:
                keys[key] = (value, i, commented)
    return keys


# ── Sub-routines for merge_configs ──────────────────────────────


def _merge_bypass_lists(content_a, content_b, file_a, file_b):
    """Union bypass lists and write back to both files."""
    ids_a = ordered_dedup(extract_event_ids(content_a))
    ids_b = ordered_dedup(extract_event_ids(content_b))

    only_a = [x for x in ids_a if x not in set(ids_b)]
    only_b = [x for x in ids_b if x not in set(ids_a)]
    merged = ordered_union(ids_a, ids_b)

    print("── bypassed_signature_events ──")
    print(f"  A: {len(ids_a)} unique | B: {len(ids_b)} unique | Union: {len(merged)}")
    for label, items in (("A", only_a), ("B", only_b)):
        if items:
            print(f"  Only in {label} ({len(items)}):")
            for eid in items:
                print(f"    + {eid}")

    if only_a or only_b:
        content_a = replace_event_ids(content_a, merged)
        content_b = replace_event_ids(content_b, merged)
        with open(file_a, "w", encoding="utf-8") as fobj:
            fobj.write(content_a)
        with open(file_b, "w", encoding="utf-8") as fobj:
            fobj.write(content_b)
        print(f"  ✓ Wrote merged list ({len(merged)} entries) to both.")
    else:
        print("  ✓ Already in sync.")
    print()
    return content_a, content_b


def _report_value_diffs(content_a, content_b):
    """Print active-key value differences between two configs."""
    keys_a = extract_keys(content_a)
    keys_b = extract_keys(content_b)
    active_a = {k: v for k, v in keys_a.items() if not v[2]}
    active_b = {k: v for k, v in keys_b.items() if not v[2]}

    print("── Value differences (active keys) ──")
    diff_count = 0
    for key in sorted(set(active_a) | set(active_b)):
        if key in INSTANCE_KEYS:
            continue
        in_a, in_b = key in active_a, key in active_b
        if in_a and in_b and active_a[key][0] != active_b[key][0]:
            diff_count += 1
            print(f"  {key}:")
            print(f"    A: {active_a[key][0]}")
            print(f"    B: {active_b[key][0]}")
        elif in_a and not in_b:
            diff_count += 1
            print(f"  {key}:")
            print(f"    A: {active_a[key][0]}")
            print("    B: (not set)")
        elif in_b and not in_a:
            diff_count += 1
            print(f"  {key}:")
            print("    A: (not set)")
            print(f"    B: {active_b[key][0]}")

    if diff_count == 0:
        print("  ✓ No differences (excluding instance-specific keys).")
    print()
    return keys_a, keys_b


def _report_missing_keys(keys_a, keys_b, example_file):
    """Check both configs against the example for missing keys."""
    # pylint: disable=too-many-locals
    with open(example_file, encoding="utf-8") as fobj:
        keys_ex = extract_keys(fobj.read())

    all_keys = set(keys_a) | set(keys_b)
    missing_both = [k for k in keys_ex if k not in all_keys]
    missing_a = [k for k in keys_ex if k not in keys_a]
    missing_b = [k for k in keys_ex if k not in keys_b]

    print("── Missing keys vs example config ──")
    print(f"  Example has {len(keys_ex)} keys total")
    print(f"  A has {len(keys_a)} keys | B has {len(keys_b)} keys")

    if missing_both:
        print(f"\n  Missing from BOTH ({len(missing_both)}):")
        for key in missing_both:
            val, line, commented = keys_ex[key]
            pfx = "#" if commented else ""
            print(f"    {pfx}{key} = {val}  (example L{line})")

    for label, missing, excl in (
        ("A", missing_a, missing_both),
        ("B", missing_b, missing_both),
    ):
        only = [k for k in missing if k not in excl]
        if only:
            print(f"\n  Missing from {label} only ({len(only)}):")
            for key in only:
                val, line, _ = keys_ex[key]
                print(f"    {key} = {val}  (example L{line})")

    if not missing_both and not missing_a and not missing_b:
        print("  ✓ Both configs have all example keys.")
    print()


# ── Entry points ────────────────────────────────────────────────


def merge_configs(file_a, file_b, example_file=None):
    """Run the full config merge pipeline."""
    with open(file_a, encoding="utf-8") as fobj:
        content_a = fobj.read()
    with open(file_b, encoding="utf-8") as fobj:
        content_b = fobj.read()

    print(f"{'=' * 60}")
    print("Config Merge Report")
    print(f"  A: {file_a}")
    print(f"  B: {file_b}")
    if example_file:
        print(f"  Example: {example_file}")
    print(f"{'=' * 60}\n")

    content_a, content_b = _merge_bypass_lists(content_a, content_b, file_a, file_b)
    keys_a, keys_b = _report_value_diffs(content_a, content_b)

    if example_file:
        _report_missing_keys(keys_a, keys_b, example_file)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Merge continuwuity TOML configs")
    parser.add_argument("config_a", help="First config file")
    parser.add_argument("config_b", help="Second config file")
    parser.add_argument(
        "--example",
        "-e",
        help="Example config to check for missing keys",
    )
    args = parser.parse_args()
    merge_configs(args.config_a, args.config_b, args.example)


if __name__ == "__main__":
    main()
