#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 23 22:46:39 2026

@author: shane
"""

import json
import re
import tomllib

with open("conduwuit.toml", "rb") as config_file:
    user_config = tomllib.load(config_file)

with open("conduwuit-example.toml", "r", encoding="utf-8") as example_file:
    example_lines = example_file.readlines()


def format_value(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    elif isinstance(v, str):
        return json.dumps(v, ensure_ascii=False)
    elif isinstance(v, list):
        if len(v) == 0:
            return "[]"
        # Keep arrays readable, including arrays of inline tables.
        contents = ",\n".join(f"    {format_value(item)}" for item in v)
        return f"[\n{contents},\n]"
    elif isinstance(v, (int, float)):
        return str(v)
    elif isinstance(v, dict):
        contents = ", ".join(
            f"{format_key(key)} = {format_value(value)}" for key, value in v.items()
        )
        return f"{{ {contents} }}"
    else:
        return str(v)


def format_key(key):
    """Format a TOML key, quoting it when it is not a bare key."""
    if re.fullmatch(r"[A-Za-z0-9_-]+", key):
        return key
    return json.dumps(key, ensure_ascii=False)


merged_lines = []
current_sect = "global"
emitted_keys = set()

# Apply MSC overwrites explicitly
if "global" not in user_config:
    user_config["global"] = {}
if not isinstance(user_config["global"], dict):
    user_config["global"] = {}
if "experimental_features" not in user_config["global"]:
    user_config["global"]["experimental_features"] = {}
user_config["global"]["experimental_features"]["msc3266_enabled"] = True
user_config["global"]["experimental_features"]["msc4222_enabled"] = True

i = 0
in_code_fence = False
while i < len(example_lines):
    line = example_lines[i]

    # Do not interpret TOML examples inside documentation fences as config.
    if re.match(r"^\s*#?\s*```", line):
        in_code_fence = not in_code_fence
        merged_lines.append(line)
        i += 1
        continue
    if in_code_fence:
        merged_lines.append(line)
        i += 1
        continue

    # Detect Section
    sect_match = re.match(r"^#?\[([a-zA-Z0-9_\.]+)\]", line)
    if sect_match:
        sect = sect_match.group(1)
        # Check if the user has keys in this section
        parts = sect.split(".")
        curr = user_config
        has_keys = True
        for p in parts:
            if isinstance(curr, dict) and p in curr:
                curr = curr[p]
            else:
                has_keys = False
                break

        if hasattr(curr, "keys") and not curr:
            has_keys = False

        if has_keys:
            merged_lines.append(f"[{sect}]\n")
        else:
            merged_lines.append(line)
        current_sect = sect
        i += 1
        continue

    # Detect Keys
    kv_match = re.match(r"^#?\s*([a-zA-Z0-9_-]+)\s*=", line)
    if kv_match:
        key = kv_match.group(1)

        # check if user explicitly set this key
        parts = current_sect.split(".")
        curr = user_config
        has_val = True
        for p in parts:
            if isinstance(curr, dict) and p in curr:
                curr = curr[p]
            else:
                has_val = False
                break

        val = None
        if has_val and isinstance(curr, dict) and key in curr:
            val = curr[key]
        else:
            has_val = False

        section_key = (current_sect, key)
        if has_val and section_key not in emitted_keys:
            merged_lines.append(f"{key} = {format_value(val)}\n")
            emitted_keys.add(section_key)
        else:
            merged_lines.append(line)
        i += 1
        continue

    merged_lines.append(line)
    i += 1

with open("conduwuit.toml", "w", encoding="utf-8") as output_file:
    output_file.writelines(merged_lines)

print("Merge completed successfully!")
