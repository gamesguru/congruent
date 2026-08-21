#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 19:20:48 2026

@author: shane
"""

import os
import re
import subprocess
import sys
import time


def sql_quote(value):
    """Escape a value for safe embedding inside a single-quoted SQL string literal."""
    return value.replace("'", "''")

args_str = " ".join(sys.argv[1:])

# Get the local machine's timezone offset (e.g. "-04:00") to send to Postgres over SSH
tz_raw = time.strftime("%z")
if tz_raw:
    # Postgres expects POSIX offsets where West of UTC is positive.
    # Python's %z is ISO 8601 (West is negative). So we invert the sign.
    sign = "+" if tz_raw[0] == "-" else "-"
    tz_sql = f"{sign}{tz_raw[1:3]}:{tz_raw[3:]}"
else:
    tz_sql = "+00:00"

# Defaults
like_str = "all"
limit = "15"
order = "run_date DESC, n_pass DESC"

# Extract like
like_match = re.search(r"like=([^\s]+)", args_str)
if like_match:
    like_str = like_match.group(1)
    args_str = args_str.replace(like_match.group(0), "")

# Extract limit
limit_match = re.search(r"limit=([0-9]+)", args_str)
if limit_match:
    limit = limit_match.group(1)
    args_str = args_str.replace(limit_match.group(0), "")

# Extract baseline (can be a branch, commit hash, or run ID)
baseline = None
baseline_match = re.search(r"baseline=([a-zA-Z0-9_\-\.]+)", args_str)
if baseline_match:
    baseline = baseline_match.group(1)
    args_str = args_str.replace(baseline_match.group(0), "")

# Extract new_passes (Must be done before order parsing to avoid greedy capture)
new_passes = False
new_passes_match = re.search(r"new_passes=([^\s]*)", args_str, re.IGNORECASE)
if new_passes_match:
    val = new_passes_match.group(1).strip().lower()
    if val in ("1", "true", "yes"):
        new_passes = True
    elif val in ("0", "false", "no", ""):
        new_passes = False
    args_str = args_str.replace(new_passes_match.group(0), "")

super_mode = bool(re.search(r"(^|\s)--super(\s|$)", args_str))
args_str = re.sub(r"(^|\s)--super(\s|$)", " ", args_str).strip()

# Extract order (it takes whatever is left if it starts with order=)
order_match = re.search(
    r"order=(.+?)(?:$| like=| limit=| new_passes=)", args_str + " ", re.IGNORECASE
)
if order_match:
    order = order_match.group(1).strip()

if new_passes:
    columns_tail = "new_failures_list,\n    new_passes_list"
else:
    columns_tail = "new_failures_list"

if super_mode:
    super_columns = "run_total,\n    detail_n_pass,\n    detail_n_fail,\n    detail_n_skip,\n    baseline_run_id,\n    baseline_n_pass,\n    baseline_n_fail,\n    baseline_n_skip,"
else:
    super_columns = ""

# baseline/like_str are real *values* from argv, so they're escaped with sql_quote() and
# embedded directly as SQL string literals below. Only *which fixed SQL fragment* to use is
# chosen here, never raw unescaped text.
if baseline:
    baseline_val = sql_quote(baseline)
    baseline_run_filter = (
        f"(b.commit_hash LIKE '{baseline_val}%' "
        f"OR b.version_string LIKE '%{baseline_val}%' "
        f"OR b.branch LIKE '%{baseline_val}%' "
        f"OR b.id::text = '{baseline_val}')"
    )
else:
    # Default to recent main/upstream. No user input involved, plain literal is fine.
    baseline_run_filter = "(b.branch IN ('main', 'main-upstream', 'refs/heads/main', 'refs/heads/main-upstream') OR b.version_string LIKE '%main%')"

if like_str == "all":
    like_filter = ""
else:
    like_filter = f"AND version_string LIKE '%{sql_quote(like_str)}%'"

# order becomes raw ORDER BY text (column identifiers/direction), which cannot be bound as a
# parameter -- SQL has no placeholder for identifiers. Allowlist it instead of escaping it.
_ORDER_COLUMNS = {
    "run_date",
    "commit_hash",
    "branch",
    "version_string",
    "arch",
    "os",
    "profile",
    "room_version",
    "n_pass",
    "n_skip",
    "n_fail",
    "id",
}
_ORDER_TOKEN_RE = re.compile(
    r"^\s*(?:{cols})(?:\s+(?:ASC|DESC))?(?:\s*,\s*(?:{cols})(?:\s+(?:ASC|DESC))?)*\s*$".format(
        cols="|".join(_ORDER_COLUMNS)
    ),
    re.IGNORECASE,
)
if not _ORDER_TOKEN_RE.match(order):
    print(f"⚠ Ignoring invalid order clause {order!r}; falling back to default.")
    order = "run_date DESC, n_pass DESC"

if not re.fullmatch(r"[0-9]+", limit):
    limit = "15"

sql_file_path = os.path.join(os.path.dirname(__file__), "queries.sql")
with open(sql_file_path, "r") as f:
    base_query_template = f.read()

query = base_query_template.format(
    baseline_run_filter=baseline_run_filter,
    tz_sql=tz_sql,
    columns_tail=columns_tail,
    order=order,
    limit=limit,
    like_filter=like_filter,
    super_columns=super_columns,
)

print(f"\nExecuting Query:\n{query}\n")

env = os.environ.copy()
env["PAGER"] = env.get("PAGER") or "less -X -F -S"

try:
    subprocess.run(["./bin/db-shell", "-c", query], env=env)
except KeyboardInterrupt:
    raise SystemExit(130)
finally:
    if sys.stdin.isatty():
        os.system("stty sane 2>/dev/null")
