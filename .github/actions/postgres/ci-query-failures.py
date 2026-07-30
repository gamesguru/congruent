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

# Extract --super flag (superscore: best-ever per test across all branches)
# --verbose is kept as backward-compat alias
super_mode = (
    "--super" in args_str
    or "-s" in args_str.split()
    or "--verbose" in args_str
    or "-v" in args_str.split()
)
for flag in ("--super", "--verbose"):
    args_str = args_str.replace(flag, "")
args_str = re.sub(r"\s-[sv]\s", " ", f" {args_str} ").strip()

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

# Extract order (it takes whatever is left if it starts with order=)
order_match = re.search(
    r"order=(.+?)(?:$| like=| limit=| new_passes=)", args_str + " ", re.IGNORECASE
)
if order_match:
    order = order_match.group(1).strip()

# Build columns_tail based on flags
cols = ["failures_list"]
if super_mode:
    cols.extend(["date_last_passed", "branches_passed_on"])
if new_passes:
    cols.append("new_passes_list")

columns_tail = ",\n    ".join(f"a.{c}" for c in cols)

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

# Pick query template:
#   --super:             "superscore" — global ever-passed across all branches
#   --super baseline=X:  superscore filtered to a specific branch
#   baseline=X:          direct diff against a specific commit/branch run
#   (default):           auto-baseline against the most recent matching run
script_dir = os.path.dirname(__file__)
if super_mode and baseline:
    # Superscore + baseline: filter to regressions that previously passed on a specific branch
    branch_filter = f"AND ep2.branches::text LIKE '%{baseline}%'"
    sql_file_path = os.path.join(script_dir, "queries_global.sql")
    with open(sql_file_path, "r", encoding="utf-8") as f:
        base_query_template = f.read()
    query = base_query_template.format(
        tz_sql=tz_sql,
        columns_tail=columns_tail,
        order=order,
        limit=limit,
        like_filter=like_filter,
        branch_filter=branch_filter,
    )
elif super_mode:
    # Superscore: global ever-passed across all branches ("what's the best I've ever done?")
    sql_file_path = os.path.join(script_dir, "queries_global.sql")
    with open(sql_file_path, "r", encoding="utf-8") as f:
        base_query_template = f.read()
    query = base_query_template.format(
        tz_sql=tz_sql,
        columns_tail=columns_tail,
        order=order,
        limit=limit,
        like_filter=like_filter,
        branch_filter="",
    )
elif baseline:
    # Direct diff: compare against a specific commit/branch run
    baseline_run_filter = (
        f"(b.commit_hash LIKE '{baseline}%'"
        f" OR b.version_string LIKE '%{baseline}%'"
        f" OR b.branch LIKE '%{baseline}%'"
        f" OR b.id::text = '{baseline}')"
    )
    sql_file_path = os.path.join(script_dir, "queries.sql")
    with open(sql_file_path, "r", encoding="utf-8") as f:
        base_query_template = f.read()
    query = base_query_template.format(
        baseline_run_filter=baseline_run_filter,
        tz_sql=tz_sql,
        columns_tail=columns_tail,
        order=order,
        limit=limit,
        like_filter=like_filter,
    )
else:
    # Default: auto-baseline against the most recent matching run
    auto_baseline = like_str if like_str != "all" else "dev"
    baseline_run_filter = (
        f"(b.commit_hash LIKE '{auto_baseline}%'"
        f" OR b.version_string LIKE '%{auto_baseline}%'"
        f" OR b.branch LIKE '%{auto_baseline}%'"
        f" OR b.id::text = '{auto_baseline}')"
    )
    sql_file_path = os.path.join(script_dir, "queries.sql")
    with open(sql_file_path, "r", encoding="utf-8") as f:
        base_query_template = f.read()
    query = base_query_template.format(
        baseline_run_filter=baseline_run_filter,
        tz_sql=tz_sql,
        columns_tail=columns_tail,
        order=order,
        limit=limit,
        like_filter=like_filter,
    )

print(f"\nExecuting Query:\n{query}\n")

env = os.environ.copy()
env["PAGER"] = env.get("PAGER") or "less -X -F -S"

try:
    subprocess.run(["./bin/db-shell", "-c", query], env=env, check=False)
except KeyboardInterrupt as exc:
    raise SystemExit(130) from exc
finally:
    if sys.stdin.isatty():
        os.system("stty sane 2>/dev/null")
