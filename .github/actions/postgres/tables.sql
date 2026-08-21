-- tables.sql
-- Relational schema for Continuwuity CI runs.
-- Drop views to allow schema updates
DROP VIEW IF EXISTS v_run_regressions CASCADE;

-- Create runs table
CREATE TABLE IF NOT EXISTS runs (
    id serial PRIMARY KEY,
    run_date timestamp with time zone NOT NULL,
    commit_hash text NOT NULL,
    upstream_commit text,
    branch text,
    author_name text,
    actor text,
    provider text,
    arch text,
    os text,
    version_string text,
    features text,
    profile text,
    binary_sha256 text,
    n_pass integer DEFAULT 0,
    n_skip integer DEFAULT 0,
    n_fail integer DEFAULT 0,
    room_version text
);

-- Create run_details table
CREATE TABLE IF NOT EXISTS run_details (
    id serial PRIMARY KEY,
    run_id integer REFERENCES runs (id) ON DELETE CASCADE,
    test_name text NOT NULL,
    status text NOT NULL
);

-- Ensure we don't log the same test twice for the same run
CREATE UNIQUE INDEX IF NOT EXISTS idx_run_details_unique_test ON run_details (run_id, test_name);

-- Unique index to prevent duplicate machine reports.
-- Keep this in sync with ON CONFLICT targets in ingest scripts.
DROP INDEX IF EXISTS idx_runs_unique_machine_run;

UPDATE
    runs
SET
    arch = NULLIF (arch, ''),
    os = NULLIF (os, ''),
    profile = NULLIF (profile, ''),
    room_version = COALESCE(NULLIF (room_version, ''), '11'),
    features = COALESCE(regexp_replace(btrim(features, ' ,'), '[,\s]+', ' ', 'g'), '');

CREATE TEMP TABLE duplicated_runs AS
WITH normalized_runs AS (
    SELECT
        id AS run_id,
        commit_hash,
        NULLIF (arch, '') AS arch,
        NULLIF (os, '') AS os,
        NULLIF (profile, '') AS profile,
        COALESCE(NULLIF (room_version, ''), '11') AS room_version,
        COALESCE(regexp_replace(btrim(features, ' ,'), '[,\s]+', ' ', 'g'), '') AS features,
        COUNT(*) OVER GROUPING AS dup_count,
            ROW_NUMBER() OVER ordered_grouping AS rn,
                FIRST_VALUE(id) OVER ordered_grouping AS keep_id
                FROM
                    runs
WINDOW GROUPING AS (PARTITION BY commit_hash,
    NULLIF (arch, ''),
    NULLIF (os, ''),
    NULLIF (profile, ''),
    COALESCE(NULLIF (room_version, ''), '11'),
    COALESCE(regexp_replace(btrim(features, ' ,'), '[,\s]+', ' ', 'g'), '')),
ordered_grouping AS (PARTITION BY commit_hash,
    NULLIF (arch, ''),
    NULLIF (os, ''),
    NULLIF (profile, ''),
    COALESCE(NULLIF (room_version, ''), '11'),
    COALESCE(regexp_replace(btrim(features, ' ,'), '[,\s]+', ' ', 'g'), '')
ORDER BY
    run_date DESC,
    id DESC))
SELECT
    run_id,
    keep_id,
    rn
FROM
    normalized_runs
WHERE
    dup_count > 1;

CREATE TEMP TABLE duplicated_run_details AS
SELECT
    dr.keep_id,
    rd.test_name,
    rd.status,
    dr.rn
FROM
    duplicated_runs dr
    JOIN run_details rd ON rd.run_id = dr.run_id;

DELETE FROM run_details rd USING duplicated_runs dr
WHERE rd.run_id = dr.run_id;

INSERT INTO run_details (run_id, test_name, status)
SELECT DISTINCT ON (keep_id, test_name)
    keep_id,
    test_name,
    status
FROM
    duplicated_run_details
ORDER BY
    keep_id,
    test_name,
    rn;

UPDATE
    runs r
SET
    n_pass = counts.n_pass,
    n_skip = counts.n_skip,
    n_fail = counts.n_fail
FROM (
    SELECT
        keep.keep_id,
        COUNT(rd.test_name) FILTER (WHERE rd.status = 'pass')::integer AS n_pass,
        COUNT(rd.test_name) FILTER (WHERE rd.status = 'skip')::integer AS n_skip,
        COUNT(rd.test_name) FILTER (WHERE rd.status = 'fail')::integer AS n_fail
    FROM ( SELECT DISTINCT
            keep_id
        FROM
            duplicated_runs) keep
    LEFT JOIN run_details rd ON rd.run_id = keep.keep_id
GROUP BY
    keep.keep_id) counts
WHERE
    r.id = counts.keep_id;

DELETE FROM runs r USING duplicated_runs dr
WHERE r.id = dr.run_id
    AND dr.rn > 1;

CREATE UNIQUE INDEX idx_runs_unique_machine_run ON runs (commit_hash, arch, os, profile, room_version, features) NULLS NOT DISTINCT;

-- Remember the latest passing date per test/room_version across all ingests.
CREATE TABLE IF NOT EXISTS ever_passed (
    test_name text NOT NULL,
    rv text NOT NULL,
    last_passed text NOT NULL,
    last_commit text,
    last_branch text,
    branches text[] NOT NULL DEFAULT '{}'::text[]
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ever_passed_unique_test_rv ON ever_passed (test_name, rv);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_run_details_run_id ON run_details (run_id);

CREATE INDEX IF NOT EXISTS idx_runs_commit_hash ON runs (commit_hash);

-- Combine Regressions View: compares each individual run against the UNION of "best"
-- statuses (pass > skip > fail) seen across every arch's most recent 'main' run for the
-- same os. This stops one arch's baseline flake/lag from hiding a real fix or regression
-- that's genuinely new on another arch, while still letting a per-arch run be flagged as
-- a regression if it fails a test that passed somewhere on main.
CREATE OR REPLACE VIEW v_run_regressions AS
WITH baseline_selection AS (
    -- latest run per (arch, os) on main, so each arch contributes its own most recent result
    SELECT DISTINCT ON (arch,
        os)
        id,
        os
    FROM
        runs
    WHERE (branch IN ('main', 'main-upstream', 'refs/heads/main', 'refs/heads/main-upstream')
        OR version_string LIKE '%main%')
ORDER BY
    arch,
    os,
    run_date DESC,
    id DESC
),
baseline_status AS (
    SELECT
        bsel.os,
        rd.test_name,
        bool_or(rd.status = 'pass') AS any_pass,
        bool_or(rd.status = 'skip') AS any_skip
    FROM
        baseline_selection bsel
        JOIN run_details rd ON rd.run_id = bsel.id
    GROUP BY
        bsel.os,
        rd.test_name
)
SELECT
    r.id,
    r.version_string,
    r.run_date,
    r.commit_hash,
    r.upstream_commit AS upstream_sha,
    r.author_name,
    r.actor,
    r.branch,
    r.arch,
    r.os,
    r.room_version,
    r.features,
    r.profile,
    r.n_pass,
    r.n_fail,
    r.n_skip,
    baseline_totals.baseline_total,
    counts.run_total,
    (counts.run_total - baseline_totals.baseline_total) AS diff_total,
    -- Calculate deltas vs the unioned baseline
    counts.new_pass,
    counts.new_skip,
    counts.new_fail,
    counts.new_failures_list,
    counts.new_passes_list
FROM
    runs r
    LEFT JOIN LATERAL (
        SELECT
            COUNT(*) AS baseline_total
        FROM
            baseline_status bs
        WHERE
            bs.os IS NOT DISTINCT FROM r.os) baseline_totals ON TRUE
    LEFT JOIN LATERAL (
        SELECT
            COUNT(*) AS run_total,
            COUNT(*) FILTER (WHERE rd.status = 'pass'
                    AND eb.status IS NOT NULL
                    AND eb.status != 'pass') AS new_pass,
                COUNT(*) FILTER (WHERE rd.status = 'skip'
                    AND eb.status IS NOT NULL
                    AND eb.status != 'skip') AS new_skip,
                COUNT(*) FILTER (WHERE rd.status = 'fail'
                    AND eb.status IS NOT NULL
                    AND eb.status != 'fail') AS new_fail,
                STRING_AGG(rd.test_name, E'\n' ORDER BY rd.test_name) FILTER (WHERE rd.status = 'fail'
                    AND eb.status IS NOT NULL
                    AND eb.status != 'fail') AS new_failures_list,
                STRING_AGG(rd.test_name, E'\n' ORDER BY rd.test_name) FILTER (WHERE rd.status = 'pass'
                    AND eb.status IS NOT NULL
                    AND eb.status != 'pass') AS new_passes_list
            FROM
                run_details rd
        LEFT JOIN LATERAL (
            SELECT
                CASE WHEN bs.any_pass THEN
                    'pass'
                WHEN bs.any_skip THEN
                    'skip'
                ELSE
                    'fail'
                END AS status
            FROM
                baseline_status bs
            WHERE
                bs.os IS NOT DISTINCT FROM r.os
                AND bs.test_name = rd.test_name) eb ON TRUE
        WHERE
            rd.run_id = r.id) counts ON TRUE
WHERE
    r.n_pass > 0
    AND counts.run_total > 0;

-- Ensure read-only users can query the view and raw tables even after they get recreated by CI
GRANT SELECT ON ALL TABLES IN SCHEMA public TO public;
