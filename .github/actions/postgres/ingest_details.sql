-- ingest_details.sql
-- Run against a temp table `t (j jsonb)` already loaded (via \copy) with test-result
-- rows tagged with commit/arch/os/profile/room_version/features. Upserts run_details
-- and ever_passed for exactly the run rows those lines belong to. Called from
-- sync_recent.sh's ingest_details() for both the bulk and direct ingest paths.
SELECT
    pg_advisory_lock(42);

CREATE TABLE IF NOT EXISTS ever_passed (
    test_name text NOT NULL,
    rv text NOT NULL,
    last_passed text NOT NULL,
    last_commit text,
    last_branch text,
    branches text[] NOT NULL DEFAULT '{}'::text[]
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ever_passed_unique_test_rv ON ever_passed (test_name, rv);

-- Map the distinct run configurations in the temp table to actual run IDs
CREATE TEMP TABLE newly_ingested_runs AS SELECT DISTINCT
    r.id AS run_id
FROM ( SELECT DISTINCT
        (j ->> 'commit') AS commit_hash,
        (NULLIF ((j ->> 'arch'), '')) AS arch,
        (NULLIF ((j ->> 'os'), '')) AS os,
        (NULLIF ((j ->> 'profile'), '')) AS profile,
        (COALESCE(NULLIF ((j ->> 'room_version'), ''), '11')) AS room_version,
        (NULLIF ((j ->> 'features'), '')) AS features
    FROM
        t) nt
    JOIN runs r ON r.commit_hash = nt.commit_hash
        AND NULLIF (r.arch, '') IS NOT DISTINCT FROM nt.arch
        AND NULLIF (r.os, '') IS NOT DISTINCT FROM nt.os
        AND NULLIF (r.profile, '') IS NOT DISTINCT FROM nt.profile
        AND COALESCE(NULLIF (r.room_version, ''), '11') IS NOT DISTINCT FROM nt.room_version
        AND COALESCE(regexp_replace(btrim(r.features, ' ,'), '[,\s]+', ' ', 'g'), '') IS NOT DISTINCT FROM COALESCE(regexp_replace(btrim(nt.features, ' ,'), '[,\s]+', ' ', 'g'), '');

INSERT INTO run_details (run_id, test_name, status)
SELECT DISTINCT ON (r.id, (t.j ->> 'Test')
) r.id,
(t.j ->> 'Test'),
(t.j ->> 'Action')
FROM
    t
    JOIN runs r ON r.commit_hash = (t.j ->> 'commit')
        AND NULLIF (r.arch, '') IS NOT DISTINCT FROM (NULLIF ((t.j ->> 'arch'), ''))
    AND NULLIF (r.os, '') IS NOT DISTINCT FROM (NULLIF ((t.j ->> 'os'), ''))
AND NULLIF (r.profile, '') IS NOT DISTINCT FROM (NULLIF ((t.j ->> 'profile'), ''))
AND COALESCE(NULLIF (r.room_version, ''), '11') IS NOT DISTINCT FROM (COALESCE(NULLIF ((t.j ->> 'room_version'), ''), '11'))
AND COALESCE(regexp_replace(btrim(r.features, ' ,'), '[,\s]+', ' ', 'g'), '') IS NOT DISTINCT FROM COALESCE(regexp_replace(btrim((t.j ->> 'features'), ' ,'), '[,\s]+', ' ', 'g'), '')
WHERE (t.j ->> 'Action') IN ('pass', 'fail', 'skip')
AND r.id IN (
    SELECT
        run_id
    FROM
        newly_ingested_runs)
ON CONFLICT (run_id,
    test_name)
    DO UPDATE SET
        status = EXCLUDED.status;

-- Incremental ever_passed: scoped to only the newly ingested runs
INSERT INTO ever_passed (test_name, rv, last_passed, last_commit, last_branch, branches)
SELECT
    rd.test_name,
    COALESCE(r.room_version, '11'),
    MAX(r.run_date)::date::text,
    (ARRAY_AGG(r.commit_hash ORDER BY r.run_date DESC))[1],
    (ARRAY_AGG(r.branch ORDER BY r.run_date DESC))[1],
    COALESCE(ARRAY_AGG(DISTINCT r.branch) FILTER (WHERE r.branch IS NOT NULL), ARRAY[]::text[])
FROM
    run_details rd
    JOIN runs r ON rd.run_id = r.id
WHERE
    rd.status = 'pass'
    AND r.id IN (
        SELECT
            run_id
        FROM
            newly_ingested_runs)
GROUP BY
    rd.test_name,
    COALESCE(r.room_version, '11')
ON CONFLICT (test_name,
    rv)
    DO UPDATE SET
        last_passed = GREATEST (ever_passed.last_passed, EXCLUDED.last_passed),
        last_commit = CASE WHEN EXCLUDED.last_passed > COALESCE(ever_passed.last_passed, '') THEN
            EXCLUDED.last_commit
        ELSE
            ever_passed.last_commit
        END,
        last_branch = CASE WHEN EXCLUDED.last_passed > COALESCE(ever_passed.last_passed, '') THEN
            EXCLUDED.last_branch
        ELSE
            ever_passed.last_branch
        END,
        branches = (
            SELECT
                ARRAY_AGG(DISTINCT b ORDER BY b)
            FROM
                UNNEST(COALESCE(ever_passed.branches, ARRAY[]::text[]) || COALESCE(EXCLUDED.branches, ARRAY[]::text[])) AS b
            WHERE
                b IS NOT NULL);

SELECT
    pg_advisory_unlock(42);
