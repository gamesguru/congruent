/*
Created on Sat Apr 04 13:21:17 2026

@author: shane

Single-commit baseline comparison.
Bulk JOIN approach: scales O(n) with limit.
 */
WITH baseline_commit AS (
    SELECT
        b.commit_hash
    FROM
        runs b
    WHERE
        {baseline_run_filter}
    ORDER BY
        b.run_date DESC,
        b.id DESC
    LIMIT 1
),
baseline_runs AS (
    SELECT
        b2.id,
        b2.os,
        b2.arch,
        b2.profile,
        COALESCE(b2.room_version, '11') AS room_version
    FROM
        runs b2
    WHERE
        b2.commit_hash = (
            SELECT
                commit_hash
            FROM
                baseline_commit)
),
baseline_details AS (
    SELECT
        rd.test_name,
        rd.status,
        b.id AS baseline_run_id
    FROM
        baseline_runs b
        JOIN run_details rd ON rd.run_id = b.id
),
recent_runs AS (
    SELECT
        r.*
    FROM
        runs r
    WHERE
        r.n_pass > 0
        AND EXISTS (
            SELECT
                1
            FROM
                run_details rd
            WHERE
                rd.run_id = r.id) {like_filter}
            ORDER BY
                {order}
            LIMIT {limit}
),
matched_baselines AS (
    SELECT DISTINCT ON (r.id)
        r.id AS run_id,
        b2.id AS baseline_run_id
    FROM
        recent_runs r
        LEFT JOIN baseline_runs b2 ON b2.os IS NOT DISTINCT FROM r.os
            AND b2.arch IS NOT DISTINCT FROM r.arch
            AND b2.profile IS NOT DISTINCT FROM r.profile
            AND b2.room_version IS NOT DISTINCT FROM COALESCE(r.room_version, '11')
        LEFT JOIN LATERAL (
            SELECT
                COUNT(*) AS cnt
            FROM
                run_details rd
            WHERE
                rd.run_id = b2.id) bd_count ON TRUE
        ORDER BY
            r.id,
            bd_count.cnt DESC NULLS LAST
),
run_agg AS (
    SELECT
        rd.run_id,
        COUNT(*) AS run_total,
        COUNT(*) FILTER (WHERE rd.status = 'pass'
            AND (mb.baseline_run_id IS NOT NULL
            AND (bd.status IS NULL
            OR bd.status != 'pass'))) AS new_pass,
COUNT(*) FILTER (WHERE rd.status = 'fail') AS new_fail,
COUNT(*) FILTER (WHERE rd.status = 'skip'
    AND (mb.baseline_run_id IS NOT NULL
    AND (bd.status IS NULL
    OR bd.status != 'skip'))) AS new_skip,
STRING_AGG(rd.test_name, E'\n' ORDER BY rd.test_name) FILTER (WHERE rd.status = 'fail') AS failures_list,
STRING_AGG(rd.test_name, E'\n' ORDER BY rd.test_name) FILTER (WHERE rd.status = 'pass'
    AND (mb.baseline_run_id IS NOT NULL
    AND (bd.status IS NULL
    OR bd.status != 'pass'))) AS new_passes_list
FROM
    run_details rd
    JOIN matched_baselines mb ON mb.run_id = rd.run_id
        LEFT JOIN baseline_details bd ON bd.test_name = rd.test_name
            AND bd.baseline_run_id = mb.baseline_run_id
    GROUP BY
        rd.run_id
)
SELECT
    r.id AS run_id,
    r.version_string,
    to_char(r.run_date AT TIME ZONE '{tz_sql}', 'YYYY-MM-DD HH24:MI:SS') AS run_date,
    (r.n_pass + r.n_skip + r.n_fail) AS n_total,
    r.n_pass,
    r.n_skip,
    r.n_fail,
    a.new_pass,
    a.new_fail,
    r.profile,
    r.room_version,
    regexp_replace(btrim(r.features, ' ,'), '[,\s]+', ' ', 'g') AS features,
    r.os,
    r.arch,
    -- r.github_run_id,
    {columns_tail}
FROM
    recent_runs r
    JOIN run_agg a ON a.run_id = r.id
WHERE
    a.run_total > 0
ORDER BY
    r.run_date DESC
