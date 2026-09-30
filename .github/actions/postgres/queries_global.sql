/*
Global "ever-passed" regression query.
Uses ever_passed table (incremental UPSERT, no materialized view refresh).
A test is a regression if it fails now but has ever passed in any prior run.
Bulk JOIN approach: scales O(n) with limit, not O(n * tests).
 */
WITH recent_runs AS (
    SELECT
        r.*
    FROM
        runs r
    WHERE
        r.n_pass > 0 {like_filter}
    ORDER BY
        {order}
    LIMIT {limit}
),
run_agg AS (
    SELECT
        rd.run_id,
        COUNT(*) AS run_total,
        COUNT(*) FILTER (WHERE rd.status = 'pass'
            AND ep.test_name IS NULL) AS new_pass,
        COUNT(*) FILTER (WHERE rd.status = 'fail') AS new_fail,
        COUNT(*) FILTER (WHERE rd.status = 'skip') AS new_skip,
        STRING_AGG(rd.test_name, E'\n' ORDER BY rd.test_name) FILTER (WHERE rd.status = 'fail') AS failures_list,
        STRING_AGG(rd.test_name, E'\n' ORDER BY rd.test_name) FILTER (WHERE rd.status = 'pass'
            AND ep.test_name IS NULL) AS new_passes_list,
        STRING_AGG(COALESCE(ep.last_passed, 'never'), E'\n' ORDER BY rd.test_name) FILTER (WHERE rd.status = 'fail'
            AND ep.test_name IS NOT NULL) AS date_last_passed,
        STRING_AGG(COALESCE(ep.branches::text, '[]'), E'\n' ORDER BY rd.test_name) FILTER (WHERE rd.status = 'fail'
            AND ep.test_name IS NOT NULL) AS branches_passed_on
    FROM
        recent_runs r
        JOIN run_details rd ON rd.run_id = r.id
        LEFT JOIN LATERAL (
            SELECT
                ep2.test_name,
                ep2.last_passed,
                ep2.branches
            FROM
                mv_ever_passed ep2
            WHERE
                ep2.test_name = rd.test_name {branch_filter}
            ORDER BY
                ep2.last_passed DESC NULLS LAST
            LIMIT 1) ep ON TRUE
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
    {columns_tail}
FROM
    recent_runs r
    JOIN run_agg a ON a.run_id = r.id
WHERE
    a.run_total > 0
ORDER BY
    r.run_date DESC
