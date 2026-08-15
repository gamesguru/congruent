/*
Created on Sat Apr 04 13:21:17 2026

@author: shane
 */
WITH baseline_commit AS (
    SELECT
        b.commit_hash
    FROM
        runs b
    WHERE
        {baseline_run_filter}
    ORDER BY
        b.run_date DESC
    LIMIT 1
),
baseline_runs AS (
    SELECT
        b2.id,
        b2.os,
        b2.arch,
        b2.profile,
        COALESCE(b2.room_version, '11') AS room_version,
        COALESCE(regexp_replace(btrim(b2.features, ' ,'), '[,\s]+', ' ', 'g'), '') AS features,
        b2.run_date,
        b2.n_pass,
        b2.n_fail,
        b2.n_skip
    FROM
        runs b2
    WHERE
        b2.commit_hash = (
            SELECT
                commit_hash
            FROM
                baseline_commit)
),
-- Union each test's status across every job (arch/os/room_version/profile/features) at
-- the baseline commit, so one config's baseline flake or gap can't hide a real
-- fix/regression seen anywhere else on that same commit.
baseline_status AS (
    SELECT
        rd.test_name,
        bool_or(rd.status = 'pass') AS any_pass,
        bool_or(rd.status = 'skip') AS any_skip
    FROM
        baseline_runs br
        JOIN run_details rd ON rd.run_id = br.id
    GROUP BY
        rd.test_name
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
run_regs AS (
    SELECT
        r.id,
        r.version_string,
        r.run_date,
        r.n_pass,
        r.n_fail,
        r.n_skip,
        r.profile,
        r.room_version,
        r.features,
        r.os,
        r.arch,
        counts.run_total,
        counts.detail_n_pass,
        counts.detail_n_skip,
        counts.detail_n_fail,
        counts.new_pass,
        counts.new_skip,
        counts.new_fail,
        counts.new_failures_list,
        counts.new_passes_list,
        baseline_ids.baseline_run_id,
        baseline_totals.baseline_n_pass,
        baseline_totals.baseline_n_fail,
        baseline_totals.baseline_n_skip
    FROM
        recent_runs r
        LEFT JOIN LATERAL (
            SELECT
                COUNT(*) FILTER (WHERE bs.any_pass) AS baseline_n_pass,
            COUNT(*) FILTER (WHERE NOT bs.any_pass
                    AND bs.any_skip) AS baseline_n_skip,
                COUNT(*) FILTER (WHERE NOT bs.any_pass
                    AND NOT bs.any_skip) AS baseline_n_fail
        FROM
            baseline_status bs) baseline_totals ON TRUE
    LEFT JOIN LATERAL (
        SELECT
            array_agg(b2.id) AS baseline_run_id
        FROM
            baseline_runs b2) baseline_ids ON TRUE
        LEFT JOIN LATERAL (
            SELECT
                COUNT(*) AS run_total,
                COUNT(*) FILTER (WHERE rd.status = 'pass') AS detail_n_pass,
                COUNT(*) FILTER (WHERE rd.status = 'skip') AS detail_n_skip,
                COUNT(*) FILTER (WHERE rd.status = 'fail') AS detail_n_fail,
                COUNT(*) FILTER (WHERE rd.status = 'pass'
                        AND eb.status IS NOT NULL
                        AND eb.status != 'pass') AS new_pass,
                    COUNT(*) FILTER (WHERE rd.status = 'fail'
                        AND eb.status IS NOT NULL
                        AND eb.status != 'fail') AS new_fail,
                    COUNT(*) FILTER (WHERE rd.status = 'skip'
                        AND eb.status IS NOT NULL
                        AND eb.status != 'skip') AS new_skip,
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
                    bs.test_name = rd.test_name) eb ON TRUE
            WHERE
                rd.run_id = r.id) counts ON TRUE
        WHERE
            counts.run_total > 0
)
SELECT
    id AS run_id,
    version_string,
    to_char(run_date AT TIME ZONE '{tz_sql}', 'YYYY-MM-DD HH24:MI:SS') AS run_date,
(n_pass + n_skip + n_fail) AS n_total,
n_pass,
n_skip,
n_fail,
new_pass,
new_fail,
{super_columns} profile,
room_version,
regexp_replace(btrim(features, ' ,'), '[,\s]+', ' ', 'g') AS features,
os,
arch,
{columns_tail}
FROM
    run_regs
