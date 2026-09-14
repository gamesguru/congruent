-- reset.sql
-- Destructive operations to completely reset the continuwuity CI PostgreSQL schema
DROP VIEW IF EXISTS v_run_regressions CASCADE;

DROP VIEW IF EXISTS mv_ever_passed CASCADE;

DROP MATERIALIZED VIEW IF EXISTS mv_ever_passed CASCADE;

DROP TABLE IF EXISTS run_details CASCADE;

DROP TABLE IF EXISTS runs CASCADE;

DROP TABLE IF EXISTS master_baseline CASCADE;

DROP TABLE IF EXISTS ever_passed CASCADE;
