-- Migration 033: BLS facts are unique by period code, not by period date.
--
-- BLS's annual average of a monthly series (period `M13`, requested with
-- `annualaverage=true`) ends on December 31 like `M12`. Keyed by
-- (series_id, period_date) the two collided and one overwrote the other, so
-- the provider's own annual average could not be kept (ADR-0007, RU-3).
-- This swaps the constraint on a populated warehouse; a fresh one gets it from
-- `src/data_ingestion_toolbox/bls/DDL/silver_bls.sql`. Safe to rerun: the swap
-- runs only while the old definition is in place.

DO $$
DECLARE
    current_definition TEXT;
BEGIN
    SELECT pg_get_constraintdef(constraint_row.oid)
      INTO current_definition
      FROM pg_constraint AS constraint_row
     WHERE constraint_row.conname = 'fact_labor_stats_uk'
       AND constraint_row.conrelid = 'silver_bls.fact_labor_statistics'::REGCLASS;

    IF current_definition IS NULL THEN
        ALTER TABLE silver_bls.fact_labor_statistics
            ADD CONSTRAINT fact_labor_stats_uk UNIQUE (series_id, year, period);
    ELSIF current_definition <> 'UNIQUE (series_id, year, period)' THEN
        ALTER TABLE silver_bls.fact_labor_statistics DROP CONSTRAINT fact_labor_stats_uk;
        ALTER TABLE silver_bls.fact_labor_statistics
            ADD CONSTRAINT fact_labor_stats_uk UNIQUE (series_id, year, period);
    END IF;
END
$$;
