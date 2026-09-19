-- The ACS and BLS serving relations admit a value the provider withheld.
--
-- `027_acs_bls_fact_lineage.sql` gave the two silver facts `capture_id`,
-- `source_value` and `value_status`. The serving layer still dropped those
-- rows: `gold_census.fact_acs_observation` filtered `estimate_value IS NOT
-- NULL` and `gold_bls.fact_bls_observation` filtered `value IS NOT NULL`, and
-- both reporting tables declared their value column `NOT NULL`, so there was
-- nowhere to put a withheld cell even if the filter had allowed it.
--
-- Measured on the internal stack: 31,481,530 of 99,783,997 ACS fact rows carry
-- no estimate. Every one of them was indistinguishable, to a consumer, from a
-- geography that does not exist.
--
-- The column definitions live in the phase files the bootstrap applies and the
-- DAGs re-apply; this step is what a populated warehouse needs and a rerunnable
-- `CREATE TABLE IF NOT EXISTS` cannot do.
--
-- Rerun-safe: every ADD COLUMN is `IF NOT EXISTS`, `DROP NOT NULL` is
-- idempotent, and each constraint is dropped before it is added.
--
-- Note on ordering: the views are `CREATE OR REPLACE`d by the phase files,
-- which may only *append* columns. This step does not restate them.

-- ---------------------------------------------------------------------------
-- Census ACS
-- ---------------------------------------------------------------------------

ALTER TABLE gold_census.rpt_acs_observations
    ALTER COLUMN value DROP NOT NULL,
    ALTER COLUMN estimate_value DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS value_status TEXT NOT NULL DEFAULT 'valid',
    ADD COLUMN IF NOT EXISTS source_value TEXT,
    ADD COLUMN IF NOT EXISTS capture_id UUID;

ALTER TABLE gold_census.mv_acs_latest
    ALTER COLUMN value DROP NOT NULL,
    ALTER COLUMN estimate_value DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS value_status TEXT NOT NULL DEFAULT 'valid',
    ADD COLUMN IF NOT EXISTS source_value TEXT,
    ADD COLUMN IF NOT EXISTS capture_id UUID;

-- Every row already served carries an estimate -- that was the filter's whole
-- effect -- so the default is true of all of them and the constraint validates
-- without a rewrite. The withheld rows arrive with the re-serve that follows
-- this step, carrying their own status.
ALTER TABLE gold_census.rpt_acs_observations
    DROP CONSTRAINT IF EXISTS rpt_acs_observations_value_status_check;
ALTER TABLE gold_census.rpt_acs_observations
    ADD CONSTRAINT rpt_acs_observations_value_status_check
    CHECK (value_status IN ('valid', 'absent', 'blank', 'sentinel', 'invalid'));

ALTER TABLE gold_census.rpt_acs_observations
    DROP CONSTRAINT IF EXISTS rpt_acs_observations_published_value_check;
ALTER TABLE gold_census.rpt_acs_observations
    ADD CONSTRAINT rpt_acs_observations_published_value_check
    CHECK (value_status <> 'valid' OR estimate_value IS NOT NULL);

-- ---------------------------------------------------------------------------
-- BLS
-- ---------------------------------------------------------------------------

ALTER TABLE gold_bls.rpt_bls_observations
    ALTER COLUMN value DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS value_status TEXT NOT NULL DEFAULT 'valid',
    ADD COLUMN IF NOT EXISTS source_value TEXT,
    ADD COLUMN IF NOT EXISTS capture_id UUID;

ALTER TABLE gold_bls.mv_bls_latest
    ALTER COLUMN value DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS value_status TEXT NOT NULL DEFAULT 'valid',
    ADD COLUMN IF NOT EXISTS source_value TEXT,
    ADD COLUMN IF NOT EXISTS capture_id UUID;

ALTER TABLE gold_bls.rpt_bls_observations
    DROP CONSTRAINT IF EXISTS rpt_bls_observations_value_status_check;
ALTER TABLE gold_bls.rpt_bls_observations
    ADD CONSTRAINT rpt_bls_observations_value_status_check
    CHECK (value_status IN ('valid', 'missing', 'invalid'));

ALTER TABLE gold_bls.rpt_bls_observations
    DROP CONSTRAINT IF EXISTS rpt_bls_observations_published_value_check;
ALTER TABLE gold_bls.rpt_bls_observations
    ADD CONSTRAINT rpt_bls_observations_published_value_check
    CHECK (value_status <> 'valid' OR value IS NOT NULL);

COMMENT ON COLUMN gold_census.rpt_acs_observations.value_status IS
    'Why a served value is absent, in the vocabulary '
    'silver_census.observation_revision uses. A row may only claim ''valid'' '
    'while carrying an estimate.';
COMMENT ON COLUMN gold_bls.rpt_bls_observations.value_status IS
    'Why a served value is absent, in the vocabulary '
    'silver_bls.observation_revision uses. A row may only claim ''valid'' '
    'while carrying a value.';
