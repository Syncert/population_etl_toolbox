-- The two largest sources' facts gain capture lineage and a value status.
--
-- Five of seven silver facts could already name the response they were parsed
-- from and say why a number was absent. ACS and BLS could do neither: their
-- revision relations distinguish absent/blank/sentinel/invalid (ACS) and
-- valid/missing/invalid (BLS), and the fact aggregation kept only the number.
-- So a cell Census suppressed, or an observation BLS footnoted as
-- unavailable, reached serving as no row at all -- and a consumer cannot tell
-- a value the provider withheld from one it never published.
--
-- The column definitions themselves live in the phase files the bootstrap
-- applies and the DAGs re-apply
-- (`src/data_ingestion_toolbox/census_acs/DDL/silver_census.sql` and
-- `.../bls/DDL/silver_bls.sql`); this step is what a populated warehouse
-- needs and a rerunnable `CREATE TABLE IF NOT EXISTS` cannot do.
--
-- Rerun-safe: every ADD COLUMN is `IF NOT EXISTS`, every constraint is
-- dropped before it is added, and the two rewrites are idempotent because
-- they select on the state they leave.

-- ---------------------------------------------------------------------------
-- Census ACS
-- ---------------------------------------------------------------------------

ALTER TABLE silver_census.fact_demographics
    ADD COLUMN IF NOT EXISTS source_value TEXT,
    ADD COLUMN IF NOT EXISTS value_status TEXT NOT NULL DEFAULT 'valid',
    ADD COLUMN IF NOT EXISTS capture_id UUID;

-- Existing rows were written before the status existed, and the column's
-- default claims `valid` for all of them. That is false for exactly the rows
-- with no estimate: they are the suppressed and unparsable cells the fact
-- kept as a NULL number. `absent` is the ACS revision's own word for a cell
-- the provider published nothing in, and it is the honest reading available
-- from the fact alone -- the revision distinguishes blank from sentinel from
-- invalid, and this step deliberately does not guess which.
UPDATE silver_census.fact_demographics
   SET value_status = 'absent'
 WHERE estimate_value IS NULL
   AND value_status = 'valid';

ALTER TABLE silver_census.fact_demographics
    DROP CONSTRAINT IF EXISTS fact_demographics_value_status_check;
ALTER TABLE silver_census.fact_demographics
    ADD CONSTRAINT fact_demographics_value_status_check
    CHECK (value_status IN ('valid', 'absent', 'blank', 'sentinel', 'invalid'));

ALTER TABLE silver_census.fact_demographics
    DROP CONSTRAINT IF EXISTS fact_demographics_published_value_check;
ALTER TABLE silver_census.fact_demographics
    ADD CONSTRAINT fact_demographics_published_value_check
    CHECK (value_status <> 'valid' OR estimate_value IS NOT NULL);

ALTER TABLE silver_census.fact_demographics
    DROP CONSTRAINT IF EXISTS fact_demographics_capture_id_fkey;
ALTER TABLE silver_census.fact_demographics
    ADD CONSTRAINT fact_demographics_capture_id_fkey
    FOREIGN KEY (capture_id) REFERENCES raw_capture.response_capture(capture_id);

-- ---------------------------------------------------------------------------
-- BLS
-- ---------------------------------------------------------------------------

ALTER TABLE silver_bls.fact_labor_statistics
    ADD COLUMN IF NOT EXISTS source_value TEXT,
    ADD COLUMN IF NOT EXISTS value_status TEXT NOT NULL DEFAULT 'valid',
    ADD COLUMN IF NOT EXISTS capture_id UUID;

UPDATE silver_bls.fact_labor_statistics
   SET value_status = 'missing'
 WHERE value IS NULL
   AND value_status = 'valid';

ALTER TABLE silver_bls.fact_labor_statistics
    DROP CONSTRAINT IF EXISTS fact_labor_statistics_value_status_check;
ALTER TABLE silver_bls.fact_labor_statistics
    ADD CONSTRAINT fact_labor_statistics_value_status_check
    CHECK (value_status IN ('valid', 'missing', 'invalid'));

ALTER TABLE silver_bls.fact_labor_statistics
    DROP CONSTRAINT IF EXISTS fact_labor_statistics_published_value_check;
ALTER TABLE silver_bls.fact_labor_statistics
    ADD CONSTRAINT fact_labor_statistics_published_value_check
    CHECK (value_status <> 'valid' OR value IS NOT NULL);

ALTER TABLE silver_bls.fact_labor_statistics
    DROP CONSTRAINT IF EXISTS fact_labor_statistics_capture_id_fkey;
ALTER TABLE silver_bls.fact_labor_statistics
    ADD CONSTRAINT fact_labor_statistics_capture_id_fkey
    FOREIGN KEY (capture_id) REFERENCES raw_capture.response_capture(capture_id);

-- ---------------------------------------------------------------------------
-- FRED: the constraint its fact never had
-- ---------------------------------------------------------------------------
--
-- `silver_fred.fact_economic_indicators` has carried `value_status` since the
-- ARC-007 cutover, with no constraint tying it to the number and a DEFAULT of
-- `valid` -- so a writer that set no status at all produced a row asserting a
-- published value it did not have. The revision relation beside it has had
-- the constraint all along. This is the same defect the two facts above are
-- getting fixed, one source over, and it is not a change to FRED's row shape:
-- no column is added, removed, or retyped.

UPDATE silver_fred.fact_economic_indicators
   SET value_status = 'missing'
 WHERE value IS NULL
   AND value_status = 'valid';

ALTER TABLE silver_fred.fact_economic_indicators
    DROP CONSTRAINT IF EXISTS fact_economic_indicators_published_value_check;
ALTER TABLE silver_fred.fact_economic_indicators
    ADD CONSTRAINT fact_economic_indicators_published_value_check
    CHECK (value_status <> 'valid' OR value IS NOT NULL);

COMMENT ON COLUMN silver_census.fact_demographics.value_status IS
    'Why a value is absent, in the vocabulary silver_census.observation_revision '
    'uses. A row may only claim ''valid'' while carrying an estimate.';
COMMENT ON COLUMN silver_bls.fact_labor_statistics.value_status IS
    'Why a value is absent, in the vocabulary silver_bls.observation_revision '
    'uses. A row may only claim ''valid'' while carrying a value.';
COMMENT ON COLUMN silver_census.fact_demographics.capture_id IS
    'The response this row was parsed from. NULL for rows written before '
    'migration 027, whose capture was never recorded.';
COMMENT ON COLUMN silver_bls.fact_labor_statistics.capture_id IS
    'The response this row was parsed from. NULL for rows written before '
    'migration 027, whose capture was never recorded.';
