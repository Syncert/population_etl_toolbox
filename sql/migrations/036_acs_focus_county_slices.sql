-- Census ACS focus-county tract slices (focus-counties).
--
-- A focus county (`AcsConfig.focus_counties`) is asked for a few more tables
-- at tract grain in a slice of its own: `for=tract:*&in=state:SS county:CCC`.
-- Its ledger row has the same dataset, year, level and state as that state's
-- ordinary tract slice, so the ledger needs the county to tell them apart:
--
-- * `control.acs_ingestion_slices.county_fips` names the three-digit county
--   of a focus-county slice and is NULL on every other slice;
-- * the per-state unique index now includes it, so the two rows coexist.
--
-- Every statement is idempotent, and nothing already stored changes: every
-- existing row keeps a NULL county.

ALTER TABLE control.acs_ingestion_slices
    ADD COLUMN IF NOT EXISTS county_fips TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'control.acs_ingestion_slices'::regclass
          AND conname = 'acs_ingestion_slices_focus_county_check'
    ) THEN
        -- Only a tract slice names a county, and then by its three digits.
        ALTER TABLE control.acs_ingestion_slices
            ADD CONSTRAINT acs_ingestion_slices_focus_county_check
            CHECK (
                county_fips IS NULL
                OR (geo_level = 'tract' AND county_fips ~ '^[0-9]{3}$')
            );
    END IF;
END
$$;

DROP INDEX IF EXISTS control.acs_ingestion_slices_uniq_state;
CREATE UNIQUE INDEX IF NOT EXISTS acs_ingestion_slices_uniq_state_county
    ON control.acs_ingestion_slices (
        dataset, year, geo_level, state_fips, COALESCE(county_fips, '')
    )
    WHERE state_fips IS NOT NULL;
