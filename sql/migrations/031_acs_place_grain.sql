-- Census ACS at place grain (acs-place-grain).
--
-- The ACS adapter now requests `for=place:*&in=state:<fips>` beside its
-- nation, state and county slices. Two relations a populated warehouse
-- already holds refuse the new level by CHECK, and the revision relation has
-- no column for the place code:
--
-- * `control.acs_ingestion_slices` (created by 001) admits `us`, `state` and
--   `county`, and requires a state only for a county slice;
-- * `silver_census.observation_revision` (created by 005 and by
--   `census_acs/DDL/silver_census.sql`) admits the same three levels.
--
-- The phase file now declares the widened revision relation for a fresh
-- warehouse; this step carries what a rerunnable `CREATE TABLE IF NOT EXISTS`
-- cannot: the column and the constraint swap on tables that already exist.
-- Every statement is idempotent, so a fresh bootstrap that already has the
-- widened table passes through unchanged. Nothing already stored changes:
-- every existing row is one of the three levels it was before.

ALTER TABLE silver_census.observation_revision
    ADD COLUMN IF NOT EXISTS place_fips_source TEXT;

DO $$
DECLARE
    _constraint RECORD;
BEGIN
    -- The original constraints were unnamed, so find them by what they say.
    FOR _constraint IN
        SELECT con.conname
        FROM pg_constraint AS con
        WHERE con.conrelid = 'silver_census.observation_revision'::regclass
          AND con.contype = 'c'
          AND pg_get_constraintdef(con.oid) LIKE '%geo_level%'
          AND con.conname <> 'observation_revision_geo_level_place_check'
    LOOP
        EXECUTE format(
            'ALTER TABLE silver_census.observation_revision DROP CONSTRAINT %I',
            _constraint.conname
        );
    END LOOP;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'silver_census.observation_revision'::regclass
          AND conname = 'observation_revision_geo_level_place_check'
    ) THEN
        ALTER TABLE silver_census.observation_revision
            ADD CONSTRAINT observation_revision_geo_level_place_check
            CHECK (geo_level IN ('us', 'state', 'county', 'place'));
    END IF;

    FOR _constraint IN
        SELECT con.conname
        FROM pg_constraint AS con
        WHERE con.conrelid = 'control.acs_ingestion_slices'::regclass
          AND con.contype = 'c'
          AND pg_get_constraintdef(con.oid) LIKE '%geo_level%'
          AND con.conname NOT IN (
              'acs_ingestion_slices_geo_level_place_check',
              'acs_ingestion_slices_state_scope_check'
          )
    LOOP
        EXECUTE format(
            'ALTER TABLE control.acs_ingestion_slices DROP CONSTRAINT %I',
            _constraint.conname
        );
    END LOOP;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'control.acs_ingestion_slices'::regclass
          AND conname = 'acs_ingestion_slices_geo_level_place_check'
    ) THEN
        ALTER TABLE control.acs_ingestion_slices
            ADD CONSTRAINT acs_ingestion_slices_geo_level_place_check
            CHECK (geo_level IN ('us', 'state', 'county', 'place'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'control.acs_ingestion_slices'::regclass
          AND conname = 'acs_ingestion_slices_state_scope_check'
    ) THEN
        -- A county or place slice is one state's; the nation and the
        -- all-states slice name none.
        ALTER TABLE control.acs_ingestion_slices
            ADD CONSTRAINT acs_ingestion_slices_state_scope_check
            CHECK (
                (geo_level IN ('us', 'state') AND state_fips IS NULL)
                OR (geo_level IN ('county', 'place') AND state_fips IS NOT NULL AND state_fips ~ '^[0-9]{2}$')
            );
    END IF;
END
$$;
