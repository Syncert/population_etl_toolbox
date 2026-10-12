-- Census ACS at tract grain (sub-county-geography).
--
-- The ACS adapter now requests `for=tract:*&in=state:<fips> county:*` from
-- the 5-year estimates beside its nation, state, county and place slices.
-- The two relations 031 widened for places refuse the new level by CHECK,
-- and the revision relation has no column for the tract code:
--
-- * `control.acs_ingestion_slices` admits `us`, `state`, `county` and
--   `place`, and requires a state for a county or place slice;
-- * `silver_census.observation_revision` admits the same four levels.
--
-- The phase file declares the widened revision relation for a fresh
-- warehouse; this step carries what a rerunnable `CREATE TABLE IF NOT EXISTS`
-- cannot. Every statement is idempotent, and nothing already stored changes.

ALTER TABLE silver_census.observation_revision
    ADD COLUMN IF NOT EXISTS tract_code_source TEXT;

DO $$
DECLARE
    _constraint RECORD;
BEGIN
    FOR _constraint IN
        SELECT con.conname
        FROM pg_constraint AS con
        WHERE con.conrelid = 'silver_census.observation_revision'::regclass
          AND con.contype = 'c'
          AND pg_get_constraintdef(con.oid) LIKE '%geo_level%'
          AND con.conname <> 'observation_revision_geo_level_tract_check'
    LOOP
        EXECUTE format(
            'ALTER TABLE silver_census.observation_revision DROP CONSTRAINT %I',
            _constraint.conname
        );
    END LOOP;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'silver_census.observation_revision'::regclass
          AND conname = 'observation_revision_geo_level_tract_check'
    ) THEN
        ALTER TABLE silver_census.observation_revision
            ADD CONSTRAINT observation_revision_geo_level_tract_check
            CHECK (geo_level IN ('us', 'state', 'county', 'place', 'tract'));
    END IF;

    FOR _constraint IN
        SELECT con.conname
        FROM pg_constraint AS con
        WHERE con.conrelid = 'control.acs_ingestion_slices'::regclass
          AND con.contype = 'c'
          AND pg_get_constraintdef(con.oid) LIKE '%geo_level%'
          AND con.conname NOT IN (
              'acs_ingestion_slices_geo_level_tract_check',
              'acs_ingestion_slices_state_scope_tract_check'
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
          AND conname = 'acs_ingestion_slices_geo_level_tract_check'
    ) THEN
        ALTER TABLE control.acs_ingestion_slices
            ADD CONSTRAINT acs_ingestion_slices_geo_level_tract_check
            CHECK (geo_level IN ('us', 'state', 'county', 'place', 'tract'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'control.acs_ingestion_slices'::regclass
          AND conname = 'acs_ingestion_slices_state_scope_tract_check'
    ) THEN
        -- A county, place or tract slice is one state's; the nation and the
        -- all-states slice name none.
        ALTER TABLE control.acs_ingestion_slices
            ADD CONSTRAINT acs_ingestion_slices_state_scope_tract_check
            CHECK (
                (geo_level IN ('us', 'state') AND state_fips IS NULL)
                OR (geo_level IN ('county', 'place', 'tract') AND state_fips IS NOT NULL AND state_fips ~ '^[0-9]{2}$')
            );
    END IF;
END
$$;
