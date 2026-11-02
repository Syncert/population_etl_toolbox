-- Census ACS at metro (CBSA) grain (v1.0 build-out, W-7).
--
-- The ACS adapter now requests
-- `for=metropolitan statistical area/micropolitan statistical area:*` beside
-- its nation, state, county, place and tract slices: the Bureau's published
-- estimates for each metro and micro area, not a roll-up of counties. The two
-- relations 032 widened refuse the new level by CHECK, and the revision
-- relation has no column for the CBSA code:
--
-- * `control.acs_ingestion_slices` admits `us`, `state`, `county`, `place`
--   and `tract`;
-- * `silver_census.observation_revision` admits the same five levels.
--
-- A metro slice, like the nation's and the all-states slice, names no state:
-- a CBSA can span several. Every statement is idempotent, and nothing already
-- stored changes.

ALTER TABLE silver_census.observation_revision
    ADD COLUMN IF NOT EXISTS cbsa_source TEXT;

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
          AND con.conname <> 'observation_revision_geo_level_metro_check'
    LOOP
        EXECUTE format(
            'ALTER TABLE silver_census.observation_revision DROP CONSTRAINT %I',
            _constraint.conname
        );
    END LOOP;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'silver_census.observation_revision'::regclass
          AND conname = 'observation_revision_geo_level_metro_check'
    ) THEN
        ALTER TABLE silver_census.observation_revision
            ADD CONSTRAINT observation_revision_geo_level_metro_check
            CHECK (geo_level IN ('us', 'state', 'county', 'place', 'tract', 'metro'));
    END IF;

    FOR _constraint IN
        SELECT con.conname
        FROM pg_constraint AS con
        WHERE con.conrelid = 'control.acs_ingestion_slices'::regclass
          AND con.contype = 'c'
          AND pg_get_constraintdef(con.oid) LIKE '%geo_level%'
          AND con.conname NOT IN (
              'acs_ingestion_slices_geo_level_metro_check',
              'acs_ingestion_slices_state_scope_metro_check'
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
          AND conname = 'acs_ingestion_slices_geo_level_metro_check'
    ) THEN
        ALTER TABLE control.acs_ingestion_slices
            ADD CONSTRAINT acs_ingestion_slices_geo_level_metro_check
            CHECK (geo_level IN ('us', 'state', 'county', 'place', 'tract', 'metro'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'control.acs_ingestion_slices'::regclass
          AND conname = 'acs_ingestion_slices_state_scope_metro_check'
    ) THEN
        ALTER TABLE control.acs_ingestion_slices
            ADD CONSTRAINT acs_ingestion_slices_state_scope_metro_check
            CHECK (
                (geo_level IN ('us', 'state', 'metro') AND state_fips IS NULL)
                OR (geo_level IN ('county', 'place', 'tract') AND state_fips IS NOT NULL AND state_fips ~ '^[0-9]{2}$')
            );
    END IF;
END
$$;
