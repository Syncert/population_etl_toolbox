-- 040: an FBI agency's county can come from the Return A file's county code.
--
-- The Return A master file (every agency's monthly offenses, one file a year)
-- carries each agency's FBI county code, which the loader resolves through
-- `silver_fbi.return_a_county_crosswalk`. Relationships it writes record
-- `resolution_method = 'fbi_county_code'`, a spelling the populated table's
-- CHECK does not admit yet. Widening a list invalidates nothing already
-- stored. A CHECK already naming the method is left alone, so a rerun
-- changes nothing; on a fresh bootstrap `silver_fbi.sql` creates the table
-- with the same list.

DO $$
BEGIN
    IF to_regclass('silver_fbi.agency_geography_relationship') IS NULL THEN
        RETURN;
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'silver_fbi.agency_geography_relationship'::regclass
          AND conname = 'agency_geography_relationship_resolution_method_check'
          AND pg_get_constraintdef(oid) LIKE '%fbi_county_code%'
    ) THEN
        RETURN;
    END IF;
    ALTER TABLE silver_fbi.agency_geography_relationship
        DROP CONSTRAINT IF EXISTS
            agency_geography_relationship_resolution_method_check;
    ALTER TABLE silver_fbi.agency_geography_relationship
        ADD CONSTRAINT agency_geography_relationship_resolution_method_check
        CHECK (resolution_method IS NULL OR resolution_method IN (
            'exact_state_code', 'county_label_match',
            'reviewed_place_crosswalk', 'fbi_county_code'
        ));
END
$$;
