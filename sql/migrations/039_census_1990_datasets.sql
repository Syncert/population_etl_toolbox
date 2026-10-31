-- The 1990 census beside the ACS and Census 2000 (decennial-2000-and-1990-history).
--
-- The 1990 Summary Tape Files 1A and 3A load as datasets `dec1990stf1` and
-- `dec1990stf3`, each value stored under the ACS estimate the 1990 crosswalk
-- sums it into. 037 named the four dataset CHECKs `*_census_check` and
-- admitted the 2000 datasets; this widens the same four to the 1990 ones.
-- As in 037, the small relations are validated and the partitioned serving
-- fact's CHECK is `NOT VALID`, so no exclusive scan of it runs. A CHECK
-- already naming `dec1990stf3` is left alone, so a rerun changes nothing;
-- absent gold relations are skipped (`gold_acs.sql` creates them later in
-- the manifest with the same list).

DO $$
DECLARE
    _target RECORD;
    _datasets CONSTANT TEXT :=
        '(''acs1'', ''acs5'', ''dec2000sf1'', ''dec2000sf3'', ''dec1990stf1'', ''dec1990stf3'')';
BEGIN
    FOR _target IN
        SELECT *
        FROM (
            VALUES
                ('control.acs_ingestion_slices', 'dataset',
                 'acs_ingestion_slices_dataset_census_check', TRUE),
                ('gold_census.dim_acs_table', 'dataset_code',
                 'dim_acs_table_dataset_code_census_check', TRUE),
                ('gold_census.dim_acs_variable', 'dataset_code',
                 'dim_acs_variable_dataset_code_census_check', TRUE),
                ('gold_census.rpt_acs_observations', 'dataset_code',
                 'rpt_acs_observations_dataset_code_census_check', FALSE)
        ) AS t(relation, column_name, constraint_name, validate)
    LOOP
        CONTINUE WHEN to_regclass(_target.relation) IS NULL;
        CONTINUE WHEN EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = _target.relation::regclass
              AND conname = _target.constraint_name
              AND pg_get_constraintdef(oid) LIKE '%dec1990stf3%'
        );

        EXECUTE format(
            'ALTER TABLE %s DROP CONSTRAINT IF EXISTS %I',
            _target.relation,
            _target.constraint_name
        );
        EXECUTE format(
            'ALTER TABLE %s ADD CONSTRAINT %I CHECK (%I IN %s)%s',
            _target.relation,
            _target.constraint_name,
            _target.column_name,
            _datasets,
            CASE WHEN _target.validate THEN '' ELSE ' NOT VALID' END
        );
    END LOOP;
END
$$;
