-- Census 2000 beside the ACS (decennial-2000-and-1990-history).
--
-- The Census ACS adapter now requests the 2000 census's Summary Files 1 and
-- 3 as datasets `dec2000sf1` and `dec2000sf3`, storing each crosswalked value
-- under the ACS estimate it measures. Four relations refuse any dataset but
-- `acs1` and `acs5` by CHECK:
--
-- * `control.acs_ingestion_slices.dataset`;
-- * `gold_census.dim_acs_table.dataset_code`;
-- * `gold_census.dim_acs_variable.dataset_code`;
-- * `gold_census.rpt_acs_observations.dataset_code` (partitioned).
--
-- `gold_census.dim_acs_table.survey_span_years` also admits 0: a census is
-- one day's count and pools no survey years.
--
-- Each CHECK is replaced by one admitting the census datasets too. The new
-- set is a superset of the old, so every stored row already satisfies it;
-- the three small relations are validated here, while the serving fact
-- (168 GB on OFFICE_LAB in October 2026) takes its CHECK `NOT VALID`, which
-- enforces every new row at once without scanning old ones under an
-- exclusive lock. `VALIDATE CONSTRAINT` can run later under a lock that
-- does not block readers. The gold relations do not exist yet on a fresh
-- warehouse, where `gold_acs.sql` (the manifest's later `gold-census` phase)
-- creates them with the new CHECK. Every statement is idempotent.

DO $$
DECLARE
    _target RECORD;
    _constraint RECORD;
    _datasets CONSTANT TEXT := '(''acs1'', ''acs5'', ''dec2000sf1'', ''dec2000sf3'')';
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

        FOR _constraint IN
            SELECT con.conname
            FROM pg_constraint AS con
            WHERE con.conrelid = _target.relation::regclass
              AND con.contype = 'c'
              AND con.conislocal
              AND pg_get_constraintdef(con.oid) LIKE '%' || _target.column_name || '%acs1%'
              AND con.conname <> _target.constraint_name
        LOOP
            EXECUTE format(
                'ALTER TABLE %s DROP CONSTRAINT %I',
                _target.relation,
                _constraint.conname
            );
        END LOOP;

        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = _target.relation::regclass
              AND conname = _target.constraint_name
        ) THEN
            EXECUTE format(
                'ALTER TABLE %s ADD CONSTRAINT %I CHECK (%I IN %s)%s',
                _target.relation,
                _target.constraint_name,
                _target.column_name,
                _datasets,
                CASE WHEN _target.validate THEN '' ELSE ' NOT VALID' END
            );
        END IF;
    END LOOP;

    IF to_regclass('gold_census.dim_acs_table') IS NOT NULL THEN
        FOR _constraint IN
            SELECT con.conname
            FROM pg_constraint AS con
            WHERE con.conrelid = 'gold_census.dim_acs_table'::regclass
              AND con.contype = 'c'
              AND pg_get_constraintdef(con.oid) LIKE '%survey_span_years%'
              AND con.conname <> 'dim_acs_table_survey_span_years_census_check'
        LOOP
            EXECUTE format(
                'ALTER TABLE gold_census.dim_acs_table DROP CONSTRAINT %I',
                _constraint.conname
            );
        END LOOP;
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = 'gold_census.dim_acs_table'::regclass
              AND conname = 'dim_acs_table_survey_span_years_census_check'
        ) THEN
            ALTER TABLE gold_census.dim_acs_table
                ADD CONSTRAINT dim_acs_table_survey_span_years_census_check
                CHECK (survey_span_years IN (0, 1, 5));
        END IF;
    END IF;
END
$$;
