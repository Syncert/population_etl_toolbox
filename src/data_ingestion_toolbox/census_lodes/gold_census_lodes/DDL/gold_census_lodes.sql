-- Publication views over reconciled LEHD LODES county aggregates
-- (census-lehd-lodes). Applied by the bootstrap manifest in the `gold`
-- phase and re-applied by the DAG's `ensure_census_lodes_schema` task; this
-- file is the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_census_lodes;

CREATE OR REPLACE VIEW gold_census_lodes.measure_definition AS
SELECT measure.measure, measure.measure_label, 'jobs'::TEXT AS unit,
       'LEHD LODES: jobs counted from administrative records, summed from '
       || 'census blocks by this warehouse; workplace counts carry noise and '
       || 'residence counts are synthesized, so each is a protected estimate'::TEXT
           AS observation_basis
FROM (VALUES
    ('resident_workers', 'Jobs held by people living here'),
    ('jobs', 'Jobs located here'),
    ('live_and_work', 'Jobs held by people living and working here'),
    ('inbound', 'Jobs here held by people living elsewhere'),
    ('outbound_in_state', 'Jobs elsewhere in the state held by people living here')
) AS measure(measure, measure_label);

-- Every published state-year's county and state sums, one row per measure
-- and geography, computed once when the state-year publishes
-- (`refresh_observation_fact`) instead of on every read. The serving views
-- below read this table by key: a place page asks for one geography and
-- never makes the warehouse re-sum every published year's flows (W-9 loads
-- 2002 onwards, about 1,100 state-years).
--
-- A county that did not resolve is not computed (DB-035), though its blocks
-- still count toward its state, which is a sum of every block the state
-- published. A measure whose file the state did not publish (no workplace
-- file, for instance) has no row, never a zero. The release identity is the
-- state's data vintage. Rows of every published run stay: a new vintage is a
-- second release beside the one it revised.
CREATE TABLE IF NOT EXISTS gold_census_lodes.observation_fact (
    metric_key TEXT NOT NULL,
    geo_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    release_key TEXT NOT NULL,
    run_id UUID NOT NULL REFERENCES control.census_lodes_slice(run_id),
    geo_type TEXT NOT NULL CHECK (geo_type IN ('county', 'state')),
    value BIGINT NOT NULL CHECK (value >= 0),
    capture_id UUID NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT lodes_observation_fact_pkey
        PRIMARY KEY (metric_key, geo_id, year, release_key, run_id)
);

-- A place page reads every measure of one geography.
CREATE INDEX IF NOT EXISTS ix_lodes_observation_fact_geo_year
    ON gold_census_lodes.observation_fact (geo_id, year);
-- The refresh replaces one run's rows.
CREATE INDEX IF NOT EXISTS ix_lodes_observation_fact_run
    ON gold_census_lodes.observation_fact (run_id);

-- Recompute the served sums of the given published runs, or, given NULL, of
-- every published run that has none yet. Each run's sums read only its own
-- silver rows (both fact tables are keyed by run first), so a refresh costs
-- the state-years it names, never the whole history. Returns the rows written.
CREATE OR REPLACE FUNCTION gold_census_lodes.refresh_observation_fact(p_run_ids UUID[])
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_runs UUID[];
    v_written BIGINT;
BEGIN
    IF p_run_ids IS NULL THEN
        SELECT COALESCE(ARRAY_AGG(slice.run_id), ARRAY[]::UUID[]) INTO v_runs
        FROM control.census_lodes_slice AS slice
        WHERE slice.status = 'published'
          AND NOT EXISTS (
              SELECT 1 FROM gold_census_lodes.observation_fact AS fact
              WHERE fact.run_id = slice.run_id
          );
    ELSE
        SELECT COALESCE(ARRAY_AGG(slice.run_id), ARRAY[]::UUID[]) INTO v_runs
        FROM control.census_lodes_slice AS slice
        WHERE slice.status = 'published' AND slice.run_id = ANY(p_run_ids);
    END IF;
    IF CARDINALITY(v_runs) = 0 THEN
        RETURN 0;
    END IF;

    DELETE FROM gold_census_lodes.observation_fact WHERE run_id = ANY(v_runs);

    WITH published AS (
        SELECT slice.run_id, slice.data_vintage, slice.published_at
        FROM control.census_lodes_slice AS slice
        WHERE slice.run_id = ANY(v_runs)
    ),
    area AS (
        SELECT area.run_id, area.family, area.geo_id, area.geography_status,
               area.value, area.capture_id
        FROM silver_census_lodes.fact_area AS area
        WHERE area.run_id = ANY(v_runs) AND area.column_code = 'C000'
    ),
    flow AS (
        SELECT flow.run_id, flow.part, flow.home_geo_id, flow.work_geo_id,
               flow.jobs, flow.capture_id
        FROM silver_census_lodes.fact_flow AS flow
        WHERE flow.run_id = ANY(v_runs)
    ),
    measured AS (
        SELECT run_id, CASE family WHEN 'rac' THEN 'resident_workers' ELSE 'jobs' END AS measure,
               geo_id, value, capture_id, 'county'::TEXT AS geo_type
        FROM area
        WHERE geography_status NOT IN ('unsupported', 'ambiguous', 'unmapped')
        UNION ALL
        SELECT run_id, 'live_and_work', work_geo_id, SUM(jobs), MIN(capture_id::TEXT)::UUID, 'county'
        FROM flow WHERE part = 'main' AND home_geo_id = work_geo_id
        GROUP BY run_id, work_geo_id
        UNION ALL
        SELECT run_id, 'inbound', work_geo_id, SUM(jobs), MIN(capture_id::TEXT)::UUID, 'county'
        FROM flow WHERE home_geo_id <> work_geo_id
        GROUP BY run_id, work_geo_id
        UNION ALL
        SELECT run_id, 'outbound_in_state', home_geo_id, SUM(jobs), MIN(capture_id::TEXT)::UUID, 'county'
        FROM flow WHERE part = 'main' AND home_geo_id <> work_geo_id
        GROUP BY run_id, home_geo_id
        UNION ALL
        SELECT run_id, CASE family WHEN 'rac' THEN 'resident_workers' ELSE 'jobs' END,
               SPLIT_PART(geo_id, '|', 1), SUM(value), MIN(capture_id::TEXT)::UUID, 'state'
        FROM area
        GROUP BY run_id, family, SPLIT_PART(geo_id, '|', 1)
        UNION ALL
        SELECT run_id, CASE part WHEN 'main' THEN 'live_and_work' ELSE 'inbound' END,
               SPLIT_PART(work_geo_id, '|', 1), SUM(jobs), MIN(capture_id::TEXT)::UUID, 'state'
        FROM flow
        GROUP BY run_id, part, SPLIT_PART(work_geo_id, '|', 1)
    )
    INSERT INTO gold_census_lodes.observation_fact (
        metric_key, geo_id, year, release_key, run_id, geo_type, value,
        capture_id, retrieved_at
    )
    SELECT measured.measure, measured.geo_id, slice.year, published.data_vintage,
           measured.run_id, measured.geo_type, measured.value, measured.capture_id,
           published.published_at
    FROM measured
    JOIN published USING (run_id)
    JOIN control.census_lodes_slice AS slice USING (run_id);
    GET DIAGNOSTICS v_written = ROW_COUNT;
    RETURN v_written;
END;
$$;

-- Every published state-year, one row per measure and geography, read from
-- the precomputed sums by key.
CREATE OR REPLACE VIEW gold_census_lodes.observation_revision AS
SELECT fact.metric_key,
       fact.metric_key AS measure,
       definition.measure_label,
       definition.unit,
       definition.observation_basis,
       fact.year,
       MAKE_DATE(fact.year, 1, 1) AS period_start,
       MAKE_DATE(fact.year, 12, 31) AS period_end,
       fact.geo_id,
       entity.geo_sk,
       fact.geo_type,
       gold_glossary.geo_grain(fact.geo_type) AS geo_level,
       fact.value::NUMERIC AS value,
       'valid'::TEXT AS value_status,
       fact.release_key,
       fact.retrieved_at,
       md5(fact.metric_key || '|' || fact.geo_id || '|' || fact.year::TEXT
           || '|' || fact.release_key) AS source_record_id,
       fact.capture_id,
       fact.run_id
FROM gold_census_lodes.observation_fact AS fact
JOIN gold_census_lodes.measure_definition AS definition ON definition.measure = fact.metric_key
JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = fact.geo_id;

-- The served sums of every state-year published before this table existed.
-- A no-op once each published run has its rows.
SELECT gold_census_lodes.refresh_observation_fact(NULL);

-- The newest published vintage of each observation.
CREATE OR REPLACE VIEW gold_census_lodes.observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.year)
       revision.*
FROM gold_census_lodes.observation_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.year,
         revision.release_key DESC, revision.retrieved_at DESC, revision.run_id DESC;

CREATE OR REPLACE VIEW gold_census_lodes.measure_export AS
SELECT measure AS source_object_key, measure, measure_label, unit, observation_basis,
       'census_lodes8_csv:v1'::TEXT AS schema_version
FROM gold_census_lodes.measure_definition;

COMMENT ON SCHEMA gold_census_lodes IS
    'Policy-free publication views for LEHD LODES county and state job counts, summed from blocks.';
