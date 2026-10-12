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

-- Every published state-year, one row per measure and geography. A county
-- that did not resolve is not served (DB-035), though its blocks still count
-- toward its state, which is a sum of every block the state published. A measure
-- whose file the state did not publish (no workplace file, for instance) has
-- no row, never a zero. The release identity is the state's data vintage.
CREATE OR REPLACE VIEW gold_census_lodes.observation_revision AS
WITH published AS (
    SELECT slice.run_id, slice.state, slice.year, slice.data_vintage, slice.published_at
    FROM control.census_lodes_slice AS slice
    WHERE slice.status = 'published'
),
area AS (
    SELECT area.run_id, area.family, area.geo_id, area.geo_sk, area.value, area.capture_id
    FROM silver_census_lodes.fact_area AS area
    JOIN published USING (run_id)
    WHERE area.column_code = 'C000'
      AND area.geography_status NOT IN ('unsupported', 'ambiguous', 'unmapped')
),
flow AS (
    SELECT flow.* FROM silver_census_lodes.fact_flow AS flow JOIN published USING (run_id)
),
county AS (
    SELECT run_id, CASE family WHEN 'rac' THEN 'resident_workers' ELSE 'jobs' END AS measure,
           geo_id, value, capture_id
    FROM area
    UNION ALL
    SELECT run_id, 'live_and_work', work_geo_id, SUM(jobs), MIN(capture_id::TEXT)::UUID
    FROM flow WHERE part = 'main' AND home_geo_id = work_geo_id
    GROUP BY run_id, work_geo_id
    UNION ALL
    SELECT run_id, 'inbound', work_geo_id, SUM(jobs), MIN(capture_id::TEXT)::UUID
    FROM flow WHERE home_geo_id <> work_geo_id
    GROUP BY run_id, work_geo_id
    UNION ALL
    SELECT run_id, 'outbound_in_state', home_geo_id, SUM(jobs), MIN(capture_id::TEXT)::UUID
    FROM flow WHERE part = 'main' AND home_geo_id <> work_geo_id
    GROUP BY run_id, home_geo_id
),
state AS (
    SELECT area.run_id, CASE area.family WHEN 'rac' THEN 'resident_workers' ELSE 'jobs' END AS measure,
           SPLIT_PART(area.geo_id, '|', 1) AS geo_id,
           SUM(area.value) AS value, MIN(area.capture_id::TEXT)::UUID AS capture_id
    FROM silver_census_lodes.fact_area AS area
    JOIN published USING (run_id)
    WHERE area.column_code = 'C000'
    GROUP BY area.run_id, area.family, 3
    UNION ALL
    SELECT run_id, CASE part WHEN 'main' THEN 'live_and_work' ELSE 'inbound' END,
           SPLIT_PART(work_geo_id, '|', 1), SUM(jobs), MIN(capture_id::TEXT)::UUID
    FROM flow
    GROUP BY run_id, part, 3
),
measured AS (
    SELECT county.*, 'county'::TEXT AS geo_type FROM county
    UNION ALL
    SELECT state.run_id, state.measure, state.geo_id, state.value, state.capture_id, 'state'::TEXT
    FROM state
)
SELECT measured.measure AS metric_key,
       measured.measure,
       definition.measure_label,
       definition.unit,
       definition.observation_basis,
       published.year,
       MAKE_DATE(published.year, 1, 1) AS period_start,
       MAKE_DATE(published.year, 12, 31) AS period_end,
       measured.geo_id,
       entity.geo_sk,
       measured.geo_type,
       gold_glossary.geo_grain(measured.geo_type) AS geo_level,
       measured.value::NUMERIC AS value,
       'valid'::TEXT AS value_status,
       published.data_vintage AS release_key,
       published.published_at AS retrieved_at,
       md5(measured.measure || '|' || measured.geo_id || '|' || published.year::TEXT
           || '|' || published.data_vintage) AS source_record_id,
       measured.capture_id,
       measured.run_id
FROM measured
JOIN published USING (run_id)
JOIN gold_census_lodes.measure_definition AS definition ON definition.measure = measured.measure
JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = measured.geo_id;

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
