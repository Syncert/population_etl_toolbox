-- BLS publishes two kinds of catalog identity.
--
-- Non-LA programs (CES, CPI, JOLTS, CPS) are national fixed-coded series, so
-- one series is one metric. LAUS codes a program, an area, and a measure into
-- every series id: series-level publication produced 13,261 single-place
-- metrics and left no BLS metric spanning geographies, so the explorer's map,
-- distribution bins, and comparison routes had nothing to draw. LAUS therefore
-- publishes per measure, with the grains read from the fact rows rather than
-- declared as a constant.
--
-- The series arm reads them from the rows too, as of 2026-09-12. It used to
-- map `dim_bls_series.geographic_level` -- a configured attribute of the
-- series, not a fact about what is served -- through a CASE whose ELSE was
-- ARRAY['NATIONAL'], so a series with an unrecognised or absent level was
-- published as national, and a series serving nothing at all was published as
-- serving the nation. `fact_bls_observation.geo_level` is already normalised
-- to the catalog vocabulary by the gold refresh, so the rows can say it
-- themselves.

CREATE OR REPLACE VIEW gold_bls.measure_export AS
SELECT
    measure.bls_measure_sk,
    measure.program_code,
    measure.metric_key AS source_object_key,
    measure.metric_display_name,
    measure.unit_of_measure,
    measure.value_type,
    ARRAY_AGG(DISTINCT UPPER(fact.geo_level) ORDER BY UPPER(fact.geo_level)) AS valid_geo_grains,
    MAX(fact.updated_at) AS publication_time
FROM gold_bls.dim_bls_measure AS measure
JOIN gold_bls.fact_bls_observation AS fact
  ON fact.program_code = measure.program_code
 AND fact.measure_code = measure.measure_code
GROUP BY measure.bls_measure_sk;

CREATE OR REPLACE VIEW gold_bls.metric_publisher AS
SELECT
    'BLS'::TEXT AS source_code,
    '1.0'::TEXT AS publisher_contract_version,
    series.series_id::TEXT AS source_object_key,
    'series'::TEXT AS source_object_type,
    COALESCE(NULLIF(series.series_title, ''), series.series_id)::TEXT AS metric_display_name,
    series.unit_of_measure::TEXT AS units,
    series.value_type::TEXT AS measure_kind,
    -- ARRAY_REMOVE because the join is outer: a series the refresh has served
    -- no row for aggregates to {NULL}, and it must publish no grain rather
    -- than a grain spelled NULL.
    ARRAY_REMOVE(
        ARRAY_AGG(DISTINCT UPPER(fact.geo_level) ORDER BY UPPER(fact.geo_level)),
        NULL
    )::TEXT[] AS valid_geo_grains,
    ARRAY['MONTHLY']::TEXT[] AS valid_time_grains,
    NULL::TEXT AS aggregation_characteristic,
    JSONB_BUILD_OBJECT('schema', 'gold_bls', 'relation', 'fact_bls_observation', 'key', series.series_id) AS physical_lineage,
    COALESCE(MAX(fact.updated_at), series.updated_at)::TEXT AS source_watermark,
    NULL::UUID AS source_run_id,
    COALESCE(MAX(fact.updated_at), series.updated_at) AS publication_time,
    'U.S. Bureau of Labor Statistics'::TEXT AS source_name,
    'official-statistics'::TEXT AS source_type,
    survey.reference_url::TEXT AS reference_url
FROM gold_bls.dim_bls_series AS series
JOIN gold_bls.dim_bls_survey AS survey USING (bls_survey_sk)
LEFT JOIN gold_bls.fact_bls_observation AS fact ON fact.bls_series_sk = series.bls_series_sk
WHERE series.program_code NOT IN (SELECT DISTINCT program_code FROM gold_bls.dim_bls_measure)
GROUP BY series.bls_series_sk, survey.bls_survey_sk

UNION ALL

SELECT
    'BLS'::TEXT,
    '1.0'::TEXT,
    export.source_object_key::TEXT,
    'measure'::TEXT,
    export.metric_display_name::TEXT,
    export.unit_of_measure::TEXT,
    export.value_type::TEXT,
    export.valid_geo_grains::TEXT[],
    ARRAY['MONTHLY']::TEXT[],
    NULL::TEXT,
    JSONB_BUILD_OBJECT('schema', 'gold_bls', 'relation', 'fact_bls_observation', 'key', export.source_object_key),
    export.publication_time::TEXT,
    NULL::UUID,
    export.publication_time,
    'U.S. Bureau of Labor Statistics'::TEXT,
    'official-statistics'::TEXT,
    survey.reference_url::TEXT
FROM gold_bls.measure_export AS export
JOIN gold_bls.dim_bls_survey AS survey ON survey.program_code = export.program_code;
