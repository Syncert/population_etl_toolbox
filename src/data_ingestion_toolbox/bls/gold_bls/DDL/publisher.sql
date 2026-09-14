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
--
-- Both arms now read those rows from `gold_bls.mv_bls_latest`, the relation
-- the dispatch entry names for a `latest` read: a grain published here is one
-- the API can answer. `fact_bls_observation` is a view straight over silver,
-- so grains and publication time advanced at silver ingest, before the
-- serving refresh -- land 2025 county LAUS in silver, harvest, then refresh,
-- and the catalog published COUNTY while `/observations?geo_level=COUNTY`
-- read the projection and answered an empty page. Worse, the harvest recorded
-- the new fingerprint, so the next harvest skipped and the catalog stayed
-- wrong until something else republished (DB-036). `gold_fred` and
-- `gold_census` were changed this way first.
--
-- Through `gold_glossary.geo_grain`, the one mapping (migration 018, moved
-- earlier in the bootstrap by 021 so the phases that call it run after it):
-- a grain spelled here is the word the catalog publishes and the API
-- filters on, and no view carries its own copy (DB-037).
--
-- The identity rule matches the serving refresh exactly. The refresh writes
-- `COALESCE('BLS:' || measure.metric_key, 'BLS:' || series.series_id)` per
-- row, so a series keeps its own identity when *its rows* carry a
-- (program_code, measure_code) pair `dim_bls_measure` does not hold. The
-- series arm used to exclude whole programs instead, so an LA measure code
-- nobody seeded served rows under `BLS:LAU...` identities the publisher never
-- published: `/observations?metric_code=...` answered "unknown metric" while
-- `/bls/observations/timeseries` paged the rows.

CREATE OR REPLACE VIEW gold_bls.measure_export AS
SELECT
    measure.bls_measure_sk,
    measure.program_code,
    measure.metric_key AS source_object_key,
    measure.metric_display_name,
    measure.unit_of_measure,
    measure.value_type,
    COALESCE(served.valid_geo_grains, ARRAY[]::TEXT[]) AS valid_geo_grains,
    -- The served rows' time, falling back to the fact rows only when nothing
    -- is served yet: such a measure publishes no grain either, so nothing
    -- claims the API can answer it, and the harvest's content fingerprint
    -- re-harvests once the projection carries it.
    COALESCE(served.publication_time, MAX(fact.updated_at)) AS publication_time
FROM gold_bls.dim_bls_measure AS measure
JOIN gold_bls.fact_bls_observation AS fact
  ON fact.program_code = measure.program_code
 AND fact.measure_code = measure.measure_code
LEFT JOIN (
    SELECT latest.metric_code,
           ARRAY_AGG(DISTINCT gold_glossary.geo_grain(latest.geo_level)
                     ORDER BY gold_glossary.geo_grain(latest.geo_level))::TEXT[]
               AS valid_geo_grains,
           MAX(latest.updated_at) AS publication_time
    FROM gold_bls.mv_bls_latest AS latest
    GROUP BY latest.metric_code
) AS served
  ON served.metric_code = 'BLS:' || measure.metric_key
GROUP BY measure.bls_measure_sk, served.valid_geo_grains, served.publication_time;

CREATE OR REPLACE VIEW gold_bls.metric_publisher AS
SELECT
    'BLS'::TEXT AS source_code,
    '1.0'::TEXT AS publisher_contract_version,
    series.series_id::TEXT AS source_object_key,
    'series'::TEXT AS source_object_type,
    COALESCE(NULLIF(series.series_title, ''), series.series_id)::TEXT AS metric_display_name,
    series.unit_of_measure::TEXT AS units,
    series.value_type::TEXT AS measure_kind,
    -- Empty rather than {NULL}: a series the refresh has served no row for
    -- must publish no grain, not a grain spelled NULL.
    COALESCE(served.valid_geo_grains, ARRAY[]::TEXT[])::TEXT[] AS valid_geo_grains,
    ARRAY['MONTHLY']::TEXT[] AS valid_time_grains,
    NULL::TEXT AS aggregation_characteristic,
    JSONB_BUILD_OBJECT('schema', 'gold_bls', 'relation', 'fact_bls_observation', 'key', series.series_id) AS physical_lineage,
    COALESCE(served.publication_time, series.updated_at)::TEXT AS source_watermark,
    NULL::UUID AS source_run_id,
    COALESCE(served.publication_time, series.updated_at) AS publication_time,
    'U.S. Bureau of Labor Statistics'::TEXT AS source_name,
    'official-statistics'::TEXT AS source_type,
    survey.reference_url::TEXT AS reference_url
FROM gold_bls.dim_bls_series AS series
JOIN gold_bls.dim_bls_survey AS survey USING (bls_survey_sk)
LEFT JOIN (
    SELECT latest.series_id,
           ARRAY_AGG(DISTINCT gold_glossary.geo_grain(latest.geo_level)
                     ORDER BY gold_glossary.geo_grain(latest.geo_level))::TEXT[]
               AS valid_geo_grains,
           MAX(latest.updated_at) AS publication_time
    FROM gold_bls.mv_bls_latest AS latest
    WHERE latest.metric_code = 'BLS:' || latest.series_id
    GROUP BY latest.series_id
) AS served
  ON served.series_id = series.series_id
WHERE NOT EXISTS (
        -- Its rows are measure-identified, so the measure arm publishes them.
        SELECT 1
        FROM gold_bls.fact_bls_observation AS fact
        JOIN gold_bls.dim_bls_measure AS measure
          ON measure.program_code = fact.program_code
         AND measure.measure_code = fact.measure_code
        WHERE fact.bls_series_sk = series.bls_series_sk
      )
  AND (
        -- A series with no rows at all keeps its identity only where the
        -- program has no measure identities: a LAUS series captured with an
        -- empty answer must not become one of the 13,261 single-place metrics
        -- measure identity exists to avoid.
        EXISTS (
            SELECT 1
            FROM gold_bls.fact_bls_observation AS fact
            WHERE fact.bls_series_sk = series.bls_series_sk
        )
        OR series.program_code NOT IN (
            SELECT DISTINCT program_code FROM gold_bls.dim_bls_measure
        )
      )
GROUP BY series.bls_series_sk, survey.bls_survey_sk, served.valid_geo_grains,
         served.publication_time

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
