-- Grains are derived from the served relation, never declared.
--
-- This view used to declare ARRAY['NATIONAL'] for every series. It was true:
-- every FRED series served today is national. It was true the way the ACS
-- declaration was true on the day it was written, and on 2026-09-12 that one
-- was found advertising 2,487 metric/grain pairs nothing served. A declared
-- grain is a claim about the future; a derived grain is a fact about the rows.
-- FRED is the source most likely to gain a regional series -- one
-- configuration entry away -- and the first such series would have been
-- published as NATIONAL by this line.
--
-- Deriving also removes a category of defect rather than catching it: a series
-- with no served rows publishes no grain, so the catalog-serving agreement
-- guards (DB-025, DB-028) report a current code with nothing behind it instead
-- of this view inventing a grain for it.
--
-- Order matters now: the glossary harvest must run after the serving refresh,
-- or it publishes the empty array. The ingest DAG already emits
-- `publisher_ready` downstream of `gold_fred_refresh`.
CREATE OR REPLACE VIEW gold_fred.metric_publisher AS
SELECT
    'FRED'::TEXT AS source_code,
    '1.0'::TEXT AS publisher_contract_version,
    series.series_id::TEXT AS source_object_key,
    'series'::TEXT AS source_object_type,
    COALESCE(NULLIF(series.series_title, ''), series.series_id)::TEXT AS metric_display_name,
    series.units::TEXT AS units,
    NULL::TEXT AS measure_kind,
    COALESCE(served.valid_geo_grains, ARRAY[]::TEXT[]) AS valid_geo_grains,
    CASE
        WHEN LOWER(COALESCE(series.frequency, '')) LIKE '%daily%' THEN ARRAY['DAILY']::TEXT[]
        WHEN LOWER(COALESCE(series.frequency, '')) LIKE '%weekly%' THEN ARRAY['WEEKLY']::TEXT[]
        WHEN LOWER(COALESCE(series.frequency, '')) LIKE '%quarter%' THEN ARRAY['QUARTERLY']::TEXT[]
        WHEN LOWER(COALESCE(series.frequency, '')) LIKE '%annual%' THEN ARRAY['ANNUAL']::TEXT[]
        ELSE ARRAY['MONTHLY']::TEXT[]
    END AS valid_time_grains,
    NULL::TEXT AS aggregation_characteristic,
    JSONB_BUILD_OBJECT('schema', 'gold_fred', 'relation', 'fact_fred_observation', 'key', series.series_id) AS physical_lineage,
    COALESCE(MAX(fact.updated_at), series.updated_at)::TEXT AS source_watermark,
    NULL::UUID AS source_run_id,
    COALESCE(MAX(fact.updated_at), series.updated_at) AS publication_time,
    'Federal Reserve Economic Data'::TEXT AS source_name,
    'economic-data-aggregator'::TEXT AS source_type,
    series.reference_url::TEXT AS reference_url
FROM gold_fred.dim_fred_series AS series
LEFT JOIN gold_fred.fact_fred_observation AS fact ON fact.fred_series_sk = series.fred_series_sk
LEFT JOIN (
    -- One row per served metric with the grains its latest rows carry, in the
    -- vocabulary the API filters on -- through `gold_glossary.geo_grain`, the
    -- one mapping. Migration 021 moved that function ahead of the `gold` and
    -- `publisher` phases so this call is legal at bootstrap; before it, this
    -- file upper-cased the served word itself, which was right for FRED and
    -- one more copy of the vocabulary (DB-037).
    --
    -- `mv_fred_latest` rather than the fact table, because it is the relation
    -- the dispatch entry names for a `latest` read: a grain published here is
    -- one the API can answer.
    SELECT latest.metric_code,
           ARRAY_AGG(DISTINCT gold_glossary.geo_grain(latest.geo_level)
                     ORDER BY gold_glossary.geo_grain(latest.geo_level))::TEXT[]
               AS valid_geo_grains
    FROM gold_fred.mv_fred_latest AS latest
    GROUP BY latest.metric_code
) AS served
  ON served.metric_code = 'FRED:' || series.series_id
GROUP BY series.fred_series_sk, served.valid_geo_grains;
