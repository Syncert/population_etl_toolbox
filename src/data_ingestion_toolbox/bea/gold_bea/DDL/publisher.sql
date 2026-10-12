-- Provider-neutral glossary publisher contract for BEA regional accounts
-- (bea-regional-accounts). One row per (table, line); the display name
-- carries the line's own description and its unit, so chained and current
-- dollars are two metrics that read differently. Grains are aggregated from
-- the rows the latest view serves with a value.
CREATE OR REPLACE VIEW gold_bea.metric_publisher AS
SELECT 'BEA'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       served.metric_key AS source_object_key,
       'measure'::TEXT AS source_object_type,
       (line.description || ' (' || line.table_code || ', BEA ' || LOWER(line.unit) || ')')::TEXT
           AS metric_display_name,
       line.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY['ANNUAL']::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_bea',
           'relation', 'observation_revision',
           'key', served.metric_key
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       'Bureau of Economic Analysis regional economic accounts'::TEXT AS source_name,
       'government-economic-accounts'::TEXT AS source_type,
       line.methodology_url::TEXT AS reference_url
FROM (
    SELECT latest.metric_key,
           latest.table_code,
           latest.line_code,
           MAX(latest.release_date)::TEXT AS source_watermark,
           -- A grain is one a value is published at.
           COALESCE(
               ARRAY_AGG(DISTINCT gold_glossary.geo_grain(latest.geo_type)
                         ORDER BY gold_glossary.geo_grain(latest.geo_type))
                   FILTER (WHERE latest.value IS NOT NULL),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains,
           (ARRAY_AGG(latest.run_id ORDER BY latest.retrieved_at DESC))[1] AS source_run_id,
           MAX(capture.published_at) AS publication_time
    FROM gold_bea.observation_latest AS latest
    JOIN control.bea_table_capture AS capture ON capture.capture_id = latest.capture_id
    GROUP BY latest.metric_key, latest.table_code, latest.line_code
) AS served
JOIN silver_bea.dim_line AS line
  ON line.table_code = served.table_code
 AND line.line_code = served.line_code
WHERE served.publication_time IS NOT NULL;
