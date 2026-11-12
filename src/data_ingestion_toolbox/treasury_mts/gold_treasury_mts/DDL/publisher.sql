-- Provider-neutral glossary publisher contract for the Monthly Treasury
-- Statement, Table 9 (treasury-mts). One row per line with a published
-- fiscal-year total. Grains are aggregated from served rows: one served row
-- per line is read, since every row of a line is the nation.
CREATE OR REPLACE VIEW gold_treasury_mts.metric_publisher AS
SELECT 'TREASURY_MTS'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       served.metric_key AS source_object_key,
       'measure'::TEXT AS source_object_type,
       (export.measure_label || ' (US Treasury)')::TEXT AS metric_display_name,
       export.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY[export.time_grain]::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_treasury_mts',
           'relation', 'observation_revision',
           'key', served.metric_key
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       'U.S. Department of the Treasury, Bureau of the Fiscal Service'::TEXT AS source_name,
       'government-statistical-program'::TEXT AS source_type,
       'https://fiscaldata.treasury.gov/datasets/monthly-treasury-statement/'::TEXT AS reference_url
FROM (
    SELECT definition.metric_key,
           'MTS ' || TO_CHAR(newest.period_end, 'YYYY-MM') AS source_watermark,
           newest.run_id AS source_run_id,
           newest.published_at AS publication_time,
           COALESCE(
               (SELECT ARRAY_AGG(DISTINCT gold_glossary.geo_grain(latest.geo_type)
                                 ORDER BY gold_glossary.geo_grain(latest.geo_type))
                           FILTER (WHERE latest.value IS NOT NULL)
                  FROM (
                        SELECT sample.geo_type, sample.value
                          FROM gold_treasury_mts.observation_latest AS sample
                         WHERE sample.metric_key = definition.metric_key
                         LIMIT 1
                       ) AS latest),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains
    FROM gold_treasury_mts.line_definition AS definition
    -- The newest published fiscal-year total of the line.
    JOIN LATERAL (
        SELECT latest.period_end, latest.run_id, latest.published_at
        FROM gold_treasury_mts.observation_latest AS latest
        WHERE latest.metric_key = definition.metric_key AND latest.value IS NOT NULL
        ORDER BY latest.period_end DESC
        LIMIT 1
    ) AS newest ON TRUE
) AS served
JOIN gold_treasury_mts.measure_export AS export ON export.source_object_key = served.metric_key
WHERE CARDINALITY(served.valid_geo_grains) > 0;
