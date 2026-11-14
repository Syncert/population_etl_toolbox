-- Publication views over IRS SOI county and state income data (irs-soi).
-- Applied by the bootstrap manifest in the `gold` phase and re-applied by the
-- DAG's `ensure_irs_soi_schema` task; this file is the only definition of
-- these views.

CREATE SCHEMA IF NOT EXISTS gold_irs_soi;

-- Every published capture of every value: the as-released surface. A revised
-- file is a second row with its own capture and retrieval time, never an
-- overwrite. SOI names no release, so the release identity is the read.
-- The all-returns row of the by-AGI county file repeats the county totals
-- file, which is the product for all returns, so it is not served twice.
CREATE OR REPLACE VIEW gold_irs_soi.observation_revision AS
SELECT fact.metric_key,
       fact.measure_id,
       measure.measure_label,
       measure.soi_column,
       measure.unit,
       fact.agi_band,
       band.band_label,
       band.lower_dollars AS agi_lower_dollars,
       band.upper_dollars AS agi_upper_dollars,
       fact.file_kind,
       CASE WHEN fact.file_kind = 'state' THEN 'state' ELSE 'county' END AS product,
       fact.tax_year,
       fact.tax_year AS year,
       MAKE_DATE(fact.tax_year, 1, 1) AS period_start,
       MAKE_DATE(fact.tax_year, 12, 31) AS period_end,
       fact.geo_id,
       fact.geo_sk,
       fact.geo_type,
       gold_glossary.geo_grain(fact.geo_type) AS geo_level,
       fact.value_source,
       fact.value,
       fact.value_status,
       fact.retrieved_at,
       TO_CHAR(fact.retrieved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS release_key,
       fact.source_record_id,
       fact.capture_id,
       fact.run_id
FROM silver_irs_soi.fact_observation AS fact
JOIN silver_irs_soi.dim_measure AS measure ON measure.measure_id = fact.measure_id
JOIN silver_irs_soi.dim_agi_band AS band ON band.agi_band = fact.agi_band
JOIN control.irs_soi_file AS file ON file.capture_id = fact.capture_id
WHERE file.status = 'published'
  AND fact.geography_status NOT IN ('unmapped', 'unsupported', 'ambiguous')
  AND NOT (fact.file_kind = 'county_agi' AND fact.agi_band = 'all');

-- One value per metric, place and tax year: the county files first (they
-- describe counties and their states together), the state file where they
-- say nothing (the nation, and a band only the state file splits out), then
-- the newest capture.
CREATE OR REPLACE VIEW gold_irs_soi.observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.tax_year)
       revision.*
FROM gold_irs_soi.observation_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.tax_year,
         (revision.product = 'county') DESC,
         revision.retrieved_at DESC, revision.capture_id DESC;

-- Every metric the registered items and bands can publish.
CREATE OR REPLACE VIEW gold_irs_soi.measure_export AS
SELECT measure.measure_id
           || CASE WHEN band.agi_band = 'all' THEN '' ELSE ':' || band.agi_band END
           AS source_object_key,
       measure.measure_id,
       measure.measure_label,
       measure.soi_column,
       measure.unit,
       band.agi_band,
       band.band_label,
       measure.parser_contract_version AS schema_version
FROM silver_irs_soi.dim_measure AS measure
CROSS JOIN silver_irs_soi.dim_agi_band AS band;

COMMENT ON SCHEMA gold_irs_soi IS
    'Policy-free publication views for IRS SOI individual income tax statistics by county and state.';

COMMENT ON VIEW gold_irs_soi.observation_latest IS
    'One SOI value per metric, place and tax year: county files first, the state file where they are silent, newest capture.';
