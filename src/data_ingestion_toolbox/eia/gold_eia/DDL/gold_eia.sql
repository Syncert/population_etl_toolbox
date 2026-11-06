-- Publication views over reconciled EIA retail gasoline prices
-- (grocery-and-gasoline-prices). Applied by the bootstrap manifest in the
-- `gold` phase and re-applied by the DAG's `ensure_eia_schema` task; this file
-- is the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_eia;

-- Every published read of every weekly price: the as-released surface. EIA
-- publishes no release identity, so a read is identified by the day it was
-- retrieved, and a week EIA revised is a second row beside the first. A
-- state EIA names that the reference cannot resolve has no identity and is
-- not served; it is ledgered in `silver_ref.geography_resolution`.
CREATE OR REPLACE VIEW gold_eia.observation_revision AS
SELECT fact.product AS metric_key,
       fact.product,
       CASE fact.product
           WHEN 'EPMR' THEN 'Regular gasoline'
           WHEN 'EPMM' THEN 'Midgrade gasoline'
           WHEN 'EPMP' THEN 'Premium gasoline'
           WHEN 'EPM0' THEN 'All grades gasoline'
       END AS grade,
       fact.series_id,
       fact.week_start,
       fact.week_start AS period_start,
       (fact.week_start + 6) AS period_end,
       fact.duoarea,
       fact.area_name,
       fact.geo_id,
       fact.geo_sk,
       fact.geo_type,
       gold_glossary.geo_grain(fact.geo_type) AS geo_level,
       fact.geography_status,
       'U.S. dollars per gallon'::TEXT AS unit,
       fact.value_source,
       fact.value,
       fact.value_status,
       fact.retrieved_at,
       fact.retrieved_at::DATE AS release_date,
       fact.retrieved_at::DATE::TEXT AS release_key,
       fact.source_record_id,
       fact.capture_id,
       fact.run_id,
       'gasoline'::TEXT AS route,
       'weekly'::TEXT AS frequency,
       FALSE AS outside_survey_season
FROM silver_eia.fact_retail_price AS fact
JOIN control.eia_read AS read ON read.run_id = fact.run_id
WHERE read.status = 'published'
  AND fact.geography_status NOT IN ('unsupported', 'ambiguous')
  AND fact.geo_id IS NOT NULL
UNION ALL
-- Home energy prices (home-heating-fuel-prices): residential electricity and
-- natural gas monthly, residential heating oil and propane weekly from
-- October to March, retail diesel weekly. A summer week of heating oil has
-- no row: not published, never zero.
SELECT fact.metric_key,
       fact.metric_key AS product,
       CASE fact.metric_key
           WHEN 'ELEC_RES' THEN 'Residential electricity'
           WHEN 'NG_RES' THEN 'Residential natural gas'
           WHEN 'EPD2F' THEN 'Residential heating oil'
           WHEN 'EPLLPA' THEN 'Residential propane'
           WHEN 'EPD2D' THEN 'On-highway diesel fuel'
       END AS grade,
       fact.series_id,
       fact.period_start AS week_start,
       fact.period_start,
       fact.period_end,
       fact.area_code AS duoarea,
       fact.area_name,
       fact.geo_id,
       fact.geo_sk,
       fact.geo_type,
       gold_glossary.geo_grain(fact.geo_type) AS geo_level,
       fact.geography_status,
       CASE fact.metric_key
           WHEN 'ELEC_RES' THEN 'cents per kilowatt-hour'
           WHEN 'NG_RES' THEN 'U.S. dollars per thousand cubic feet'
           ELSE 'U.S. dollars per gallon'
       END AS unit,
       fact.value_source,
       fact.value,
       fact.value_status,
       fact.retrieved_at,
       fact.retrieved_at::DATE AS release_date,
       fact.retrieved_at::DATE::TEXT AS release_key,
       fact.source_record_id,
       fact.capture_id,
       fact.run_id,
       fact.route,
       fact.frequency,
       fact.outside_survey_season
FROM silver_eia.fact_energy_price AS fact
JOIN control.eia_read AS read ON read.run_id = fact.run_id
WHERE read.status = 'published'
  AND fact.geography_status NOT IN ('unsupported', 'ambiguous')
  AND fact.geo_id IS NOT NULL;

-- The newest published read of each period's price. Written as "no newer
-- published read of the same series and period" rather than DISTINCT ON,
-- so a read pinned to one metric and area reaches each fact table through
-- its own index instead of sorting every price EIA has published.
CREATE OR REPLACE VIEW gold_eia.observation_latest AS
SELECT revision.*
FROM gold_eia.observation_revision AS revision
WHERE NOT EXISTS (
    SELECT 1
    FROM silver_eia.fact_retail_price AS newer
    JOIN control.eia_read AS read ON read.run_id = newer.run_id
    WHERE revision.route = 'gasoline'
      AND read.status = 'published'
      AND newer.series_id = revision.series_id
      AND newer.week_start = revision.week_start
      -- Only a newer read the revision view would serve supersedes one.
      AND newer.geography_status NOT IN ('unsupported', 'ambiguous')
      AND newer.geo_id IS NOT NULL
      AND (newer.retrieved_at, newer.capture_id) > (revision.retrieved_at, revision.capture_id)
)
AND NOT EXISTS (
    SELECT 1
    FROM silver_eia.fact_energy_price AS newer
    JOIN control.eia_read AS read ON read.run_id = newer.run_id
    WHERE revision.route <> 'gasoline'
      AND read.status = 'published'
      AND newer.series_id = revision.series_id
      AND newer.period_start = revision.period_start
      -- Only a newer read the revision view would serve supersedes one.
      AND newer.geography_status NOT IN ('unsupported', 'ambiguous')
      AND newer.geo_id IS NOT NULL
      AND (newer.retrieved_at, newer.capture_id) > (revision.retrieved_at, revision.capture_id)
);

CREATE OR REPLACE VIEW gold_eia.measure_export AS
SELECT DISTINCT fact.product AS source_object_key,
       fact.product,
       'U.S. dollars per gallon'::TEXT AS unit
FROM silver_eia.fact_retail_price AS fact
UNION
SELECT DISTINCT fact.metric_key AS source_object_key,
       fact.metric_key AS product,
       CASE fact.metric_key
           WHEN 'ELEC_RES' THEN 'cents per kilowatt-hour'
           WHEN 'NG_RES' THEN 'U.S. dollars per thousand cubic feet'
           ELSE 'U.S. dollars per gallon'
       END AS unit
FROM silver_eia.fact_energy_price AS fact;

COMMENT ON SCHEMA gold_eia IS
    'Policy-free publication views for EIA retail gasoline, diesel and home energy prices.';
