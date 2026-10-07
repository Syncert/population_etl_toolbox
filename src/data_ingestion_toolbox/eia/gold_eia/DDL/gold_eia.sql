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
       fact.run_id
FROM silver_eia.fact_retail_price AS fact
JOIN control.eia_read AS read ON read.run_id = fact.run_id
WHERE read.status = 'published'
  AND fact.geography_status NOT IN ('unsupported', 'ambiguous')
  AND fact.geo_id IS NOT NULL;

-- The newest read of each week's price.
CREATE OR REPLACE VIEW gold_eia.observation_latest AS
SELECT DISTINCT ON (revision.series_id, revision.week_start)
       revision.*
FROM gold_eia.observation_revision AS revision
ORDER BY revision.series_id, revision.week_start,
         revision.retrieved_at DESC, revision.capture_id DESC;

CREATE OR REPLACE VIEW gold_eia.measure_export AS
SELECT DISTINCT fact.product AS source_object_key,
       fact.product,
       'U.S. dollars per gallon'::TEXT AS unit
FROM silver_eia.fact_retail_price AS fact;

COMMENT ON SCHEMA gold_eia IS
    'Policy-free publication views for EIA weekly retail gasoline prices.';
