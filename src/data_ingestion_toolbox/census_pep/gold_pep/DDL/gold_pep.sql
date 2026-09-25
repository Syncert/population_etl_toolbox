CREATE SCHEMA IF NOT EXISTS gold_pep;

CREATE OR REPLACE VIEW gold_pep.population_estimate_revision AS
WITH ranked AS (
    SELECT fact.*,
        capture.retrieved_at AS source_retrieved_at,
        ROW_NUMBER() OVER (
            PARTITION BY fact.dataset_code, fact.release_vintage,
                fact.metric_code, fact.geo_id, fact.observation_year
            ORDER BY capture.retrieved_at DESC, fact.capture_id DESC
        ) AS capture_rank
    FROM silver_pep.fact_population_estimate AS fact
    JOIN silver_pep.release_load AS load USING (capture_id)
    JOIN raw_capture.response_capture AS capture USING (capture_id)
    WHERE load.completeness_status = 'complete'
      AND fact.resolution_status = 'resolved'
)
SELECT capture_id, dataset_code, release_vintage AS pep_vintage,
    product_code, metric_code, observation_year, estimate_date,
    geo_id, geo_sk, geo_type, geography_basis_date, summary_level,
    source_name, functional_status_source, value_source, value, unit,
    source_retrieved_at
FROM ranked
WHERE capture_rank = 1;

-- The currently published value for one measure, geography and year --
-- exactly one row, chosen across every product that publishes it.
--
-- Ranking within a dataset was enough while the 2020s were the only decade,
-- but PEP publishes overlapping products: the state file and the county file
-- both carry state rows, and consecutive decades both carry their shared
-- seam year. Partitioning by dataset kept every one of them, so a state read
-- answered twice per period and a seam year would answer once per decade.
--
-- Precedence, in order:
--   1. An intercensal series beats a postcensal one for the same year. It is
--      the Bureau's closing word on a decade, computed against both
--      enumerations, where a postcensal estimate only had the opening one.
--   2. Otherwise the newest vintage, which is the latest revision.
--   3. Otherwise the product that publishes this geography in its own right
--      rather than as a rollup: a state row from the state file beats the
--      same state row summed into the county file.
--   4. Otherwise the dataset code, so the choice is deterministic rather
--      than left to whichever row the planner returned last.
CREATE OR REPLACE VIEW gold_pep.population_estimate_latest AS
SELECT capture_id, dataset_code, pep_vintage, product_code, metric_code,
    observation_year, estimate_date, geo_id, geo_sk, geo_type,
    geography_basis_date, summary_level, source_name,
    functional_status_source, value_source, value, unit,
    source_retrieved_at
FROM (
    SELECT revision.*,
        ROW_NUMBER() OVER (
            PARTITION BY revision.metric_code, revision.geo_id,
                revision.observation_year
            ORDER BY
                CASE WHEN dataset.series_kind = 'intercensal' THEN 0 ELSE 1 END,
                revision.pep_vintage DESC,
                CASE
                    WHEN revision.summary_level = dataset.native_grain THEN 0
                    ELSE 1
                END,
                revision.dataset_code
        ) AS vintage_rank
    FROM gold_pep.population_estimate_revision AS revision
    JOIN silver_pep.pep_dataset AS dataset
      ON dataset.dataset_code = revision.dataset_code
) AS ranked
WHERE vintage_rank = 1;

CREATE OR REPLACE VIEW gold_pep.population_change AS
SELECT *, FALSE AS is_derived
FROM gold_pep.population_estimate_revision
WHERE metric_code IN ('NPOPCHG', 'NATURALCHG', 'NETMIG');

CREATE OR REPLACE VIEW gold_pep.rpt_pep_observations AS
SELECT revision.capture_id,
    'CENSUS_PEP'::TEXT AS source_code,
    revision.estimate_date AS observation_date,
    revision.estimate_date::TEXT AS period,
    revision.estimate_date AS duration_start,
    revision.estimate_date AS duration_end,
    time.time_sk,
    release.release_date AS as_of_date,
    revision.source_retrieved_at AS updated_at,
    revision.geo_id,
    revision.geo_type AS geo_level,
    entity.state_fips,
    entity.county_fips,
    entity.place_fips,
    current.state_name,
    current.county_name,
    current.place_name,
    current.latitude AS geo_latitude,
    current.longitude AS geo_longitude,
    'CENSUS_PEP:' || revision.dataset_code || ':' || revision.metric_code AS metric_code,
    measure.display_name AS metric_display_name,
    revision.value,
    measure.value_type,
    revision.unit AS units,
    NULL::TEXT AS seasonal_adjustment_status,
    revision.dataset_code,
    revision.pep_vintage AS vintage_year,
    NULL::NUMERIC AS margin_of_error,
    NULL::NUMERIC AS margin_of_error_pct
FROM gold_pep.population_estimate_revision AS revision
JOIN silver_pep.dim_measure AS measure USING (metric_code)
JOIN silver_pep.pep_release AS release
  ON release.dataset_code = revision.dataset_code
 AND release.vintage_year = revision.pep_vintage
JOIN silver_ref.dim_geo_entity AS entity USING (geo_sk)
LEFT JOIN silver_ref.dim_geo_current AS current USING (geo_sk)
LEFT JOIN silver_ref.dim_time AS time ON time.date_key = revision.estimate_date;

CREATE OR REPLACE VIEW gold_pep.mv_pep_latest AS
SELECT reporting.*
FROM gold_pep.rpt_pep_observations AS reporting
JOIN gold_pep.population_estimate_latest AS latest
  ON latest.capture_id = reporting.capture_id
 AND latest.dataset_code = reporting.dataset_code
 AND latest.pep_vintage = reporting.vintage_year
 AND ('CENSUS_PEP:' || latest.dataset_code || ':' || latest.metric_code) = reporting.metric_code
 AND latest.geo_id = reporting.geo_id
 AND latest.observation_year = EXTRACT(YEAR FROM reporting.observation_date)::INTEGER;

CREATE OR REPLACE VIEW gold_pep.measure_export AS
SELECT measure.metric_code AS source_object_key,
    measure.display_name AS metric_display_name, measure.unit,
    measure.is_component, measure.allows_negative,
    measure.population_universe,
    -- Grains are the ones the served relation carries, in the vocabulary the
    -- API filters on. Read from the silver fact this also published
    -- 'unsupported' -- 1.6M rows whose geography never resolved and which the
    -- served relations exclude. A resolution status is not a grain: nothing
    -- can be asked for it. The mapping is the gold_glossary geo_grain
    -- function, written inline here because this file is re-applied by the PEP DAG
    -- ahead of the glossary phase that defines the function, and DB-028
    -- holds the outcome to the same vocabulary either way.
    COALESCE(served.valid_geo_grains, ARRAY[]::TEXT[]) AS valid_geo_grains,
    MAX(fact.transformed_at) AS publication_time,
    -- Coverage is per measure, not per source: the Bureau published births
    -- for the 1980s onward and migration components only from 2000, so a
    -- single "PEP starts in 1970" would be wrong for most of the catalog.
    -- Read from the facts rather than declared, so a decade that fails to
    -- load narrows the published range instead of overstating it. Appended
    -- rather than inserted so CREATE OR REPLACE VIEW keeps working.
    MIN(fact.estimate_date) AS first_period,
    MAX(fact.estimate_date) AS last_period
FROM silver_pep.dim_measure AS measure
JOIN silver_pep.fact_population_estimate AS fact USING (metric_code)
LEFT JOIN (
    -- Aggregated first, then joined once per measure. Joining the served
    -- relation row-for-row beside the silver fact multiplies the two per
    -- metric -- a cross product over millions of rows -- which is exactly
    -- what the first cut of this did.
    -- Through `gold_glossary.geo_grain`, which is this CASE: it mapped NATION
    -- to NATIONAL and upper-cased the rest, one more copy of the vocabulary
    -- migration 018 exists to hold (DB-037). 021 defines the function ahead
    -- of this phase so the call is legal at bootstrap.
    SELECT revision.metric_code,
           -- A grain is one a value is published at: a grain where every row is
           -- withheld (Census publishes B24114 nationally only) is no map to offer.
           COALESCE(
               ARRAY_AGG(DISTINCT gold_glossary.geo_grain(revision.geo_type)
                         ORDER BY gold_glossary.geo_grain(revision.geo_type))
                   FILTER (WHERE revision.value IS NOT NULL),
               ARRAY[]::TEXT[]
           )::TEXT[]
               AS valid_geo_grains
    FROM gold_pep.population_estimate_revision AS revision
    GROUP BY revision.metric_code
) AS served ON served.metric_code = measure.metric_code
GROUP BY measure.metric_code, served.valid_geo_grains;

CREATE OR REPLACE VIEW gold_pep.metric_publisher AS
SELECT 'CENSUS_PEP'::TEXT AS source_code,
    '1.0'::TEXT AS publisher_contract_version,
    export.source_object_key::TEXT AS source_object_key,
    'measure'::TEXT AS source_object_type,
    export.metric_display_name::TEXT AS metric_display_name,
    export.unit::TEXT AS units,
    CASE WHEN export.is_component THEN 'component' ELSE 'level' END::TEXT AS measure_kind,
    -- The export already publishes the vocabulary; going through the one
    -- mapping again is idempotent and says which vocabulary this is.
    ARRAY(
        SELECT gold_glossary.geo_grain(value)
        FROM UNNEST(export.valid_geo_grains) AS value
    )::TEXT[] AS valid_geo_grains,
    ARRAY['ANNUAL']::TEXT[] AS valid_time_grains,
    NULL::TEXT AS aggregation_characteristic,
    JSONB_BUILD_OBJECT(
        'schema', 'gold_pep', 'relation', 'population_estimate_revision',
        'key', export.source_object_key
    ) AS physical_lineage,
    export.publication_time::TEXT AS source_watermark,
    NULL::UUID AS source_run_id,
    export.publication_time,
    'U.S. Census Bureau Population Estimates Program'::TEXT AS source_name,
    'government-statistical-program'::TEXT AS source_type,
    'https://www.census.gov/programs-surveys/popest.html'::TEXT AS reference_url
FROM gold_pep.measure_export AS export;
