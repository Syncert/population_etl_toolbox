-- Publication views over reconciled USDA ERS county codes and atlas
-- indicators (usda-ers-county-codes-and-atlases). Applied by the bootstrap
-- manifest in the `gold` phase and re-applied by the DAG's
-- `ensure_usda_ers_schema` task; this file is the only definition of these
-- views.

CREATE SCHEMA IF NOT EXISTS gold_usda_ers;

-- The published measures. A classification is a code, not a quantity: it is
-- served as its number with ERS's label where ERS gives one, and nothing
-- sums or averages it. ERS states its editions are not comparable across
-- methodological changes; each measure names its edition.
CREATE OR REPLACE VIEW gold_usda_ers.measure_definition AS
SELECT measure.measure, measure.measure_label, measure.unit, measure.product, measure.kind,
       CASE measure.product
           WHEN 'rucc' THEN
               'USDA ERS Rural-Urban Continuum Codes, 2023 edition: a classification '
               || 'of counties by metro-area size and, for nonmetro counties, urban '
               || 'population and adjacency to a metro area (1-3 metro, 4-9 nonmetro); '
               || 'a code, not a quantity, and not comparable to earlier editions. '
               || 'Source: USDA, Economic Research Service.'
           WHEN 'typology' THEN
               'USDA ERS County Typology Codes, 2025 edition: a flag (1 yes, 0 no) or, '
               || 'for industry dependence, a code; not comparable to earlier editions. '
               || 'Source: USDA, Economic Research Service.'
           ELSE
               'USDA ERS Food Environment Atlas, July 2025 release: the indicator as '
               || 'ERS publishes it for the year shown. Source: USDA, Economic Research '
               || 'Service.'
       END::TEXT AS observation_basis
FROM (VALUES
    ('rural_urban_continuum_code', 'Rural-Urban Continuum Code', 'code (1-9)', 'rucc', 'code'),
    ('farming_dependent', 'Farming-dependent county', 'flag (0/1)', 'typology', 'flag'),
    ('mining_dependent', 'Mining-dependent county', 'flag (0/1)', 'typology', 'flag'),
    ('manufacturing_dependent', 'Manufacturing-dependent county', 'flag (0/1)', 'typology', 'flag'),
    ('government_dependent', 'Federal/State government-dependent county', 'flag (0/1)', 'typology', 'flag'),
    ('recreation_dependent', 'Recreation county', 'flag (0/1)', 'typology', 'flag'),
    ('nonspecialized', 'Nonspecialized economy', 'flag (0/1)', 'typology', 'flag'),
    ('industry_dependence', 'Industry dependence (which of five industries, 0 for none)', 'code (0-5)', 'typology', 'code'),
    ('low_postsecondary_education', 'Low postsecondary education', 'flag (0/1)', 'typology', 'flag'),
    ('low_employment', 'Low employment', 'flag (0/1)', 'typology', 'flag'),
    ('population_loss', 'Population loss', 'flag (0/1)', 'typology', 'flag'),
    ('housing_stress', 'Housing stress', 'flag (0/1)', 'typology', 'flag'),
    ('retirement_destination', 'Retirement destination', 'flag (0/1)', 'typology', 'flag'),
    ('persistent_poverty', 'Persistent poverty (2017-21 and earlier)', 'flag (0/1)', 'typology', 'flag'),
    ('snap_authorized_stores', 'SNAP-authorized stores', 'stores', 'fea', 'number'),
    ('snap_authorized_stores_per_1000', 'SNAP-authorized stores per 1,000 people', 'stores per 1,000 people', 'fea', 'number'),
    ('snap_households_low_store_access', 'SNAP households with low access to a store', 'households', 'fea', 'number'),
    ('snap_households_low_store_access_pct', 'SNAP households with low access to a store, percent of households', 'percent', 'fea', 'number')
) AS measure(measure, measure_label, unit, product, kind);

-- Every published capture of every observation. The release identity is the
-- product and edition, plus the read time when ERS replaces a file in place;
-- a replaced file is a second row with its own capture, never an overwrite.
-- The fact's CHECK admits only `resolved` and `unmapped`; the predicate
-- states the serving rule (DB-035).
CREATE OR REPLACE VIEW gold_usda_ers.observation_revision AS
SELECT fact.measure AS metric_key,
       fact.measure,
       measure.measure_label,
       measure.unit,
       measure.kind,
       measure.observation_basis,
       fact.product,
       fact.edition,
       fact.attribute,
       fact.year,
       MAKE_DATE(fact.year, 1, 1) AS period_start,
       MAKE_DATE(fact.year, 12, 31) AS period_end,
       fact.geo_id,
       fact.geo_sk,
       'county'::TEXT AS geo_type,
       gold_glossary.geo_grain('county') AS geo_level,
       fact.geography_status,
       fact.fips_code,
       fact.value_source,
       fact.value,
       fact.value_status,
       fact.missing_reason,
       fact.code_label,
       fact.product || ':' || fact.edition || ':'
           || TO_CHAR(fact.retrieved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS release_key,
       file.published_at,
       fact.retrieved_at,
       fact.source_record_id,
       fact.capture_id,
       fact.run_id
FROM silver_usda_ers.fact_observation AS fact
JOIN control.usda_ers_file AS file ON file.run_id = fact.run_id
JOIN gold_usda_ers.measure_definition AS measure ON measure.measure = fact.measure
WHERE file.status = 'published'
  AND fact.geography_status NOT IN ('unsupported', 'ambiguous', 'unmapped');

-- The newest published capture of each observation.
CREATE OR REPLACE VIEW gold_usda_ers.observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.year)
       revision.*
FROM gold_usda_ers.observation_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.year,
         revision.retrieved_at DESC, revision.capture_id DESC;

CREATE OR REPLACE VIEW gold_usda_ers.measure_export AS
SELECT measure AS source_object_key, measure, measure_label, unit, kind, observation_basis,
       'usda_ers_csv:v1'::TEXT AS schema_version
FROM gold_usda_ers.measure_definition;

COMMENT ON SCHEMA gold_usda_ers IS
    'Policy-free publication views for USDA ERS county classifications and Food Environment Atlas indicators.';
