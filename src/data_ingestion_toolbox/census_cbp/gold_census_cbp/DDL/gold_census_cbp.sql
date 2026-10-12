-- Publication views over reconciled County Business Patterns observations
-- (census-county-business-patterns). Applied by the bootstrap manifest in
-- the `gold` phase and re-applied by the DAG's `ensure_census_cbp_schema`
-- task; this file is the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_census_cbp;

-- What each measure is, and what it never covers. Stated on every row so a
-- CBP count is not read as all employment: the self-employed, private
-- households, railroads, crop and animal production and most government
-- employees are out of scope.
CREATE OR REPLACE VIEW gold_census_cbp.measure_definition AS
SELECT measure.measure, measure.measure_label, measure.unit,
       'County Business Patterns: employer establishments in the private '
       || 'nonfarm economy, employment in the pay period including March 12; '
       || 'excludes the self-employed, private households, railroads, crop and '
       || 'animal production and most government employees'::TEXT AS observation_basis
FROM (VALUES
    ('est', 'Establishments', 'establishments'),
    ('emp', 'Employees in the pay period including March 12', 'employees'),
    ('qp1', 'First-quarter payroll', 'thousands of dollars'),
    ('ap', 'Annual payroll', 'thousands of dollars')
) AS measure(measure, measure_label, unit);

CREATE OR REPLACE VIEW gold_census_cbp.sector_definition AS
SELECT sector.naics_key, sector.naics_label
FROM (VALUES
    ('total', 'Total for all sectors'),
    ('11', 'Forestry, fishing, hunting, and agriculture support'),
    ('21', 'Mining, quarrying, and oil and gas extraction'),
    ('22', 'Utilities'),
    ('23', 'Construction'),
    ('31', 'Manufacturing'),
    ('42', 'Wholesale trade'),
    ('44', 'Retail trade'),
    ('48', 'Transportation and warehousing'),
    ('51', 'Information'),
    ('52', 'Finance and insurance'),
    ('53', 'Real estate and rental and leasing'),
    ('54', 'Professional, scientific, and technical services'),
    ('55', 'Management of companies and enterprises'),
    ('56', 'Administrative and support and waste management and remediation services'),
    ('61', 'Educational services'),
    ('62', 'Health care and social assistance'),
    ('71', 'Arts, entertainment, and recreation'),
    ('72', 'Accommodation and food services'),
    ('81', 'Other services (except public administration)'),
    ('99', 'Industries not classified')
) AS sector(naics_key, naics_label);

-- Every published capture of every observation. The files name no release,
-- so the release identity is the read; a corrected file is a second row with
-- its own capture, never an overwrite. The fact's CHECK admits only
-- `resolved` and `unmapped`; the predicate states the serving rule (DB-035).
CREATE OR REPLACE VIEW gold_census_cbp.observation_revision AS
SELECT fact.measure || ':' || fact.naics_key AS metric_key,
       fact.measure,
       measure.measure_label,
       measure.unit,
       measure.observation_basis,
       fact.naics_key,
       fact.naics_code,
       sector.naics_label,
       fact.year,
       MAKE_DATE(fact.year, 1, 1) AS period_start,
       MAKE_DATE(fact.year, 12, 31) AS period_end,
       fact.geo_id,
       fact.geo_sk,
       fact.geo_type,
       gold_glossary.geo_grain(fact.geo_type) AS geo_level,
       fact.geography_status,
       fact.value_source,
       fact.value,
       fact.value_status,
       fact.noise_flag,
       fact.employment_range,
       fact.retrieved_at,
       TO_CHAR(fact.retrieved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS release_key,
       fact.source_record_id,
       fact.capture_id,
       fact.run_id
FROM silver_census_cbp.fact_observation AS fact
JOIN gold_census_cbp.measure_definition AS measure ON measure.measure = fact.measure
JOIN gold_census_cbp.sector_definition AS sector ON sector.naics_key = fact.naics_key
JOIN control.census_cbp_file AS file ON file.capture_id = fact.capture_id
WHERE file.status = 'published'
  AND fact.geography_status NOT IN ('unsupported', 'ambiguous');

-- The newest published capture of each observation.
CREATE OR REPLACE VIEW gold_census_cbp.observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.year)
       revision.*
FROM gold_census_cbp.observation_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.year,
         revision.retrieved_at DESC, revision.capture_id DESC;

CREATE OR REPLACE VIEW gold_census_cbp.measure_export AS
SELECT measure.measure || ':' || sector.naics_key AS source_object_key,
       measure.measure,
       measure.measure_label,
       measure.unit,
       measure.observation_basis,
       sector.naics_key,
       sector.naics_label,
       'census_cbp_csv:v1'::TEXT AS schema_version
FROM gold_census_cbp.measure_definition AS measure
CROSS JOIN gold_census_cbp.sector_definition AS sector;

COMMENT ON SCHEMA gold_census_cbp IS
    'Policy-free publication views for Census County Business Patterns establishments, employment and payroll.';
