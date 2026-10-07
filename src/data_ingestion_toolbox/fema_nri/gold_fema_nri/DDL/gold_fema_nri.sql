-- Publication views over the FEMA National Risk Index and disaster
-- declarations (fema-nri-declarations). Applied by the bootstrap manifest in
-- the `gold` phase and re-applied by the DAG's `ensure_fema_nri_schema` task;
-- this file is the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_fema_nri;

-- The published measures. Expected annual loss is FEMA's modelled estimate
-- (annualized frequency x exposure x historic loss ratio), in dollars per
-- year, with no margin of error; the NRI's risk scores, ratings, social
-- vulnerability and resilience are relative ranks and are not published.
-- A declaration count is this warehouse's count of FEMA's declarations.
CREATE OR REPLACE VIEW gold_fema_nri.measure_definition AS
WITH hazard (prefix, hazard_key, hazard_name) AS (
    VALUES
        ('AVLN', 'avalanche', 'Avalanche'),
        ('CFLD', 'coastal_flooding', 'Coastal Flooding'),
        ('CWAV', 'cold_wave', 'Cold Wave'),
        ('DRGT', 'drought', 'Drought'),
        ('ERQK', 'earthquake', 'Earthquake'),
        ('HAIL', 'hail', 'Hail'),
        ('HWAV', 'heat_wave', 'Heat Wave'),
        ('HRCN', 'hurricane', 'Hurricane'),
        ('ISTM', 'ice_storm', 'Ice Storm'),
        ('LNDS', 'landslide', 'Landslide'),
        ('LTNG', 'lightning', 'Lightning'),
        ('IFLD', 'inland_flooding', 'Inland Flooding'),
        ('SWND', 'strong_wind', 'Strong Wind'),
        ('TRND', 'tornado', 'Tornado'),
        ('TSUN', 'tsunami', 'Tsunami'),
        ('VLCN', 'volcanic_activity', 'Volcanic Activity'),
        ('WFIR', 'wildfire', 'Wildfire'),
        ('WNTW', 'winter_weather', 'Winter Weather')
),
nri_basis (text) AS (
    VALUES (
        'FEMA National Risk Index: a modelled estimate for planning, not a measurement; '
        || 'expected annual loss is annualized frequency times exposure times historic loss '
        || 'ratio, in dollars per year, with no margin of error, and methods change between '
        || 'versions. National Risk Index data: Federal Emergency Management Agency, FEMA '
        || 'National Risk Index.'
    )
)
SELECT 'expected_annual_loss'::TEXT AS measure,
       'Expected Annual Loss - Total - Composite'::TEXT AS measure_label,
       'dollars per year'::TEXT AS unit, 'nri'::TEXT AS stream, nri_basis.text AS observation_basis
FROM nri_basis
UNION ALL
SELECT 'expected_annual_loss_' || hazard.hazard_key,
       hazard.hazard_name || ' - Expected Annual Loss - Total',
       'dollars per year', 'nri', nri_basis.text
FROM hazard CROSS JOIN nri_basis
UNION ALL
SELECT 'annualized_frequency_' || hazard.hazard_key,
       hazard.hazard_name || ' - Annualized Frequency',
       'events per year', 'nri', nri_basis.text
FROM hazard CROSS JOIN nri_basis
WHERE hazard.prefix IN ('IFLD', 'TRND', 'WFIR', 'HRCN', 'HWAV')
UNION ALL
SELECT declaration.measure, declaration.label, 'declarations', 'declarations',
       'FEMA disaster declarations (OpenFEMA DisasterDeclarationsSummaries): the number of '
       || 'distinct declarations of this type that designated this county, by the calendar '
       || 'year of the declaration date; a statewide or tribal-area designation is not '
       || 'counted toward a county, and a year with no declaration has no row. This product '
       || 'uses the Federal Emergency Management Agency''s OpenFEMA API, but is not endorsed '
       || 'by FEMA.'
FROM (VALUES
    ('major_disaster_declarations', 'Major disaster declarations', 'DR'),
    ('emergency_declarations', 'Emergency declarations', 'EM'),
    ('fire_management_declarations', 'Fire management assistance declarations', 'FM')
) AS declaration(measure, label, declaration_type);

-- Every published NRI read of every county field, keyed by the NRI version
-- (`December 2025`); a field whose rating says it is not a measurement is
-- served with its status and no number.
CREATE OR REPLACE VIEW gold_fema_nri.nri_observation AS
SELECT fact.measure AS metric_key,
       fact.measure,
       fact.field,
       fact.geo_id,
       fact.geo_sk,
       fact.geography_status,
       RIGHT(fact.nri_version, 4)::INTEGER AS year,
       fact.nri_version AS release_key,
       fact.value_source,
       fact.value,
       fact.value_status,
       fact.missing_reason,
       fact.rating,
       fact.source_record_id,
       fact.capture_id,
       fact.run_id,
       run.published_at
FROM silver_fema_nri.nri_fact AS fact
JOIN control.fema_nri_run AS run ON run.run_id = fact.run_id
WHERE run.status = 'published' AND RIGHT(fact.nri_version, 4) ~ '^[0-9]{4}$';

-- The newest revision of every declaration area row a published read has
-- seen, counted per county, year and type: the count is of distinct
-- declarations, so a declaration that designated a county twice counts once.
CREATE OR REPLACE VIEW gold_fema_nri.declaration_count AS
WITH current_revision AS (
    SELECT DISTINCT ON (revision.declaration_id) revision.*
    FROM silver_fema_nri.declaration_revision AS revision
    JOIN control.fema_nri_run AS run ON run.run_id = revision.run_id
    WHERE run.status = 'published'
    ORDER BY revision.declaration_id, revision.last_refresh DESC, revision.created_at DESC
),
latest_read AS (
    SELECT MAX(published_at) AS published_at FROM control.fema_nri_run
    WHERE stream = 'declarations' AND status = 'published'
)
SELECT CASE current_revision.declaration_type
           WHEN 'DR' THEN 'major_disaster_declarations'
           WHEN 'EM' THEN 'emergency_declarations'
           ELSE 'fire_management_declarations'
       END AS metric_key,
       current_revision.geo_id,
       MIN(current_revision.geo_sk) AS geo_sk,
       EXTRACT(YEAR FROM current_revision.declaration_date)::INTEGER AS year,
       COUNT(DISTINCT current_revision.disaster_number) AS value,
       STRING_AGG(DISTINCT current_revision.declaration_string, ', '
                  ORDER BY current_revision.declaration_string) AS declarations,
       MAX(current_revision.last_refresh) AS last_refresh,
       MIN(current_revision.capture_id::TEXT)::UUID AS capture_id,
       MIN(current_revision.run_id::TEXT)::UUID AS run_id,
       (SELECT published_at FROM latest_read) AS published_at
FROM current_revision
WHERE current_revision.geography_status = 'resolved'
GROUP BY 1, current_revision.geo_id, 4;

-- Both streams in the neutral observation shape. The declaration release is
-- the newest OpenFEMA refresh the warehouse has read. The predicate states
-- the serving rule (DB-035).
CREATE OR REPLACE VIEW gold_fema_nri.observation_revision AS
SELECT observation.metric_key,
       definition.measure_label,
       definition.unit,
       definition.observation_basis,
       observation.year,
       MAKE_DATE(observation.year, 1, 1) AS period_start,
       MAKE_DATE(observation.year, 12, 31) AS period_end,
       observation.geo_id,
       observation.geo_sk,
       'county'::TEXT AS geo_type,
       gold_glossary.geo_grain('county') AS geo_level,
       observation.value,
       observation.value_status,
       observation.missing_reason,
       observation.rating,
       observation.declarations,
       observation.release_key,
       observation.published_at,
       observation.source_record_id,
       observation.capture_id,
       observation.run_id
FROM (
    SELECT nri.metric_key, nri.year, nri.geo_id, nri.geo_sk, nri.geography_status, nri.value,
           nri.value_status, nri.missing_reason, nri.rating, NULL::TEXT AS declarations,
           nri.release_key, nri.published_at, nri.source_record_id, nri.capture_id, nri.run_id
    FROM gold_fema_nri.nri_observation AS nri
    UNION ALL
    SELECT counted.metric_key, counted.year, counted.geo_id, counted.geo_sk, 'resolved',
           counted.value::NUMERIC, 'valid', NULL, NULL, counted.declarations,
           'OpenFEMA ' || TO_CHAR(counted.last_refresh AT TIME ZONE 'UTC', 'YYYY-MM-DD'),
           counted.published_at,
           md5(counted.metric_key || '|' || counted.geo_id || '|' || counted.year::TEXT),
           counted.capture_id, counted.run_id
    FROM gold_fema_nri.declaration_count AS counted
) AS observation
JOIN gold_fema_nri.measure_definition AS definition ON definition.measure = observation.metric_key
WHERE observation.geography_status NOT IN ('unsupported', 'ambiguous', 'unmapped');

-- The newest release of each observation.
CREATE OR REPLACE VIEW gold_fema_nri.observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.year)
       revision.*
FROM gold_fema_nri.observation_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.year,
         revision.published_at DESC, revision.run_id DESC;

CREATE OR REPLACE VIEW gold_fema_nri.measure_export AS
SELECT measure AS source_object_key, measure, measure_label, unit, stream, observation_basis,
       'fema_nri_json:v1'::TEXT AS schema_version
FROM gold_fema_nri.measure_definition;

COMMENT ON SCHEMA gold_fema_nri IS
    'Policy-free publication views for FEMA National Risk Index expected annual losses and county disaster declaration counts.';
