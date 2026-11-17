-- Publication views over reconciled NCES IPEDS files
-- (college-housing-capacity). Applied by the bootstrap manifest in the `gold`
-- phase and re-applied by the DAG's `ensure_nces_ipeds_schema` task; this
-- file is the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_nces_ipeds;

-- Every published file's area sums: the as-released surface. Each sum adds
-- the published figures of the institutions that year's directory places in
-- the area; the period is the IPEDS collection year (housing capacity for
-- that academic year, enrolment in its fall). A sum for an area the shared
-- geography does not hold is served as `unmapped` (DB-003).
CREATE OR REPLACE VIEW gold_nces_ipeds.area_revision AS
SELECT area.measure_id,
       'ipeds:' || area.measure_id AS metric_key,
       measure.measure_label,
       measure.unit,
       measure.basis,
       area.collection_year AS period_year,
       file.file_stem,
       file.csv_member,
       area.geo_id,
       area.geo_sk,
       area.geo_type,
       gold_glossary.geo_grain(area.geo_type) AS geo_level,
       area.geography_status,
       area.value,
       'valid'::TEXT AS value_status,
       area.institution_count,
       area.unplaced_institution_count,
       'sum_of_published_institution_figures'::TEXT AS derivation,
       capture.retrieved_at,
       TO_CHAR(capture.retrieved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS release_key,
       area.source_record_id,
       file.capture_id,
       area.run_id
FROM silver_nces_ipeds.area_total AS area
JOIN control.nces_ipeds_file AS file ON file.run_id = area.run_id
JOIN raw_capture.response_capture AS capture ON capture.capture_id = file.capture_id
JOIN silver_nces_ipeds.dim_measure AS measure ON measure.measure_id = area.measure_id
WHERE file.status = 'published'
  AND area.geography_status NOT IN ('unsupported', 'ambiguous');

-- The newest published capture of each area sum for each year.
CREATE OR REPLACE VIEW gold_nces_ipeds.area_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.period_year)
       revision.*
FROM gold_nces_ipeds.area_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.period_year,
         revision.retrieved_at DESC, revision.capture_id DESC;

-- Every institution and year with its directory entry and its figures, from
-- the newest published capture of each file. A figure IPEDS marks not
-- applicable or not reported keeps its status and has no value.
CREATE OR REPLACE VIEW gold_nces_ipeds.institution_year AS
WITH directory AS (
    SELECT DISTINCT ON (institution.unitid, institution.collection_year)
           institution.*
    FROM silver_nces_ipeds.institution AS institution
    JOIN control.nces_ipeds_file AS file ON file.run_id = institution.run_id
    WHERE file.status = 'published'
    ORDER BY institution.unitid, institution.collection_year, file.published_at DESC
), figure AS (
    SELECT DISTINCT ON (value.unitid, value.collection_year, value.measure_id)
           value.unitid, value.collection_year, value.measure_id, value.value,
           value.value_status, value.imputation_code
    FROM silver_nces_ipeds.institution_value AS value
    JOIN control.nces_ipeds_file AS file ON file.run_id = value.run_id
    WHERE file.status = 'published'
    ORDER BY value.unitid, value.collection_year, value.measure_id, file.published_at DESC
)
SELECT directory.unitid,
       directory.collection_year AS period_year,
       directory.institution_name,
       directory.city,
       directory.state_code,
       directory.state_fips,
       directory.zip_code,
       directory.county_code,
       directory.county_name,
       directory.county_geo_id,
       directory.county_status,
       directory.longitude,
       directory.latitude,
       directory.sector,
       capacity.value AS housing_capacity,
       COALESCE(capacity.value_status, 'not_listed') AS housing_capacity_status,
       total.value AS fall_enrolment_total,
       COALESCE(total.value_status, 'not_listed') AS fall_enrolment_total_status,
       full_time.value AS fall_enrolment_full_time,
       part_time.value AS fall_enrolment_part_time
FROM directory
LEFT JOIN figure AS capacity
  ON capacity.unitid = directory.unitid AND capacity.collection_year = directory.collection_year
 AND capacity.measure_id = 'housing_capacity'
LEFT JOIN figure AS total
  ON total.unitid = directory.unitid AND total.collection_year = directory.collection_year
 AND total.measure_id = 'fall_enrolment_total'
LEFT JOIN figure AS full_time
  ON full_time.unitid = directory.unitid AND full_time.collection_year = directory.collection_year
 AND full_time.measure_id = 'fall_enrolment_full_time'
LEFT JOIN figure AS part_time
  ON part_time.unitid = directory.unitid AND part_time.collection_year = directory.collection_year
 AND part_time.measure_id = 'fall_enrolment_part_time';

CREATE OR REPLACE VIEW gold_nces_ipeds.measure_export AS
SELECT measure.component_id AS source_dataset,
       measure.measure_id AS source_measure_code,
       measure.measure_label AS display_name,
       measure.unit,
       measure.basis,
       measure.source_column,
       measure.source_level,
       measure.parser_contract_version AS schema_version
FROM silver_nces_ipeds.dim_measure AS measure;

COMMENT ON SCHEMA gold_nces_ipeds IS
    'Policy-free publication views for NCES IPEDS institutions, housing capacity and fall enrolment.';

COMMENT ON VIEW gold_nces_ipeds.area_latest IS
    'Sums of the published figures of the institutions IPEDS places in each area, for each collection year.';
