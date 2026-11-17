-- Publication views over the published HHS hospital capacity snapshot
-- (hospital-capacity-and-locations). Applied by the bootstrap manifest in the
-- `gold` phase and re-applied by the DAG's `ensure_hhs_hospital_capacity_schema`
-- task; this file is the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_hhs_hospital_capacity;

-- Every published snapshot's area sums: the as-released surface. Each sum
-- adds the published figures of the hospitals HHS places in the area for the
-- collection week (Sunday to Saturday), with the count of hospitals that
-- reported a value, of cells HHS suppressed and of cells the file omits. A
-- sum for an area the shared geography does not hold is served as
-- `unmapped` (DB-003).
CREATE OR REPLACE VIEW gold_hhs_hospital_capacity.area_revision AS
SELECT area.measure_id,
       'hospital:' || area.measure_id AS metric_key,
       measure.measure_label,
       measure.unit,
       measure.basis,
       area.collection_week AS period_start,
       (area.collection_week + 6) AS period_end,
       area.geo_id,
       area.geo_sk,
       area.geo_type,
       gold_glossary.geo_grain(area.geo_type) AS geo_level,
       area.geography_status,
       area.value,
       area.value_status,
       area.facility_count,
       area.reporting_facility_count,
       area.suppressed_count,
       area.missing_count,
       area.unplaced_facility_count,
       'sum_of_published_facility_figures'::TEXT AS derivation,
       capture.retrieved_at,
       TO_CHAR(capture.retrieved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS release_key,
       area.source_record_id,
       snapshot.metadata_capture_id AS capture_id,
       snapshot.metadata_run_id AS run_id,
       snapshot.snapshot_id,
       snapshot.published_at
FROM silver_hhs_hospital_capacity.area_week AS area
JOIN control.hhs_hospital_capacity_snapshot AS snapshot ON snapshot.snapshot_id = area.snapshot_id
JOIN raw_capture.response_capture AS capture ON capture.capture_id = snapshot.metadata_capture_id
JOIN silver_hhs_hospital_capacity.dim_measure AS measure ON measure.measure_id = area.measure_id
WHERE snapshot.status = 'published'
  AND area.geography_status NOT IN ('unsupported', 'ambiguous');

-- The newest published snapshot's sum of each area, week and figure.
CREATE OR REPLACE VIEW gold_hhs_hospital_capacity.area_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.period_start)
       revision.*
FROM gold_hhs_hospital_capacity.area_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.period_start,
         revision.published_at DESC, revision.snapshot_id DESC;

-- Every hospital of the newest published snapshot, as its newest week
-- describes it, with where it is placed and why.
CREATE OR REPLACE VIEW gold_hhs_hospital_capacity.facility_latest AS
SELECT facility.hospital_pk,
       facility.ccn,
       facility.hospital_name,
       facility.hospital_subtype,
       facility.street_address,
       facility.city,
       facility.zip_code,
       facility.state_code,
       facility.state_fips,
       facility.fips_code,
       facility.county_geo_id,
       facility.county_status,
       CASE facility.county_status
           WHEN 'resolved' THEN 'HHS published county FIPS code'
           WHEN 'unmapped' THEN 'HHS published county FIPS code not in the shared geography or not of the hospital''s state'
           ELSE 'HHS published no county FIPS code'
       END AS placement_source,
       facility.longitude,
       facility.latitude,
       facility.is_metro_micro,
       facility.first_week,
       facility.last_week,
       facility.week_count,
       facility.snapshot_id
FROM silver_hhs_hospital_capacity.facility AS facility
WHERE facility.snapshot_id = (
    SELECT snapshot.snapshot_id FROM control.hhs_hospital_capacity_snapshot AS snapshot
    WHERE snapshot.status = 'published'
    ORDER BY snapshot.published_at DESC LIMIT 1
);

-- Each hospital's weekly figures from the newest published snapshot, one row
-- per week, every figure with its status (`suppressed` where HHS wrote
-- -999999, `missing` where the file omits it).
CREATE OR REPLACE VIEW gold_hhs_hospital_capacity.facility_week AS
SELECT value.hospital_pk,
       value.collection_week AS period_start,
       (value.collection_week + 6) AS period_end,
       MAX(value.value) FILTER (WHERE value.measure_id = 'inpatient_beds') AS inpatient_beds,
       MAX(value.value_status) FILTER (WHERE value.measure_id = 'inpatient_beds') AS inpatient_beds_status,
       MAX(value.value) FILTER (WHERE value.measure_id = 'inpatient_beds_used') AS inpatient_beds_used,
       MAX(value.value_status) FILTER (WHERE value.measure_id = 'inpatient_beds_used') AS inpatient_beds_used_status,
       MAX(value.value) FILTER (WHERE value.measure_id = 'total_icu_beds') AS total_icu_beds,
       MAX(value.value_status) FILTER (WHERE value.measure_id = 'total_icu_beds') AS total_icu_beds_status,
       MAX(value.value) FILTER (WHERE value.measure_id = 'icu_beds_used') AS icu_beds_used,
       MAX(value.value_status) FILTER (WHERE value.measure_id = 'icu_beds_used') AS icu_beds_used_status,
       MAX(value.value) FILTER (WHERE value.measure_id = 'staffed_adult_icu_beds') AS staffed_adult_icu_beds,
       MAX(value.value_status) FILTER (WHERE value.measure_id = 'staffed_adult_icu_beds') AS staffed_adult_icu_beds_status,
       MAX(value.value) FILTER (WHERE value.measure_id = 'staffed_adult_icu_beds_occupied') AS staffed_adult_icu_beds_occupied,
       MAX(value.value_status) FILTER (WHERE value.measure_id = 'staffed_adult_icu_beds_occupied') AS staffed_adult_icu_beds_occupied_status,
       MAX(value.value) FILTER (WHERE value.measure_id = 'inpatient_beds_used_covid') AS inpatient_beds_used_covid,
       MAX(value.value_status) FILTER (WHERE value.measure_id = 'inpatient_beds_used_covid') AS inpatient_beds_used_covid_status,
       MAX(value.value) FILTER (WHERE value.measure_id = 'patients_hospitalized_influenza') AS patients_hospitalized_influenza,
       MAX(value.value_status) FILTER (WHERE value.measure_id = 'patients_hospitalized_influenza') AS patients_hospitalized_influenza_status,
       value.snapshot_id
FROM silver_hhs_hospital_capacity.facility_week_value AS value
WHERE value.snapshot_id = (
    SELECT snapshot.snapshot_id FROM control.hhs_hospital_capacity_snapshot AS snapshot
    WHERE snapshot.status = 'published'
    ORDER BY snapshot.published_at DESC LIMIT 1
)
GROUP BY value.snapshot_id, value.hospital_pk, value.collection_week;

CREATE OR REPLACE VIEW gold_hhs_hospital_capacity.measure_export AS
SELECT 'anag-cw7u'::TEXT AS source_dataset,
       measure.measure_id AS source_measure_code,
       measure.measure_label AS display_name,
       measure.unit,
       measure.basis,
       measure.source_column,
       measure.coverage_column,
       measure.parser_contract_version AS schema_version
FROM silver_hhs_hospital_capacity.dim_measure AS measure;

COMMENT ON SCHEMA gold_hhs_hospital_capacity IS
    'Policy-free publication views for HHS COVID-19 hospital capacity by facility, 2020 to 2024.';

COMMENT ON VIEW gold_hhs_hospital_capacity.area_latest IS
    'Sums of the published weekly figures of the hospitals HHS places in each area.';
