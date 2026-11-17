-- Publication views over published vehicle registrations releases
-- (vehicle-fleet-tables). Applied by the bootstrap manifest in the `gold`
-- phase and re-applied by the DAG's `ensure_vehicle_registrations_schema`
-- task; this file is the only definition of these views. They are read by
-- the survival game, which pins a summary at build time; the county, state
-- and nation figures are also discoverable through the neutral catalog and
-- observations routes (`registration_revision`, `registration_latest`).

CREATE SCHEMA IF NOT EXISTS gold_vehicle_registrations;

-- The newest published text of every Report 28 figure: one row per year,
-- table part, county, printed line and column. `geo_id` is the county for a
-- county's lines and Sum, the state for the statewide Sum, and empty for
-- vehicles WisDOT places in no county. A city, village or town is named as
-- WisDOT prints it (`cvt_name`, `cvt_code`, `cvt_kind` T/V/C), not placed on
-- a Census geography.
CREATE OR REPLACE VIEW gold_vehicle_registrations.wisdot_registration_latest AS
SELECT DISTINCT ON (value.data_year, value.table_part, value.county_code,
                    value.line_key, value.vehicle_type)
       value.data_year,
       value.table_part,
       value.county_code,
       value.county_name,
       CASE
           WHEN value.county_code <> '' THEN county.geo_id
           WHEN value.table_part = 'county' AND value.row_kind = 'sum' THEN 'state:55'
       END AS geo_id,
       CASE
           WHEN value.county_code <> '' THEN 'county'
           WHEN value.table_part = 'county' AND value.row_kind = 'sum' THEN 'state'
       END AS geo_type,
       value.row_kind,
       value.line_key,
       value.cvt_name,
       value.cvt_code,
       value.cvt_kind,
       value.vehicle_type,
       value.value,
       value.value_status,
       value.value_source,
       value.page_number,
       document.source_url,
       capture.retrieved_at,
       value.capture_id,
       release.release_id,
       release.published_at
FROM silver_vehicle_registrations.wisdot_count AS value
JOIN control.vehicle_registrations_release AS release
  ON release.release_id = value.release_id
JOIN control.vehicle_registrations_document AS document
  ON document.release_id = value.release_id AND document.capture_id = value.capture_id
JOIN raw_capture.response_capture AS capture ON capture.capture_id = value.capture_id
LEFT JOIN silver_vehicle_registrations.wisdot_county AS county
  ON county.county_code = value.county_code
WHERE release.status = 'published'
ORDER BY value.data_year, value.table_part, value.county_code, value.line_key,
         value.vehicle_type, release.published_at DESC, release.release_id DESC;

-- The newest published text of every MV-1 figure: one row per year, area,
-- vehicle class and ownership column. `geo_id` is the state, or the nation
-- for FHWA's Total row. The footnotes FHWA printed under that year's table
-- are carried beside the marks the area's name bears.
CREATE OR REPLACE VIEW gold_vehicle_registrations.fhwa_mv1_latest AS
SELECT DISTINCT ON (value.data_year, value.area_name, value.vehicle_class, value.ownership)
       value.data_year,
       value.area_name,
       value.source_label,
       CASE WHEN value.state_fips IS NULL THEN 'us:1' ELSE 'state:' || value.state_fips END AS geo_id,
       CASE WHEN value.state_fips IS NULL THEN 'nation' ELSE 'state' END AS geo_type,
       value.vehicle_class,
       value.ownership,
       value.value,
       value.value_status,
       value.value_source,
       value.footnote_marks,
       document.footnotes AS table_footnotes,
       document.source_url,
       capture.retrieved_at,
       value.capture_id,
       release.release_id,
       release.published_at
FROM silver_vehicle_registrations.fhwa_mv1_figure AS value
JOIN control.vehicle_registrations_release AS release
  ON release.release_id = value.release_id
JOIN control.vehicle_registrations_document AS document
  ON document.release_id = value.release_id AND document.capture_id = value.capture_id
JOIN raw_capture.response_capture AS capture ON capture.capture_id = value.capture_id
WHERE release.status = 'published'
ORDER BY value.data_year, value.area_name, value.vehicle_class, value.ownership,
         release.published_at DESC, release.release_id DESC;

-- Every published revision of every served figure: the as-released surface.
-- Report 28's county lines and statewide Sum (Table 2) and MV-1's states and
-- Total, one row per figure as the release that first carried or changed it.
-- A city, village or town line, and vehicles WisDOT places in no county, are
-- in `wisdot_registration_latest` only: neither is a geography served here.
CREATE OR REPLACE VIEW gold_vehicle_registrations.registration_revision AS
WITH served AS (
    SELECT 'wisdot_rpt28:' || CASE WHEN value.vehicle_type = 'SUM' THEN 'all'
                                   ELSE LOWER(value.vehicle_type) END AS measure_id,
           value.data_year,
           CASE WHEN value.row_kind = 'sum' THEN 'state:55' ELSE county.geo_id END AS geo_id,
           CASE WHEN value.row_kind = 'sum' THEN 'state' ELSE 'county' END AS geo_type,
           CASE WHEN value.row_kind = 'sum' THEN 'WISCONSIN' ELSE value.county_name END AS source_area,
           value.value::NUMERIC AS value,
           value.value_status,
           value.value_source,
           ARRAY[]::TEXT[] AS footnote_marks,
           value.data_year::TEXT || '|wisdot|' || value.county_code || '|' || value.vehicle_type
               AS record_key,
           value.capture_id,
           value.release_id
    FROM silver_vehicle_registrations.wisdot_count AS value
    LEFT JOIN silver_vehicle_registrations.wisdot_county AS county
      ON county.county_code = value.county_code
    WHERE value.table_part = 'county'
      AND (value.row_kind = 'sum' OR (value.row_kind = 'county' AND county.geo_id IS NOT NULL))
    UNION ALL
    SELECT 'fhwa_mv1:' || value.vehicle_class || ':' || value.ownership,
           value.data_year,
           CASE WHEN value.state_fips IS NULL THEN 'us:1' ELSE 'state:' || value.state_fips END,
           CASE WHEN value.state_fips IS NULL THEN 'nation' ELSE 'state' END,
           value.source_label,
           value.value,
           CASE WHEN value.value_status = 'valid' THEN 'valid' ELSE 'not_reported' END,
           value.value_source,
           value.footnote_marks,
           value.data_year::TEXT || '|mv1|' || value.area_name || '|' || value.vehicle_class
               || '|' || value.ownership,
           value.capture_id,
           value.release_id
    FROM silver_vehicle_registrations.fhwa_mv1_figure AS value
)
SELECT served.measure_id,
       served.measure_id AS metric_key,
       measure.measure_label,
       measure.unit,
       measure.basis,
       measure.publisher,
       make_date(served.data_year, 1, 1) AS period_start,
       make_date(served.data_year, 12, 31) AS period_end,
       served.data_year,
       served.source_area,
       served.geo_id,
       entity.geo_sk,
       served.geo_type,
       served.value,
       served.value_status,
       served.value_source,
       served.footnote_marks,
       document.source_url,
       capture.retrieved_at,
       TO_CHAR(capture.retrieved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS release_key,
       md5(served.record_key) AS source_record_id,
       served.capture_id,
       release.run_id,
       release.release_id,
       release.published_at
FROM served
JOIN control.vehicle_registrations_release AS release ON release.release_id = served.release_id
JOIN control.vehicle_registrations_document AS document
  ON document.release_id = served.release_id AND document.capture_id = served.capture_id
JOIN raw_capture.response_capture AS capture ON capture.capture_id = served.capture_id
JOIN silver_vehicle_registrations.dim_measure AS measure ON measure.measure_id = served.measure_id
LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = served.geo_id
WHERE release.status = 'published';

-- The newest published revision of each served figure.
CREATE OR REPLACE VIEW gold_vehicle_registrations.registration_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.period_start)
       revision.*
FROM gold_vehicle_registrations.registration_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.period_start,
         revision.published_at DESC, revision.release_id DESC;

CREATE OR REPLACE VIEW gold_vehicle_registrations.measure_export AS
SELECT measure.publisher AS source_dataset,
       measure.measure_id AS source_measure_code,
       measure.measure_label AS display_name,
       measure.unit,
       measure.basis,
       measure.source_column,
       measure.parser_contract_version AS schema_version
FROM silver_vehicle_registrations.dim_measure AS measure;

COMMENT ON SCHEMA gold_vehicle_registrations IS
    'Policy-free views of registered vehicles by type: WisDOT Report 28 by Wisconsin county and city/village/town, and FHWA MV-1 by state, as published.';
COMMENT ON VIEW gold_vehicle_registrations.wisdot_registration_latest IS
    'The newest published WisDOT Report 28 count of each year, county, printed line and vehicle type, as printed.';
COMMENT ON VIEW gold_vehicle_registrations.fhwa_mv1_latest IS
    'The newest published FHWA MV-1 registration figure of each year, state and vehicle class, as published.';
COMMENT ON VIEW gold_vehicle_registrations.registration_latest IS
    'The newest published registration figure of each served county, state or the nation, year and measure, as WisDOT or FHWA published it.';
