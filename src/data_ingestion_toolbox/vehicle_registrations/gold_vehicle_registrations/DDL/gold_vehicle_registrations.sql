-- Publication views over published vehicle registrations releases
-- (vehicle-fleet-tables). Applied by the bootstrap manifest in the `gold`
-- phase and re-applied by the DAG's `ensure_vehicle_registrations_schema`
-- task; this file is the only definition of these views. They are read by
-- the warehouse's own consumers (the survival game pins a summary at build
-- time); nothing is served through the API.

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

COMMENT ON SCHEMA gold_vehicle_registrations IS
    'Policy-free views of registered vehicles by type: WisDOT Report 28 by Wisconsin county and city/village/town, and FHWA MV-1 by state, as published.';
COMMENT ON VIEW gold_vehicle_registrations.wisdot_registration_latest IS
    'The newest published WisDOT Report 28 count of each year, county, printed line and vehicle type, as printed.';
COMMENT ON VIEW gold_vehicle_registrations.fhwa_mv1_latest IS
    'The newest published FHWA MV-1 registration figure of each year, state and vehicle class, as published.';
