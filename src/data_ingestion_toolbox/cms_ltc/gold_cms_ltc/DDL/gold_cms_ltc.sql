-- Publication views over reconciled CMS long-term care facility files
-- (nursing-home-locations). Applied by the bootstrap manifest in the `gold`
-- phase and re-applied by the DAG's `ensure_cms_ltc_schema` task; this file
-- is the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_cms_ltc;

-- Every published file's area sums: the as-released surface. Each sum adds
-- the published figures of the facilities CMS lists in the area for that
-- release; the period is the release's own (a CMS-671 quarter, a cost-report
-- year). A sum for an area the shared geography does not hold is served as
-- `unmapped` (DB-003): explicit, not dropped.
CREATE OR REPLACE VIEW gold_cms_ltc.area_revision AS
SELECT measure.series_id,
       area.measure_id,
       measure.series_id || ':' || area.measure_id AS metric_key,
       measure.measure_label,
       measure.unit,
       measure.basis,
       measure.accrual,
       file.period_start,
       file.period_end,
       EXTRACT(YEAR FROM file.period_start)::INTEGER AS period_year,
       file.release_title,
       area.geo_id,
       area.geo_sk,
       area.geo_type,
       gold_glossary.geo_grain(area.geo_type) AS geo_level,
       area.geography_status,
       area.value,
       'valid'::TEXT AS value_status,
       area.facility_count,
       area.unplaced_facility_count,
       'sum_of_published_facility_figures'::TEXT AS derivation,
       capture.retrieved_at,
       TO_CHAR(capture.retrieved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS release_key,
       area.source_record_id,
       file.capture_id,
       area.run_id,
       CASE area.geo_type
           WHEN 'county' THEN 'Facilities CMS places on no county (no Medicare cost report, or a cost-report county name that matches no county or two) are in no county sum; they are in their state and national sums.'
           ELSE 'Includes facilities CMS places on no county; unplaced_facility_count says how many.'
       END AS coverage_note
FROM silver_cms_ltc.area_total AS area
JOIN control.cms_ltc_file AS file ON file.run_id = area.run_id
JOIN raw_capture.response_capture AS capture ON capture.capture_id = file.capture_id
JOIN silver_cms_ltc.dim_measure AS measure
  ON measure.series_id = file.series_id AND measure.measure_id = area.measure_id
WHERE file.status = 'published'
  AND area.geography_status NOT IN ('unsupported', 'ambiguous');

-- The newest published capture of each area sum for each release period.
CREATE OR REPLACE VIEW gold_cms_ltc.area_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.period_start)
       revision.*
FROM gold_cms_ltc.area_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.period_start,
         revision.retrieved_at DESC, revision.capture_id DESC;

-- The facilities CMS lists now: every facility of the newest published
-- CMS-671 quarter (residents on its survey), with its newest published cost
-- report (beds, street address, county), labelled by that report's year. The
-- county is the cost report's, placed by name (`county_match` says how); a
-- facility with no cost report, or whose county name matched no county or
-- two, has none. A facility missing from the newest quarter is not listed
-- here; `facility_history` keeps it with its last periods.
CREATE OR REPLACE VIEW gold_cms_ltc.facility_latest AS
WITH newest AS (
    SELECT file.run_id, file.period_start, file.period_end
    FROM control.cms_ltc_file AS file
    WHERE file.series_id = 'characteristics' AND file.status = 'published'
    ORDER BY file.period_start DESC, file.published_at DESC
    LIMIT 1
)
SELECT report.ccn,
       report.facility_name,
       cost.street_address,
       report.city,
       report.state_code,
       report.state_fips,
       report.zip_code,
       cost.county_name AS cost_report_county_name,
       place.county_geo_id,
       COALESCE(place.county_status, 'no_cost_report') AS county_status,
       report.ownership_type,
       report.hospital_based,
       report.certification_date,
       newest.period_start AS residents_quarter_start,
       newest.period_end AS residents_quarter_end,
       residents.value AS total_residents,
       EXTRACT(YEAR FROM place.period_start)::INTEGER AS cost_report_year,
       cost.fiscal_year_begin,
       cost.fiscal_year_end,
       beds.value AS number_of_beds,
       cost.county_match
FROM newest
JOIN silver_cms_ltc.facility_report AS report ON report.run_id = newest.run_id
LEFT JOIN silver_cms_ltc.facility_place AS place ON place.ccn = report.ccn
LEFT JOIN silver_cms_ltc.facility_report AS cost
  ON cost.run_id = place.run_id AND cost.report_key = place.report_key
LEFT JOIN silver_cms_ltc.facility_count AS residents
  ON residents.run_id = report.run_id AND residents.report_key = report.report_key
 AND residents.measure_id = 'total_residents'
LEFT JOIN silver_cms_ltc.facility_count AS beds
  ON beds.run_id = place.run_id AND beds.report_key = place.report_key
 AND beds.measure_id = 'number_of_beds';

-- Every facility CMS has listed, by certification number, including those
-- no longer in the newest CMS-671 quarter (`in_newest_quarter` false): its
-- last published CMS-671 quarter and its last published cost report, each
-- with its own period, so a facility that stopped reporting reads as such.
-- By certification number: its newest published
-- CMS-671 quarter (residents on its survey) and its newest published cost
-- report (beds, street address, county), each with its own period. The
-- county is the cost report's, placed by name; a facility with no cost
-- report, or whose county name matched no county or two, has none.
CREATE OR REPLACE VIEW gold_cms_ltc.facility_history AS
WITH form_671 AS (
    SELECT DISTINCT ON (report.ccn)
           report.ccn, report.run_id, report.report_key, report.facility_name,
           report.city, report.state_code, report.state_fips, report.zip_code,
           report.ownership_type, report.hospital_based, report.certification_date,
           file.period_start, file.period_end
    FROM silver_cms_ltc.facility_report AS report
    JOIN control.cms_ltc_file AS file ON file.run_id = report.run_id
    WHERE report.series_id = 'characteristics' AND file.status = 'published'
    ORDER BY report.ccn, file.period_start DESC, file.published_at DESC
), cost AS (
    SELECT DISTINCT ON (report.ccn)
           report.ccn, report.run_id, report.report_key, report.facility_name,
           report.street_address, report.city, report.state_code, report.state_fips,
           report.zip_code, report.county_name, report.county_geo_id,
           report.county_status, report.fiscal_year_begin, report.fiscal_year_end,
           file.period_start
    FROM silver_cms_ltc.facility_report AS report
    JOIN control.cms_ltc_file AS file ON file.run_id = report.run_id
    WHERE report.series_id = 'cost_report' AND file.status = 'published'
    ORDER BY report.ccn, file.period_start DESC, report.fiscal_year_end DESC NULLS LAST,
             LENGTH(report.report_key) DESC, report.report_key DESC, file.published_at DESC
)
SELECT COALESCE(form_671.ccn, cost.ccn) AS ccn,
       COALESCE(form_671.facility_name, cost.facility_name) AS facility_name,
       cost.street_address,
       COALESCE(form_671.city, cost.city) AS city,
       COALESCE(form_671.state_code, cost.state_code) AS state_code,
       COALESCE(form_671.state_fips, cost.state_fips) AS state_fips,
       COALESCE(form_671.zip_code, cost.zip_code) AS zip_code,
       cost.county_name AS cost_report_county_name,
       cost.county_geo_id,
       COALESCE(cost.county_status, 'no_cost_report') AS county_status,
       form_671.ownership_type,
       form_671.hospital_based,
       form_671.certification_date,
       form_671.period_start AS residents_quarter_start,
       form_671.period_end AS residents_quarter_end,
       residents.value AS total_residents,
       EXTRACT(YEAR FROM cost.period_start)::INTEGER AS cost_report_year,
       cost.fiscal_year_begin,
       cost.fiscal_year_end,
       beds.value AS number_of_beds,
       COALESCE(form_671.period_start = (
           SELECT MAX(newest.period_start) FROM control.cms_ltc_file AS newest
           WHERE newest.series_id = 'characteristics' AND newest.status = 'published'
       ), FALSE) AS in_newest_quarter
FROM form_671
FULL JOIN cost ON cost.ccn = form_671.ccn
LEFT JOIN silver_cms_ltc.facility_count AS residents
  ON residents.run_id = form_671.run_id AND residents.report_key = form_671.report_key
 AND residents.measure_id = 'total_residents'
LEFT JOIN silver_cms_ltc.facility_count AS beds
  ON beds.run_id = cost.run_id AND beds.report_key = cost.report_key
 AND beds.measure_id = 'number_of_beds';

CREATE OR REPLACE VIEW gold_cms_ltc.measure_export AS
SELECT measure.series_id AS source_dataset,
       measure.measure_id AS source_measure_code,
       measure.measure_label AS display_name,
       measure.unit,
       measure.basis,
       measure.accrual,
       measure.source_column,
       measure.parser_contract_version AS schema_version
FROM silver_cms_ltc.dim_measure AS measure;

COMMENT ON SCHEMA gold_cms_ltc IS
    'Policy-free publication views for CMS long-term care facilities (CMS Form 671 and SNF cost reports).';

COMMENT ON VIEW gold_cms_ltc.area_latest IS
    'Sums of the published figures of the facilities CMS lists in each area, for each release period.';
