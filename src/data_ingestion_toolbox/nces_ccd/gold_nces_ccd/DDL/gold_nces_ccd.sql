-- Publication views over NCES Common Core of Data public schools (CCD
-- school-universe files placed by EDGE school geocodes, nces-ccd). Applied
-- by the bootstrap manifest in the `gold` phase and re-applied by the DAG's
-- `ensure_nces_ccd_schema` task; this file is the only definition of these
-- views.

CREATE SCHEMA IF NOT EXISTS gold_nces_ccd;

CREATE OR REPLACE VIEW gold_nces_ccd.measure_definition AS
SELECT measure.measure, measure.component, measure.measure_label, measure.unit,
       'NCES Common Core of Data (public schools): NCES publishes schools, not counties or states; '
       || 'this warehouse''s figure is the sum over the public schools that NCES''s EDGE geocode file '
       || 'places in the county (by its five-digit county code) or state (by the school''s physical '
       || 'state, never the BIE or DoDEA operating code), counting only values NCES flags Reported. '
       || 'schools_without_value says how many placed schools had none, so a partial sum is never '
       || 'silent. ' || measure.definition
       || ' Source: U.S. Department of Education, National Center for Education Statistics, Common '
       || 'Core of Data and EDGE school geocodes.' AS observation_basis
FROM (VALUES
    ('operating_schools', 'directory', 'Operating public schools', 'schools',
     'A school is counted when its directory status that year is Open, New, Added, Reopened or Changed Boundary/Agency.'),
    ('charter_schools', 'directory', 'Operating public charter schools', 'schools',
     'An operating school whose directory charter flag is Yes.'),
    ('student_membership', 'membership', 'Students enrolled (membership)', 'students',
     'Membership is the school''s reported enrollment count on or about October 1.'),
    ('teacher_fte', 'staff', 'Teachers (full-time equivalent)', 'full-time-equivalent teachers',
     'Classroom teachers in full-time equivalents.'),
    ('frpl_eligible', 'lunch', 'Students eligible for free or reduced-price lunch', 'students',
     'Free lunch is family income below 130 percent of poverty or direct certification; reduced-price is 130 to 185 percent. Since 2016-17 states may report this, direct certification, or both, and Community Eligibility Provision schools may report every student as free-eligible.'),
    ('free_lunch_eligible', 'lunch', 'Students eligible for free lunch', 'students',
     'Family income below 130 percent of poverty or categorical eligibility; Community Eligibility Provision schools may report every student.'),
    ('reduced_price_lunch_eligible', 'lunch', 'Students eligible for reduced-price lunch', 'students',
     'Family income from 130 to 185 percent of poverty.'),
    ('direct_certification', 'lunch', 'Students directly certified for free meals', 'students',
     'Students categorically eligible and reported to USDA (FNS-742); a separate measure from free or reduced-price eligibility, never a substitute for it.')
) AS measure(measure, component, measure_label, unit, definition);

-- The geocode file each school year is placed by: the newest published read
-- of the newest release.
CREATE OR REPLACE VIEW gold_nces_ccd.school_placement AS
WITH selected AS (
    SELECT DISTINCT ON (file.school_year) file.run_id, file.school_year, file.file_stem
    FROM control.nces_ccd_file AS file
    WHERE file.component = 'geocode' AND file.status = 'published'
    ORDER BY file.school_year, file.version_rank DESC, file.published_at DESC, file.run_id DESC
)
SELECT selected.school_year, selected.file_stem AS geocode_file, location.ncessch,
       location.state_fips, location.county_fips, location.operating_state_fips,
       location.geo_id AS county_geo_id, location.geo_sk AS county_geo_sk,
       location.geography_status AS county_status
FROM silver_nces_ccd.school_location AS location
JOIN selected ON selected.run_id = location.run_id;

-- Every published school value: NCES's own count per school with its flag,
-- and where the selected geocode file places the school. Not dispatched (the
-- API has no school grain); it is the lineage of every county and state
-- figure. Directory rows contribute one per operating school.
CREATE OR REPLACE VIEW gold_nces_ccd.school_observation AS
WITH published AS (
    SELECT file.*
    FROM control.nces_ccd_file AS file
    WHERE file.status = 'published' AND file.component <> 'geocode'
),
school_values AS (
    SELECT count.run_id, count.ncessch, count.measure, count.value, count.value_status,
           count.dms_flag, count.capture_id
    FROM silver_nces_ccd.school_count AS count
    UNION ALL
    SELECT directory.run_id, directory.ncessch, 'operating_schools', 1, 'valid', 'Reported', directory.capture_id
    FROM silver_nces_ccd.school_directory AS directory
    WHERE directory.school_status IN ('Open', 'New', 'Added', 'Reopened', 'Changed Boundary/Agency')
    UNION ALL
    SELECT directory.run_id, directory.ncessch, 'charter_schools',
           CASE WHEN directory.charter = 'Yes' THEN 1 ELSE 0 END, 'valid', 'Reported', directory.capture_id
    FROM silver_nces_ccd.school_directory AS directory
    WHERE directory.school_status IN ('Open', 'New', 'Added', 'Reopened', 'Changed Boundary/Agency')
)
SELECT published.run_id, published.component, published.school_year, published.file_stem,
       published.release_version, published.version_rank, published.published_at,
       school_values.capture_id, school_values.ncessch, school_values.measure, school_values.value,
       school_values.value_status, school_values.dms_flag,
       placement.geocode_file, placement.state_fips, placement.county_fips,
       placement.county_geo_id, placement.county_geo_sk, placement.county_status
FROM school_values
JOIN published ON published.run_id = school_values.run_id
LEFT JOIN gold_nces_ccd.school_placement AS placement
  ON placement.school_year = published.school_year AND placement.ncessch = school_values.ncessch;

-- The county and state figure for each published file: the sum of placed
-- schools' Reported values, with how many placed schools had a value and how
-- many had none. A grain where no placed school reported has a row with no
-- value and status `missing`, never a zero.
CREATE OR REPLACE VIEW gold_nces_ccd.observation_revision AS
WITH grains AS (
    SELECT school.*, 'county'::TEXT AS geo_type, school.county_geo_id AS geo_id, school.county_geo_sk AS geo_sk
    FROM gold_nces_ccd.school_observation AS school
    WHERE school.county_status = 'resolved'
    UNION ALL
    SELECT school.*, 'state'::TEXT, entity.geo_id, entity.geo_sk
    FROM gold_nces_ccd.school_observation AS school
    JOIN silver_ref.dim_geo_entity AS entity
      ON entity.geo_id = 'state:' || school.state_fips AND entity.geo_type = 'state'
),
rolled AS (
    SELECT grains.run_id, grains.measure, grains.geo_type, grains.geo_id,
           MIN(grains.geo_sk) AS geo_sk,
           SUM(grains.value) FILTER (WHERE grains.value_status = 'valid') AS value,
           COUNT(*) FILTER (WHERE grains.value_status = 'valid') AS schools_with_value,
           COUNT(*) FILTER (WHERE grains.value_status <> 'valid') AS schools_without_value,
           MIN(grains.school_year) AS school_year,
           MIN(grains.file_stem) AS file_stem,
           MIN(grains.release_version) AS release_version,
           MIN(grains.version_rank) AS version_rank,
           MIN(grains.geocode_file) AS geocode_file,
           MIN(grains.capture_id::TEXT)::UUID AS capture_id,
           MIN(grains.published_at) AS published_at
    FROM grains
    GROUP BY grains.run_id, grains.measure, grains.geo_type, grains.geo_id
)
SELECT rolled.measure AS metric_key,
       definition.measure_label,
       definition.unit,
       definition.observation_basis,
       LEFT(rolled.school_year, 4)::INTEGER AS year,
       rolled.school_year,
       MAKE_DATE(LEFT(rolled.school_year, 4)::INTEGER, 7, 1) AS period_start,
       MAKE_DATE(RIGHT(rolled.school_year, 4)::INTEGER, 6, 30) AS period_end,
       rolled.geo_id,
       rolled.geo_sk,
       rolled.geo_type,
       gold_glossary.geo_grain(rolled.geo_type) AS geo_level,
       'resolved'::TEXT AS geography_status,
       CASE WHEN rolled.schools_with_value > 0 THEN rolled.value END AS value,
       CASE WHEN rolled.schools_with_value > 0 THEN 'valid' ELSE 'missing' END AS value_status,
       rolled.schools_with_value,
       rolled.schools_without_value,
       CASE WHEN rolled.schools_without_value = 0 THEN 'complete' ELSE 'partial' END AS completeness,
       rolled.file_stem AS ccd_file,
       rolled.geocode_file,
       'CCD ' || rolled.school_year || ' v' || rolled.release_version || ' read '
           || TO_CHAR(rolled.published_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS release_key,
       rolled.version_rank,
       rolled.published_at,
       md5(rolled.measure || '|' || rolled.geo_id || '|' || rolled.run_id::TEXT) AS source_record_id,
       rolled.capture_id,
       rolled.run_id
FROM rolled
JOIN gold_nces_ccd.measure_definition AS definition ON definition.measure = rolled.measure;

-- The newest release of each grain-year, and its newest read.
CREATE OR REPLACE VIEW gold_nces_ccd.observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.year)
       revision.*
FROM gold_nces_ccd.observation_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.year,
         revision.version_rank DESC, revision.published_at DESC, revision.run_id DESC;

CREATE OR REPLACE VIEW gold_nces_ccd.measure_export AS
SELECT measure AS source_object_key, measure, measure_label, unit, observation_basis,
       'nces_ccd_csv:v1'::TEXT AS schema_version
FROM gold_nces_ccd.measure_definition;

COMMENT ON SCHEMA gold_nces_ccd IS
    'Policy-free publication views for NCES public schools and derived county and state rollups.';
