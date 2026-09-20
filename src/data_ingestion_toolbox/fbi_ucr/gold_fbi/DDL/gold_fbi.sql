-- Publication views over reconciled, published FBI UCR releases.
--
-- Applied by the bootstrap manifest in the `gold` phase and re-applied by the
-- `fbi_ucr_ingest` DAG's `ensure_fbi_schema` task. This file is the only
-- definition of these views.

-- ---------------------------------------------------------------------------
-- Gold publication views
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW gold_fbi.crime_observation AS
SELECT fact.observation_sk, fact.product_id, release.offense_code,
       release.offense_label, release.ucr_program, fact.release_key,
       release.refresh_date, release.max_data_month, fact.measure_id,
       measure.measure_form, measure.counted_entity_basis, measure.unit,
       measure.reported_status, fact.subject_type, fact.subject_code,
       fact.subject_label, fact.source_geo_level, fact.period,
       fact.period_start, fact.period_end, fact.geo_id, fact.geo_sk,
       fact.geography_status,
       CASE fact.subject_type
           WHEN 'national' THEN 'provider-published national total'
           WHEN 'state' THEN 'provider-published state total'
           ELSE 'agency-reported for one law-enforcement agency'
       END AS geography_basis,
       fact.value_source, fact.value, fact.value_status,
       fact.population_denominator,
       coverage.population, coverage.participated_population,
       coverage.coverage_percent, coverage.coverage_basis,
       coverage.participation_status, release.counted_entity_note,
       release.methodology_url, release.documentation_url,
       fact.source_record_id, fact.capture_id
FROM silver_fbi.fact_crime_observation AS fact
JOIN silver_fbi.dim_ucr_dataset_release AS release
  ON release.product_id = fact.product_id
 AND release.release_key = fact.release_key
JOIN silver_fbi.dim_offense_measure AS measure
  ON measure.product_id = fact.product_id
 AND measure.measure_id = fact.measure_id
JOIN silver_fbi.fact_reporting_participation AS coverage
  ON coverage.product_id = fact.product_id
 AND coverage.release_key = fact.release_key
 AND coverage.subject_type = fact.subject_type
 AND coverage.subject_code = fact.subject_code
 AND coverage.period = fact.period
WHERE release.status = 'published'
  AND fact.geography_status NOT IN ('ambiguous', 'unsupported');

CREATE OR REPLACE VIEW gold_fbi.reporting_coverage AS
SELECT coverage.participation_sk, coverage.product_id, coverage.release_key,
       coverage.ucr_program, coverage.subject_type, coverage.subject_code,
       coverage.subject_label, coverage.source_geo_level, coverage.period,
       coverage.period_start, coverage.period_end, coverage.geo_id,
       coverage.geo_sk, coverage.geography_status, coverage.population,
       coverage.participated_population, coverage.coverage_percent,
       coverage.coverage_basis, coverage.participation_status,
       release.refresh_date, release.methodology_url, coverage.capture_id
FROM silver_fbi.fact_reporting_participation AS coverage
JOIN silver_fbi.dim_ucr_dataset_release AS release
  ON release.product_id = coverage.product_id
 AND release.release_key = coverage.release_key
WHERE release.status = 'published';

CREATE OR REPLACE VIEW gold_fbi.agency_geography AS
SELECT relationship.relationship_sk, relationship.ori, agency.state_code,
       version.agency_name, version.agency_type, version.county_labels,
       relationship.relationship_type, relationship.source_label,
       relationship.geo_id, relationship.geo_sk,
       relationship.resolution_method, relationship.resolution_status,
       relationship.confidence_class, relationship.reason_code,
       relationship.effective_start, relationship.effective_end,
       relationship.geography_vintage, relationship.evidence_source,
       relationship.evidence_capture_id, relationship.release_key,
       agency.geo_sk AS agency_geo_sk
FROM silver_fbi.agency_geography_relationship AS relationship
JOIN silver_fbi.dim_agency AS agency USING (ori)
JOIN silver_fbi.dim_agency_version AS version
  ON version.ori = relationship.ori
 AND version.release_key = relationship.release_key
JOIN silver_fbi.dim_ucr_dataset_release AS release
  ON release.product_id = relationship.product_id
 AND release.release_key = relationship.release_key
WHERE release.status = 'published';

-- A county or place filter selects agency observations; it never sums them
-- into an area total. The agency observation identity is carried through so a
-- multi-county agency deduplicates by observation rather than multiplying.
CREATE OR REPLACE VIEW gold_fbi.agency_observation_area_filter AS
SELECT observation.observation_sk, observation.product_id,
       observation.release_key, observation.measure_id,
       observation.measure_form, observation.counted_entity_basis,
       observation.unit, observation.subject_code AS ori,
       observation.subject_label AS agency_name, observation.period,
       observation.period_start, observation.period_end, observation.value,
       observation.value_status, observation.participation_status,
       observation.coverage_percent,
       relationship.relationship_type AS filter_geography_type,
       relationship.geo_id AS filter_geo_id,
       relationship.geo_sk AS filter_geo_sk,
       relationship.source_label AS filter_source_label,
       relationship.confidence_class AS filter_confidence_class,
       CASE relationship.relationship_type
           WHEN 'county' THEN
               'agency-reported for agencies associated with this county'
           WHEN 'place' THEN
               'agency-reported for agencies mapped to this place'
           ELSE 'agency-reported for agencies associated with this state'
       END AS result_label,
       'agency' AS observation_grain
FROM gold_fbi.crime_observation AS observation
JOIN silver_fbi.agency_geography_relationship AS relationship
  ON relationship.ori = observation.subject_code
 AND relationship.product_id = observation.product_id
 -- The relationship is effective-dated, so a filter follows the observation's
 -- period rather than the release it was last confirmed in.
 AND observation.period_start >= relationship.effective_start
 AND observation.period_end <= relationship.effective_end
WHERE observation.subject_type = 'agency'
  AND relationship.resolution_status = 'resolved';

CREATE OR REPLACE VIEW gold_fbi.latest_release_observation AS
SELECT observation.*
FROM gold_fbi.crime_observation AS observation
JOIN (
    SELECT product_id, MAX(refresh_date) AS refresh_date
    FROM silver_fbi.dim_ucr_dataset_release
    WHERE status = 'published'
    GROUP BY product_id
) AS latest
  ON latest.product_id = observation.product_id
 AND latest.refresh_date = observation.refresh_date;

CREATE OR REPLACE VIEW gold_fbi.measure_export AS
SELECT measure.product_id AS source_dataset,
       measure.measure_id AS source_measure_code,
       measure.offense_code AS source_offense_code,
       measure.offense_label AS display_name,
       measure.ucr_program, measure.measure_form,
       measure.counted_entity_basis, measure.unit, measure.reported_status,
       release.release_key AS source_watermark,
       release.methodology_url, release.counted_entity_note,
       release.parser_contract_version AS schema_version
FROM silver_fbi.dim_offense_measure AS measure
JOIN LATERAL (
    SELECT candidate.*
    FROM silver_fbi.dim_ucr_dataset_release AS candidate
    WHERE candidate.product_id = measure.product_id
      AND candidate.status = 'published'
    ORDER BY candidate.refresh_date DESC
    LIMIT 1
) AS release ON TRUE;

COMMENT ON SCHEMA gold_fbi IS
    'Policy-free publication views for validated FBI UCR observations and coverage.';

COMMENT ON VIEW gold_fbi.agency_observation_area_filter IS
    'County/place filters over agency-grain observations; never a county or city total.';

COMMENT ON VIEW gold_fbi.measure_export IS
    'Provider-neutral glossary publisher contract; owns no gold_glossary objects.';
