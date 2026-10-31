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

-- Derived county roll-up over agency-reported absolute totals.
--
-- This is the one declared-derived aggregate in this schema, the exception
-- the county-crime roll-up plan carved out of the never-sum rule that
-- `agency_observation_area_filter` keeps for provider-faithful products:
--
-- * Only absolute offense/clearance totals are summed; rates, percentages,
--   and trends are never aggregated, and no rate is derived here.
-- * An agency resolved to more than one county contributes its whole
--   published count to each of its counties, because the provider publishes
--   no allocation between them and a split would be an invented number.
--   Every row therefore says whether such an agency contributed, and county
--   values are not additive to state totals.
-- * Only `reported` values enter the sum. An agency month the provider did
--   not publish stays visible through the mapped-versus-reporting coverage
--   columns and is never read as zero; a county period with no reporting
--   agency publishes no row at all.
CREATE OR REPLACE VIEW gold_fbi.county_rollup AS
WITH county_link AS (
    SELECT relationship.product_id, relationship.ori, relationship.geo_id,
           relationship.geo_sk, relationship.effective_start,
           relationship.effective_end
    FROM silver_fbi.agency_geography_relationship AS relationship
    WHERE relationship.relationship_type = 'county'
      AND relationship.resolution_status = 'resolved'
), contribution AS (
    SELECT DISTINCT observation.product_id, observation.release_key,
           observation.measure_id, link.geo_id, link.geo_sk,
           observation.period, observation.period_start,
           observation.period_end, observation.subject_code AS ori,
           observation.value, observation.value_status,
           (SELECT COUNT(DISTINCT sibling.geo_id) > 1
            FROM county_link AS sibling
            WHERE sibling.product_id = observation.product_id
              AND sibling.ori = observation.subject_code
              AND observation.period_start >= sibling.effective_start
              AND observation.period_end <= sibling.effective_end
           ) AS is_multi_county
    FROM gold_fbi.crime_observation AS observation
    JOIN county_link AS link
      ON link.product_id = observation.product_id
     AND link.ori = observation.subject_code
     -- The relationship is effective-dated, so a contribution follows the
     -- observation's period rather than the release it was confirmed in.
     AND observation.period_start >= link.effective_start
     AND observation.period_end <= link.effective_end
    WHERE observation.subject_type = 'agency'
      AND observation.measure_form = 'absolute_total'
      AND observation.counted_entity_basis IN ('offense', 'clearance')
), summed AS (
    SELECT contribution.product_id, contribution.release_key,
           contribution.measure_id, contribution.geo_id, contribution.geo_sk,
           contribution.period, contribution.period_start,
           contribution.period_end,
           SUM(contribution.value)
               FILTER (WHERE contribution.value_status = 'reported') AS value,
           ARRAY_AGG(DISTINCT contribution.ori ORDER BY contribution.ori)
               FILTER (WHERE contribution.value_status = 'reported')
               AS contributing_oris,
           COUNT(DISTINCT contribution.ori)
               FILTER (WHERE contribution.value_status = 'reported')
               AS reporting_agency_count,
           BOOL_OR(contribution.is_multi_county)
               FILTER (WHERE contribution.value_status = 'reported')
               AS includes_multi_county_agency
    FROM contribution
    GROUP BY contribution.product_id, contribution.release_key,
             contribution.measure_id, contribution.geo_id, contribution.geo_sk,
             contribution.period, contribution.period_start,
             contribution.period_end
    -- A county period where no mapped agency reported publishes no row:
    -- an empty sum is never presented as a zero observation.
    HAVING COUNT(*) FILTER (WHERE contribution.value_status = 'reported') > 0
)
SELECT summed.product_id, summed.release_key, release.refresh_date,
       release.max_data_month, release.ucr_program, measure.offense_code,
       measure.offense_label, summed.measure_id, measure.measure_form,
       measure.counted_entity_basis, measure.unit, summed.geo_id,
       summed.geo_sk, county.county_name, county.state_fips,
       county.county_fips, summed.period, summed.period_start,
       summed.period_end, summed.value, summed.contributing_oris,
       summed.reporting_agency_count, mapped.mapped_agency_count,
       summed.includes_multi_county_agency,
       TRUE AS derived,
       'sum_of_agency_reported_totals'::TEXT AS derivation_method,
       'derived county roll-up of agency-reported totals'::TEXT
           AS result_label,
       ('Each mapped agency''s whole published count is summed; an agency '
        || 'serving more than one county is counted in full in each of its '
        || 'counties, so county values are not additive to state totals. '
        || 'Agency months the provider did not publish are excluded, never '
        || 'zero.')::TEXT AS methodology_note,
       release.counted_entity_note, release.methodology_url,
       release.documentation_url
FROM summed
JOIN silver_fbi.dim_offense_measure AS measure
  ON measure.product_id = summed.product_id
 AND measure.measure_id = summed.measure_id
JOIN silver_fbi.dim_ucr_dataset_release AS release
  ON release.product_id = summed.product_id
 AND release.release_key = summed.release_key
JOIN silver_ref.dim_geo_current AS county
  ON county.geo_id = summed.geo_id
JOIN LATERAL (
    SELECT COUNT(DISTINCT county_link.ori) AS mapped_agency_count
    FROM county_link
    WHERE county_link.product_id = summed.product_id
      AND county_link.geo_id = summed.geo_id
      AND summed.period_start >= county_link.effective_start
      AND summed.period_end <= county_link.effective_end
) AS mapped ON TRUE;

-- The roll-up at each product's most recent published release, mirroring
-- `latest_release_observation` so serving reads one release by default.
CREATE OR REPLACE VIEW gold_fbi.latest_county_rollup AS
SELECT rollup.*
FROM gold_fbi.county_rollup AS rollup
JOIN (
    SELECT product_id, MAX(refresh_date) AS refresh_date
    FROM silver_fbi.dim_ucr_dataset_release
    WHERE status = 'published'
    GROUP BY product_id
) AS latest
  ON latest.product_id = rollup.product_id
 AND latest.refresh_date = rollup.refresh_date;

-- A county crime rate computed the way the FBI computes a state rate
-- (ADR-0012). For a county and a month, the reported offenses of the
-- agencies linked to that county alone are divided by those same agencies'
-- FBI-published populations, times 100,000:
--
-- * Numerator and denominator are both FBI figures from one release and one
--   month; no Census population enters the rate or its coverage (ADR-0009
--   rule 2 holds unchanged).
-- * An agency resolved to more than one county that month is left out of
--   both sides, because the FBI publishes one count and one population for
--   its whole jurisdiction and any split would be invented. It is listed.
-- * An agency whose population the FBI did not publish is left out of both
--   sides too, and listed, so no offense is counted without its population.
-- * Coverage is published on every row: the covered population (reporting
--   agencies) over the linked population (every single-county agency with a
--   published population, reporting or not). A county-month with no
--   reporting agency, only multi-county reporters, or coverage under the 50 %
--   floor carries no rate and says why; it is never a zero.
-- * Monthly only, like the agency reports it divides; never rolled up.
CREATE OR REPLACE VIEW gold_fbi.county_agency_rate AS
WITH agency_month AS (
    SELECT DISTINCT observation.product_id, observation.release_key,
           observation.measure_id, link.geo_id, observation.period,
           observation.period_start, observation.period_end,
           observation.subject_code AS ori, observation.value,
           observation.value_status, observation.population,
           (SELECT COUNT(DISTINCT sibling.geo_id) > 1
            FROM silver_fbi.agency_geography_relationship AS sibling
            WHERE sibling.relationship_type = 'county'
              AND sibling.resolution_status = 'resolved'
              AND sibling.product_id = observation.product_id
              AND sibling.ori = observation.subject_code
              AND observation.period_start >= sibling.effective_start
              AND observation.period_end <= sibling.effective_end
           ) AS is_multi_county
    FROM gold_fbi.crime_observation AS observation
    JOIN silver_fbi.agency_geography_relationship AS link
      ON link.product_id = observation.product_id
     AND link.ori = observation.subject_code
     AND link.relationship_type = 'county'
     AND link.resolution_status = 'resolved'
     -- Effective-dated, so a county follows the observation's own month.
     AND observation.period_start >= link.effective_start
     AND observation.period_end <= link.effective_end
    WHERE observation.subject_type = 'agency'
      AND observation.measure_form = 'absolute_total'
      AND observation.counted_entity_basis = 'offense'
), classified AS (
    SELECT agency_month.*,
           NOT agency_month.is_multi_county AS is_single_county,
           agency_month.value_status = 'reported' AS is_reported,
           agency_month.population IS NOT NULL AS has_population
    FROM agency_month
), summed AS (
    SELECT classified.product_id, classified.release_key,
           classified.measure_id, classified.geo_id, classified.period,
           classified.period_start, classified.period_end,
           SUM(classified.value) FILTER (
               WHERE classified.is_single_county AND classified.is_reported
                 AND classified.has_population
           ) AS offenses,
           SUM(classified.population) FILTER (
               WHERE classified.is_single_county AND classified.is_reported
                 AND classified.has_population
           ) AS covered_population,
           SUM(classified.population) FILTER (
               WHERE classified.is_single_county AND classified.has_population
           ) AS linked_population,
           ARRAY_AGG(DISTINCT classified.ori ORDER BY classified.ori) FILTER (
               WHERE classified.is_single_county AND classified.is_reported
                 AND classified.has_population
           ) AS contributing_oris,
           ARRAY_AGG(DISTINCT classified.ori ORDER BY classified.ori) FILTER (
               WHERE classified.is_multi_county
           ) AS excluded_multi_county_oris,
           ARRAY_AGG(DISTINCT classified.ori ORDER BY classified.ori) FILTER (
               WHERE classified.is_single_county AND NOT classified.is_reported
           ) AS non_reporting_oris,
           ARRAY_AGG(DISTINCT classified.ori ORDER BY classified.ori) FILTER (
               WHERE classified.is_single_county AND classified.is_reported
                 AND NOT classified.has_population
           ) AS unknown_population_oris,
           COUNT(*) FILTER (WHERE classified.is_reported) AS reporting_rows,
           COUNT(*) FILTER (
               WHERE classified.is_single_county AND classified.is_reported
           ) AS single_county_reporting_rows
    FROM classified
    GROUP BY classified.product_id, classified.release_key,
             classified.measure_id, classified.geo_id, classified.period,
             classified.period_start, classified.period_end
), assessed AS (
    SELECT summed.*,
           CASE WHEN summed.linked_population > 0
                THEN summed.covered_population / summed.linked_population
           END AS coverage_share,
           CASE
               WHEN summed.reporting_rows = 0 THEN 'no_reporting_agency'
               WHEN summed.single_county_reporting_rows = 0
                   THEN 'only_multi_county_agencies'
               WHEN summed.covered_population IS NULL
                 OR summed.linked_population IS NULL
                 OR summed.linked_population = 0
                 OR summed.covered_population = 0
                 OR summed.covered_population < 0.5 * summed.linked_population
                   THEN 'coverage_below_floor'
           END AS unavailable_reason
    FROM summed
)
SELECT assessed.product_id, assessed.release_key, release.refresh_date,
       release.max_data_month, release.ucr_program, measure.offense_code,
       measure.offense_label, assessed.measure_id, measure.measure_form,
       measure.counted_entity_basis, assessed.geo_id, county.state_fips,
       county.county_fips, attribute.name AS county_name, assessed.period,
       assessed.period_start, assessed.period_end,
       CASE WHEN assessed.unavailable_reason IS NULL
            THEN assessed.offenses * 100000 / assessed.covered_population
       END AS rate,
       assessed.unavailable_reason, assessed.offenses,
       assessed.covered_population, assessed.linked_population,
       assessed.coverage_share, 0.5::NUMERIC AS coverage_floor,
       COALESCE(assessed.contributing_oris, ARRAY[]::TEXT[])
           AS contributing_oris,
       COALESCE(assessed.excluded_multi_county_oris, ARRAY[]::TEXT[])
           AS excluded_multi_county_oris,
       COALESCE(assessed.non_reporting_oris, ARRAY[]::TEXT[])
           AS non_reporting_oris,
       COALESCE(assessed.unknown_population_oris, ARRAY[]::TEXT[])
           AS unknown_population_oris,
       TRUE AS derived,
       'agency_reported_offenses_per_covered_population'::TEXT
           AS derivation_method,
       'per 100,000 people covered by reporting agencies in this county'::TEXT
           AS unit,
       ('Offenses reported by the agencies serving only this county, divided '
        || 'by those agencies'' FBI-published populations, times 100,000 '
        || '(ADR-0012). Agencies serving more than one county, and agencies '
        || 'with no published population, are left out of both sides. No '
        || 'rate is given when no agency reported, when only multi-county '
        || 'agencies reported, or when the reporting agencies cover under '
        || 'half the population the county''s agencies serve.')::TEXT
           AS methodology_note,
       release.counted_entity_note, release.methodology_url,
       release.documentation_url
FROM assessed
JOIN silver_fbi.dim_offense_measure AS measure
  ON measure.product_id = assessed.product_id
 AND measure.measure_id = assessed.measure_id
JOIN silver_fbi.dim_ucr_dataset_release AS release
  ON release.product_id = assessed.product_id
 AND release.release_key = assessed.release_key
JOIN silver_ref.dim_geo_entity AS county
  ON county.geo_id = assessed.geo_id
LEFT JOIN LATERAL (
    SELECT version.name
    FROM silver_ref.dim_geo_entity_version AS version
    WHERE version.geo_sk = county.geo_sk
    ORDER BY version.geography_vintage DESC, version.ingested_at DESC
    LIMIT 1
) AS attribute ON TRUE;

-- The county rate at each product's most recent published release.
CREATE OR REPLACE VIEW gold_fbi.latest_county_agency_rate AS
SELECT rate.*
FROM gold_fbi.county_agency_rate AS rate
JOIN (
    SELECT product_id, MAX(refresh_date) AS refresh_date
    FROM silver_fbi.dim_ucr_dataset_release
    WHERE status = 'published'
    GROUP BY product_id
) AS latest
  ON latest.product_id = rate.product_id
 AND latest.refresh_date = rate.refresh_date;

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


-- Derived calendar rollups (ADR-0007, RU-4): quarters and calendar years of
-- the FBI UCR metrics whose time-aggregation method is approved in
-- `docs/semantics/time_aggregation_methods.json`. Built by
-- `data_ingestion_toolbox.semantics.rollups` from the served monthly rows,
-- replaced whole on each refresh. A window missing a month, or holding a
-- month without a provider value, keeps its row with no value and the
-- reason; it is never zero. Every row is derived, never a provider fact.
CREATE TABLE IF NOT EXISTS gold_fbi.derived_calendar_rollup (
    metric_code         TEXT NOT NULL,
    geo_id              TEXT,
    geo_level           TEXT,
    subject_code        TEXT,
    unit                TEXT,
    grain               TEXT NOT NULL CHECK (grain IN ('quarter', 'year')),
    window_start        DATE NOT NULL,
    window_end          DATE NOT NULL CHECK (window_end > window_start),
    method              TEXT NOT NULL CHECK (method IN ('sum', 'mean')),
    method_version      INTEGER NOT NULL CHECK (method_version >= 1),
    expected_periods    INTEGER NOT NULL CHECK (expected_periods IN (3, 12)),
    present_periods     INTEGER NOT NULL
        CHECK (present_periods BETWEEN 0 AND expected_periods),
    value               NUMERIC,
    refusal_reason      TEXT,
    component_releases  TEXT[] NOT NULL,
    derived             BOOLEAN NOT NULL DEFAULT TRUE CHECK (derived),
    refreshed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- A complete window has a value and no reason; any other has a reason
    -- and no value.
    CONSTRAINT fbi_rollup_value_or_reason CHECK (
        (present_periods = expected_periods AND value IS NOT NULL
             AND refusal_reason IS NULL)
        OR (present_periods < expected_periods AND value IS NULL
             AND refusal_reason IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_fbi_derived_calendar_rollup
    ON gold_fbi.derived_calendar_rollup (
        metric_code, COALESCE(geo_id, ''), COALESCE(subject_code, ''),
        grain, window_start
    );

-- Every FBI UCR figure for a calendar window, for `/observations` with
-- `time_grain=quarterly|annual` (ADR-0007, RU-6). FBI publishes no calendar
-- totals of its own here, so every row is derived; a window missing a month
-- is served with no value and its reason.
CREATE OR REPLACE VIEW gold_fbi.calendar_window_observation AS
SELECT derived.metric_code,
       derived.geo_id,
       derived.geo_level,
       derived.subject_code,
       derived.unit,
       derived.grain,
       derived.window_start AS period_start,
       derived.window_end AS period_end,
       derived.value,
       CASE WHEN derived.value IS NULL THEN 'incomplete_window' ELSE 'reported' END
           AS value_status,
       'derived'::TEXT AS derivation_kind,
       derived.method,
       derived.method_version,
       derived.expected_periods,
       derived.present_periods,
       derived.refusal_reason,
       derived.component_releases,
       derived.refreshed_at::DATE AS as_of_date
FROM gold_fbi.derived_calendar_rollup AS derived;

COMMENT ON SCHEMA gold_fbi IS
    'Policy-free publication views for validated FBI UCR observations and coverage.';

COMMENT ON VIEW gold_fbi.agency_observation_area_filter IS
    'County/place filters over agency-grain observations; never a county or city total.';

COMMENT ON VIEW gold_fbi.county_rollup IS
    'Declared-derived county roll-up of agency-reported absolute totals; '
    'multi-county agencies count in full in each county, non-reporting '
    'months are never zero, and no rate is derived.';

COMMENT ON VIEW gold_fbi.measure_export IS
    'Provider-neutral glossary publisher contract; owns no gold_glossary objects.';
