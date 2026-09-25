-- Provider-neutral glossary publisher contract for FBI UCR.
--
-- One row per measure identity, with `valid_geo_grains` derived through
-- `gold_glossary.geo_grain` like every other publisher's. Applied in the
-- `publisher` phase and re-applied by the `fbi_ucr_ingest` DAG's
-- `ensure_fbi_schema` task.

-- The FBI publisher's grains through the one vocabulary mapping.
--
-- Migration 018 said the mapping "lives here once. Publisher views call it to
-- say what they publish; the API's dispatch entries call it to say what they
-- serve. A mapping written in five places is how this defect happened." It
-- routed two publishers through `gold_glossary.geo_grain`; 021 moved the
-- function ahead of the `gold` and `publisher` phases so the rest could
-- follow, and this step is the one publisher whose last definition sits in a
-- migration (020) rather than a phase file -- `sql/migrations/README.md`
-- forbids editing a step other shared environments may already hold, so it is
-- replaced here instead.
--
-- No published grain changes: FBI's `subject_type` is 'national', 'state' or
-- 'agency' by constraint, and the function maps those to the same words
-- `UPPER()` did. The resolved-geography predicate 020 added stays (DB-037).

CREATE OR REPLACE VIEW gold_fbi.metric_publisher AS
SELECT 'FBI_UCR'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       measure.product_id || ':' || measure.measure_id AS source_object_key,
       'measure'::TEXT AS source_object_type,
       (measure.offense_label || ' ' || measure.counted_entity_basis || ' ('
        || measure.measure_form || ')')::TEXT AS metric_display_name,
       measure.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       measure.valid_geo_grains,
       ARRAY['MONTHLY']::TEXT[] AS valid_time_grains,
       CASE WHEN measure.measure_form = 'absolute_total'
            THEN 'additive_within_subject'
            ELSE 'non_additive' END::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_fbi',
           'relation', 'crime_observation',
           'product_id', measure.product_id,
           'measure_id', measure.measure_id
       ) AS physical_lineage,
       release.release_key::TEXT AS source_watermark,
       release.source_run_id,
       release.published_at AS publication_time,
       'Federal Bureau of Investigation Uniform Crime Reporting Program'::TEXT
           AS source_name,
       'government-law-enforcement'::TEXT AS source_type,
       release.methodology_url::TEXT AS reference_url
FROM (
    SELECT measure.product_id, measure.measure_id, measure.offense_label,
           measure.counted_entity_basis, measure.measure_form, measure.unit,
           -- A grain is one a value is published at: a grain where every row is
           -- withheld (Census publishes B24114 nationally only) is no map to offer.
           COALESCE(
               ARRAY_AGG(DISTINCT gold_glossary.geo_grain(fact.subject_type)
                         ORDER BY gold_glossary.geo_grain(fact.subject_type))
                   FILTER (WHERE fact.value IS NOT NULL),
               ARRAY[]::TEXT[]
           )::TEXT[]
               AS valid_geo_grains
    FROM silver_fbi.dim_offense_measure AS measure
    JOIN silver_fbi.fact_crime_observation AS fact
      ON fact.product_id = measure.product_id
     AND fact.measure_id = measure.measure_id
    JOIN silver_fbi.dim_ucr_dataset_release AS release
      ON release.product_id = fact.product_id
     AND release.release_key = fact.release_key
    WHERE release.status = 'published'
      AND fact.geography_status NOT IN ('ambiguous', 'unsupported')
    GROUP BY measure.product_id, measure.measure_id, measure.offense_label,
             measure.counted_entity_basis, measure.measure_form, measure.unit
) AS measure
JOIN LATERAL (
    SELECT candidate.*
    FROM silver_fbi.dim_ucr_dataset_release AS candidate
    WHERE candidate.product_id = measure.product_id
      AND candidate.status = 'published'
    ORDER BY candidate.refresh_date DESC
    LIMIT 1
) AS release ON TRUE;
