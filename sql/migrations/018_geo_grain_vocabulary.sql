-- One geography-grain vocabulary for the catalog and the serving routes.
--
-- The consumer guide promises that a served row's geo_level is always one of
-- a small set of words and that the catalog's valid_geo_grains uses the same
-- words, so a grain read from the catalog can be sent straight back as a
-- filter. Three publishers derived their grains as UPPER(geo_type), whose
-- national value is 'nation' -- so they published NATION while BLS, ACS, FRED,
-- and FBI published NATIONAL. USDA NASS then filtered on agg_level_desc, whose
-- national value is NATIONAL, so its own catalog word could never reach its
-- own rows: every national NASS statistic was unanswerable by construction,
-- and nothing reported it because STATE and COUNTY happened to coincide.
--
-- The mapping lives here once. Publisher views call it to say what they
-- publish; the API's dispatch entries call it to say what they serve. A
-- mapping written in five places is how this defect happened.
--
-- The vocabulary is what the warehouse actually serves, not the three words
-- the guide used to name: NATIONAL, STATE, COUNTY, PLACE (Census PEP), and
-- AGENCY (FBI UCR). Anything else is passed through upper-cased rather than
-- silently folded, so an unknown grain surfaces as itself in the catalog
-- instead of hiding inside a familiar word.

CREATE OR REPLACE FUNCTION gold_glossary.geo_grain(source_grain TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE UPPER(TRIM(source_grain))
               WHEN 'NATION'   THEN 'NATIONAL'
               WHEN 'NATIONAL' THEN 'NATIONAL'
               WHEN 'US'       THEN 'NATIONAL'
               ELSE UPPER(TRIM(source_grain))
           END
$$;

COMMENT ON FUNCTION gold_glossary.geo_grain(TEXT) IS
    'The geography-grain vocabulary served rows carry and the catalog publishes: '
    'NATIONAL, STATE, COUNTY, PLACE, AGENCY. Publishers and the API dispatch both '
    'go through this so the catalog word and the serving filter cannot diverge.';

-- The CDC publisher as replaced in 014, with grains through the vocabulary.
CREATE OR REPLACE VIEW gold_cdc.metric_publisher AS
SELECT 'CDC'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       measure.asset_id || ':' || measure.measure_id || ':' ||
           measure.value_type_id AS source_object_key,
       'measure'::TEXT AS source_object_type,
       measure.measure_label::TEXT AS metric_display_name,
       measure.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       measure.valid_geo_grains,
       ARRAY['ANNUAL']::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_cdc',
           'relation', 'health_observation',
           'asset_id', measure.asset_id,
           'measure_id', measure.measure_id,
           'value_type_id', measure.value_type_id
       ) AS physical_lineage,
       release.release_watermark::TEXT AS source_watermark,
       release.source_run_id,
       release.published_at AS publication_time,
       'Centers for Disease Control and Prevention'::TEXT AS source_name,
       'government-public-health'::TEXT AS source_type,
       release.methodology_url::TEXT AS reference_url
FROM (
    SELECT measure.asset_id, measure.measure_id, measure.value_type_id,
           measure.measure_label, measure.unit,
           ARRAY_AGG(DISTINCT gold_glossary.geo_grain(fact.geo_type)
                     ORDER BY gold_glossary.geo_grain(fact.geo_type))::TEXT[]
               AS valid_geo_grains
    FROM silver_cdc.dim_measure AS measure
    JOIN silver_cdc.fact_health_observation AS fact
      ON fact.asset_id = measure.asset_id
     AND fact.measure_id = measure.measure_id
     AND fact.value_type_id = measure.value_type_id
    JOIN silver_cdc.dim_dataset_release AS release
      ON release.asset_id = fact.asset_id
     AND release.release_watermark = fact.release_watermark
    WHERE release.status = 'published'
    GROUP BY measure.asset_id, measure.measure_id, measure.value_type_id,
             measure.measure_label, measure.unit
) AS measure
JOIN LATERAL (
    SELECT candidate.*
    FROM silver_cdc.dim_dataset_release AS candidate
    WHERE candidate.asset_id = measure.asset_id
      AND candidate.status = 'published'
    ORDER BY candidate.release_watermark::BIGINT DESC
    LIMIT 1
) AS release ON TRUE;

-- The USDA NASS publisher as replaced in 014, with grains through the
-- vocabulary. The served relation filters on agg_level_desc; geo_type and
-- agg_level_desc name the same grain per row, and the function maps both
-- spellings to the one word the route matches.
CREATE OR REPLACE VIEW gold_nass.metric_publisher AS
SELECT 'USDA_NASS'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       (statistic.product_id || ':' || statistic.statistic_sk)::TEXT
           AS source_object_key,
       'statistic'::TEXT AS source_object_type,
       statistic.short_desc::TEXT AS metric_display_name,
       statistic.unit_desc::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       statistic.valid_geo_grains,
       statistic.valid_time_grains,
       statistic.additive_behavior::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_nass',
           'relation', 'crop_observation',
           'product_id', statistic.product_id,
           'statistic_sk', statistic.statistic_sk,
           'statisticcat_desc', statistic.statisticcat_desc,
           'unit_desc', statistic.unit_desc
       ) AS physical_lineage,
       release.release_watermark::TEXT AS source_watermark,
       release.source_run_id,
       release.published_at AS publication_time,
       'USDA National Agricultural Statistics Service'::TEXT AS source_name,
       'government-agricultural-statistics'::TEXT AS source_type,
       release.methodology_url::TEXT AS reference_url
FROM (
    SELECT fact.product_id, fact.statistic_sk, statistic.short_desc,
           statistic.unit_desc, statistic.statisticcat_desc,
           statistic.additive_behavior,
           ARRAY_AGG(DISTINCT gold_glossary.geo_grain(fact.geo_type)
                     ORDER BY gold_glossary.geo_grain(fact.geo_type))::TEXT[]
               AS valid_geo_grains,
           ARRAY_AGG(DISTINCT UPPER(statistic.freq_desc)
                     ORDER BY UPPER(statistic.freq_desc))::TEXT[]
               AS valid_time_grains
    FROM silver_nass.dim_statistic AS statistic
    JOIN silver_nass.fact_crop_observation AS fact
      ON fact.statistic_sk = statistic.statistic_sk
    JOIN silver_nass.dim_dataset_release AS release
      ON release.product_id = fact.product_id
     AND release.release_watermark = fact.release_watermark
    WHERE release.status = 'published'
    GROUP BY fact.product_id, fact.statistic_sk, statistic.short_desc,
             statistic.unit_desc, statistic.statisticcat_desc,
             statistic.additive_behavior
) AS statistic
JOIN LATERAL (
    SELECT candidate.*
    FROM silver_nass.dim_dataset_release AS candidate
    WHERE candidate.product_id = statistic.product_id
      AND candidate.status = 'published'
    ORDER BY candidate.release_watermark DESC
    LIMIT 1
) AS release ON TRUE;
