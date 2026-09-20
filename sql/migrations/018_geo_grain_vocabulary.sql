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

-- The two publisher views this step replaced are no longer restated here.
-- They are defined by the files their sources own and the DAGs re-apply:
-- `src/data_ingestion_toolbox/cdc/gold_cdc/DDL/publisher.sql` and
-- `src/data_ingestion_toolbox/usda_nass/gold_nass/DDL/publisher.sql`, both of
-- which derive their grains through the function above. What remains here is
-- the vocabulary itself.
