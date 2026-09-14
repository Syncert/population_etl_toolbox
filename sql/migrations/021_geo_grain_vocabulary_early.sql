-- The grain vocabulary, defined before the phases whose views call it.
--
-- Migration 018 created `gold_glossary.geo_grain(text)` and said why: "The
-- mapping lives here once. Publisher views call it to say what they publish;
-- the API's dispatch entries call it to say what they serve. A mapping
-- written in five places is how this defect happened." It then routed two
-- publishers -- CDC and USDA NASS -- through the function, and the bootstrap
-- manifest applies 018 in the `glossary` phase: after the `gold` and
-- `publisher` phases. A publisher DDL calling the function would therefore
-- fail at CREATE VIEW time on a fresh bootstrap.
--
-- So the definition moves earlier, beside the schema it lives in, and 018 is
-- left as it shipped (`sql/migrations/README.md`: do not edit a step other
-- shared environments depend on; add the next one). 018's own
-- CREATE OR REPLACE of the same body is then a no-op that keeps that step
-- readable on its own.
--
-- Nothing about the vocabulary changes: NATIONAL, STATE, COUNTY, PLACE,
-- AGENCY, with NATION and US as aliases, and an unknown word passed through
-- upper-cased so it surfaces as itself in the catalog instead of hiding
-- inside a familiar one (DB-037).

CREATE SCHEMA IF NOT EXISTS gold_glossary;

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
    'NATIONAL, STATE, COUNTY, PLACE, AGENCY. Publishers, serving views, and the '
    'API dispatch all go through this so no two of them can diverge.';
