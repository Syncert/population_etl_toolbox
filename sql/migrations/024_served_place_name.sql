-- 024: the serving relations carry the name the catalog names a place by.
--
-- `gold_glossary.dim_geography` names a geography with place_name first; the
-- six observation contract views named it with county_name first, because
-- `place_name` was not in the relations they read. Migration 023 made the
-- expression one function; this gives those relations the argument it needs,
-- so the views can call it rather than approximate it.
--
-- The column is appended rather than placed beside `county_name` (where the
-- table DDL now declares it for a fresh bootstrap) because `ALTER TABLE ADD
-- COLUMN` can only append. What matters is that `rpt_*` and `mv_*` stay in
-- the same order as each other: the latest-value refreshes insert `d.*` from
-- the reporting table into the latest table positionally, and both are
-- altered here in the same step.

ALTER TABLE gold_census.rpt_acs_observations
    ADD COLUMN IF NOT EXISTS place_name TEXT;
ALTER TABLE gold_census.mv_acs_latest
    ADD COLUMN IF NOT EXISTS place_name TEXT;

ALTER TABLE gold_bls.rpt_bls_observations
    ADD COLUMN IF NOT EXISTS place_name TEXT;
ALTER TABLE gold_bls.mv_bls_latest
    ADD COLUMN IF NOT EXISTS place_name TEXT;

ALTER TABLE gold_fred.rpt_fred_observations
    ADD COLUMN IF NOT EXISTS place_name TEXT;
ALTER TABLE gold_fred.mv_fred_latest
    ADD COLUMN IF NOT EXISTS place_name TEXT;

-- Backfill what is already served. A row whose geography is a place was
-- published with its state's name; the next reserve would fix it, but a
-- forced re-serve of 68 million ACS rows is not a migration step.
UPDATE gold_census.rpt_acs_observations AS r
   SET place_name = g.place_name
  FROM gold_glossary.dim_geo_latest AS g
 WHERE g.geo_id = r.geo_id
   AND g.place_name IS NOT NULL
   AND r.place_name IS DISTINCT FROM g.place_name;

UPDATE gold_bls.rpt_bls_observations AS r
   SET place_name = g.place_name
  FROM gold_glossary.dim_geo_latest AS g
 WHERE g.geo_id = r.geo_id
   AND g.place_name IS NOT NULL
   AND r.place_name IS DISTINCT FROM g.place_name;

UPDATE gold_fred.rpt_fred_observations AS r
   SET place_name = g.place_name
  FROM gold_glossary.dim_geo_latest AS g
 WHERE g.geo_id = r.geo_id
   AND g.place_name IS NOT NULL
   AND r.place_name IS DISTINCT FROM g.place_name;

UPDATE gold_census.mv_acs_latest AS m
   SET place_name = g.place_name
  FROM gold_glossary.dim_geo_latest AS g
 WHERE g.geo_id = m.geo_id
   AND g.place_name IS NOT NULL
   AND m.place_name IS DISTINCT FROM g.place_name;

UPDATE gold_bls.mv_bls_latest AS m
   SET place_name = g.place_name
  FROM gold_glossary.dim_geo_latest AS g
 WHERE g.geo_id = m.geo_id
   AND g.place_name IS NOT NULL
   AND m.place_name IS DISTINCT FROM g.place_name;

UPDATE gold_fred.mv_fred_latest AS m
   SET place_name = g.place_name
  FROM gold_glossary.dim_geo_latest AS g
 WHERE g.geo_id = m.geo_id
   AND g.place_name IS NOT NULL
   AND m.place_name IS DISTINCT FROM g.place_name;
