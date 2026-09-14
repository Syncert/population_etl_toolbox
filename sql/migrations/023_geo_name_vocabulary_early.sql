-- 023: one definition of a geography's published name, installed early.
--
-- `gold_glossary.dim_geography` named a geography
-- COALESCE(place_name, county_name, state_name, geo_id) and all six
-- observation contract views named it COALESCE(county_name, state_name,
-- geo_id). A place therefore answered under its place name on the catalog
-- route and under its state's name on every observation route -- the same
-- geography, two names, and nothing that could notice because each expression
-- was locally correct.
--
-- This runs in the `glossary-migration` phase, before any relation that calls
-- it is created, for the same reason 021 installs `geo_grain` there: a view's
-- body is resolved when the view is created, so a function a view calls has to
-- exist first. A fresh bootstrap and a migrated database then get the
-- definition in the same order.

CREATE SCHEMA IF NOT EXISTS gold_glossary;

CREATE OR REPLACE FUNCTION gold_glossary.geo_name(
    p_place_name  TEXT,
    p_county_name TEXT,
    p_state_name  TEXT,
    p_geo_id      TEXT
)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    -- Most specific name the row carries, and `geo_id` last so a geography
    -- with no name at all is still identified rather than served as NULL.
    SELECT COALESCE(p_place_name, p_county_name, p_state_name, p_geo_id);
$$;

COMMENT ON FUNCTION gold_glossary.geo_name(TEXT, TEXT, TEXT, TEXT) IS
    'The published name of a geography. Called by gold_glossary.dim_geography '
    'and by every observation contract view; never transcribed (DB-038).';
