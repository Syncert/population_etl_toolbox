-- sql/gold_contract/002_gold_glossary_schema.sql
-- gold_glossary schema: shared multi-source metadata tables and catalog views.
--
-- This schema is the single source of truth for cross-source metadata.
-- Source-specific schemas (gold_bls, gold_census, gold_fred) reference these objects.

CREATE SCHEMA IF NOT EXISTS gold_glossary;

-- ─────────────────────────────────────────────────────────────────────────────
-- Source system registry (shared across all sources)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold_glossary.dim_source_system (
    source_system_sk BIGSERIAL PRIMARY KEY,
    source_code      TEXT NOT NULL UNIQUE,
    source_name      TEXT NOT NULL,
    source_type      TEXT NOT NULL CHECK (source_type IN ('PRIMARY', 'REPUBLISHER', 'CURATED')),
    reference_url    TEXT,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO gold_glossary.dim_source_system (source_code, source_name, source_type, reference_url)
VALUES
    ('CENSUS_ACS', 'US Census ACS',                 'PRIMARY',     'https://www.census.gov/programs-surveys/acs'),
    ('BLS',        'Bureau of Labor Statistics',    'PRIMARY',     'https://www.bls.gov/'),
    ('FRED',       'Federal Reserve Economic Data', 'REPUBLISHER', 'https://fred.stlouisfed.org/')
ON CONFLICT (source_code) DO UPDATE
SET source_name   = EXCLUDED.source_name,
    source_type   = EXCLUDED.source_type,
    reference_url = EXCLUDED.reference_url,
    updated_at    = NOW();

-- ─────────────────────────────────────────────────────────────────────────────
-- Metric catalog (shared across all sources)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold_glossary.dim_metric_catalog (
    metric_catalog_sk      BIGSERIAL PRIMARY KEY,
    metric_code            TEXT NOT NULL UNIQUE,
    metric_display_name    TEXT NOT NULL,
    source_code            TEXT NOT NULL REFERENCES gold_glossary.dim_source_system(source_code),
    source_object_type     TEXT NOT NULL,
    source_object_key      TEXT,
    valid_geo_grains       TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    valid_time_grains      TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    units                  TEXT,
    measure_kind           TEXT,
    aggregation_characteristic TEXT,
    physical_lineage       JSONB NOT NULL DEFAULT '{}'::JSONB,
    publisher_contract_version TEXT,
    source_watermark       TEXT,
    source_run_id          UUID,
    publication_time       TIMESTAMPTZ,
    harvested_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    freshness_state        TEXT NOT NULL DEFAULT 'current'
        CHECK (freshness_state IN ('current', 'stale', 'retired')),
    missing_harvest_count  INTEGER NOT NULL DEFAULT 0
        CHECK (missing_harvest_count >= 0),
    UNIQUE (source_code, source_object_type, source_object_key)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Bridge tables (metric ↔ source-specific series/variables)
-- ─────────────────────────────────────────────────────────────────────────────
-- ─────────────────────────────────────────────────────────────────────────────
-- Geography dimension (shared across all sources)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold_glossary.dim_geo_latest (
    geo_id       TEXT PRIMARY KEY,
    geo_level    TEXT,
    state_fips   TEXT,
    county_fips  TEXT,
    place_fips   TEXT,
    state_name   TEXT,
    county_name  TEXT,
    place_name   TEXT,
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    geo_geom     geometry(MultiPolygon, 4326),
    boundary_vintage INTEGER,
    -- A geography the reference stops listing is retired, not forgotten
    -- (DB-038). The served relations keep its observations, so deleting the
    -- catalog row left rows nothing could qualify: a client resolving
    -- geographies through the catalog could not reach them, and one that did
    -- not got rows the catalog would not name. This mirrors
    -- `dim_metric_catalog.freshness_state`: the state is published, and the
    -- consumer decides.
    geography_state TEXT NOT NULL DEFAULT 'current',
    retired_at   TIMESTAMPTZ,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE gold_glossary.dim_geo_latest
    ADD COLUMN IF NOT EXISTS place_fips TEXT,
    ADD COLUMN IF NOT EXISTS place_name TEXT,
    -- A tract's or ZCTA's own name ("Census Tract 401", "ZCTA5 19901");
    -- `county_name` stays the tract's county (sub-county-geography).
    ADD COLUMN IF NOT EXISTS area_name TEXT,
    ADD COLUMN IF NOT EXISTS boundary_vintage INTEGER,
    ADD COLUMN IF NOT EXISTS geography_state TEXT NOT NULL DEFAULT 'current',
    ADD COLUMN IF NOT EXISTS retired_at TIMESTAMPTZ;

-- Drop-then-add so re-applying this file is idempotent.
ALTER TABLE gold_glossary.dim_geo_latest
    DROP CONSTRAINT IF EXISTS ck_dim_geo_latest_geography_state;
ALTER TABLE gold_glossary.dim_geo_latest
    ADD CONSTRAINT ck_dim_geo_latest_geography_state
    CHECK (geography_state IN ('current', 'retired'));

CREATE INDEX IF NOT EXISTS ix_gold_glossary_dim_geo_latest_state
    ON gold_glossary.dim_geo_latest (geography_state);

CREATE INDEX IF NOT EXISTS ix_gold_glossary_dim_geo_latest_geom
    ON gold_glossary.dim_geo_latest USING GIST (geo_geom);

DROP PROCEDURE IF EXISTS gold_glossary.refresh_dim_geo_latest();
CREATE OR REPLACE PROCEDURE gold_glossary.refresh_dim_geo_latest()
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('gold_glossary.refresh_dim_geo_latest'));

    INSERT INTO gold_glossary.dim_geo_latest (
        geo_id,
        geo_level,
        state_fips,
        county_fips,
        place_fips,
        state_name,
        county_name,
        place_name,
        latitude,
        longitude,
        geo_geom,
        boundary_vintage,
        geography_state,
        retired_at,
        refreshed_at,
        area_name
    )
    SELECT DISTINCT ON (g.geo_id)
        g.geo_id,
        -- `gold_glossary.geo_grain` is this CASE, and `dim_geo_current` has
        -- already mapped the entity's own `geo_type` ('nation' -> 'us')
        -- before the projection sees it, so 'us' is what arrives here and
        -- the function's US alias is what matches it. Both hops stay
        -- visible: collapsing the first onto the function would make
        -- 'nation' arrive where only 'us' is matched (DB-037).
        gold_glossary.geo_grain(g.geo_level),
        CASE WHEN g.state_fips  IS NOT NULL THEN LPAD(g.state_fips::TEXT,  2, '0') ELSE NULL END,
        CASE WHEN g.county_fips IS NOT NULL THEN LPAD(g.county_fips::TEXT, 3, '0') ELSE NULL END,
        CASE WHEN g.place_fips IS NOT NULL THEN LPAD(g.place_fips::TEXT, 5, '0') ELSE NULL END,
        g.state_name,
        g.county_name,
        g.place_name,
        g.latitude,
        g.longitude,
        g.geom,
        g.boundary_vintage,
        'current',
        NULL,
        NOW(),
        g.area_name
    FROM silver_ref.dim_geo g
    WHERE g.is_active = TRUE
    ORDER BY g.geo_id, g.source_year DESC NULLS LAST, g.ingested_at DESC
    ON CONFLICT (geo_id) DO UPDATE
    SET geo_level = EXCLUDED.geo_level,
        -- A reference that lists a retired geography again un-retires it. This
        -- has to be part of the compared tuple below as well: a geography
        -- retired and then restored with byte-identical attributes would
        -- otherwise skip the update and stay `retired` forever.
        geography_state = 'current',
        retired_at = NULL,
        state_fips = EXCLUDED.state_fips,
        county_fips = EXCLUDED.county_fips,
        place_fips = EXCLUDED.place_fips,
        state_name = EXCLUDED.state_name,
        county_name = EXCLUDED.county_name,
        place_name = EXCLUDED.place_name,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        geo_geom = EXCLUDED.geo_geom,
        boundary_vintage = EXCLUDED.boundary_vintage,
        area_name = EXCLUDED.area_name,
        refreshed_at = NOW()
    WHERE (
        gold_glossary.dim_geo_latest.geo_level,
        gold_glossary.dim_geo_latest.state_fips,
        gold_glossary.dim_geo_latest.county_fips,
        gold_glossary.dim_geo_latest.place_fips,
        gold_glossary.dim_geo_latest.state_name,
        gold_glossary.dim_geo_latest.county_name,
        gold_glossary.dim_geo_latest.place_name,
        gold_glossary.dim_geo_latest.latitude,
        gold_glossary.dim_geo_latest.longitude,
        gold_glossary.dim_geo_latest.geo_geom,
        gold_glossary.dim_geo_latest.boundary_vintage,
        gold_glossary.dim_geo_latest.geography_state,
        gold_glossary.dim_geo_latest.area_name
    ) IS DISTINCT FROM (
        EXCLUDED.geo_level,
        EXCLUDED.state_fips,
        EXCLUDED.county_fips,
        EXCLUDED.place_fips,
        EXCLUDED.state_name,
        EXCLUDED.county_name,
        EXCLUDED.place_name,
        EXCLUDED.latitude,
        EXCLUDED.longitude,
        EXCLUDED.geo_geom,
        EXCLUDED.boundary_vintage,
        'current',
        EXCLUDED.area_name
    );

    -- Retire rather than delete (DB-038). `retired_at` records when the
    -- reference stopped listing it and is not overwritten on a later refresh,
    -- so "when did this county go away" survives every sweep after the first.
    -- The attributes stay as last published: they are what the observations
    -- this geography still has were served with.
    UPDATE gold_glossary.dim_geo_latest d
       SET geography_state = 'retired',
           retired_at = COALESCE(d.retired_at, NOW()),
           refreshed_at = NOW()
     WHERE d.geography_state <> 'retired'
       AND NOT EXISTS (
           SELECT 1
           FROM silver_ref.dim_geo g
           WHERE g.is_active = TRUE
             AND g.geo_id = d.geo_id
       );
END;
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- Schema migration state (shared, tracks DDL hash per component)
-- ─────────────────────────────────────────────────────────────────────────────
-- ─────────────────────────────────────────────────────────────────────────────
-- Contract views exposed to the API
-- ─────────────────────────────────────────────────────────────────────────────

-- Metric catalog view
CREATE OR REPLACE VIEW gold_glossary.dim_metric AS
SELECT
    metric_code,
    metric_display_name,
    source_code,
    source_object_type,
    source_object_key,
    units,
    measure_kind,
    valid_geo_grains,
    valid_time_grains,
    aggregation_characteristic,
    physical_lineage,
    publisher_contract_version,
    source_watermark,
    source_run_id,
    publication_time,
    harvested_at,
    freshness_state,
    freshness_state = 'current' AS is_active
FROM gold_glossary.dim_metric_catalog;

-- Geography catalog view
CREATE OR REPLACE VIEW gold_glossary.dim_geography AS
SELECT
    geo_id,
    geo_level,
    state_fips,
    county_fips,
    place_fips,
    state_name,
    county_name,
    place_name,
    latitude,
    longitude,
    latitude AS geo_latitude,
    longitude AS geo_longitude,
    boundary_vintage,
    refreshed_at,
    -- A tract's own name is its most specific one; for every other level
    -- `area_name` is NULL and this is the call it always was.
    gold_glossary.geo_name(COALESCE(area_name, place_name), county_name, state_name, geo_id) AS geo_name,
    -- Published for the same reason `dim_metric` publishes `freshness_state`:
    -- a retired geography stays resolvable, and the consumer decides whether
    -- to show it (DB-038). Its observations are still served, so a catalog
    -- that hid it would leave rows nothing could name.
    geography_state,
    retired_at,
    geography_state = 'current' AS is_active,
    -- Appended: `CREATE OR REPLACE VIEW` can only add columns at the end.
    area_name
FROM gold_glossary.dim_geo_latest;

-- Geography relationships, as the reference recorded them (nearby-and-related-places).
--
-- One row per relationship between two geographies the catalog currently
-- serves, from each parent's newest geography vintage of that relationship
-- type, so a reference that redraws a boundary replaces its neighbours rather
-- than adding to them. Every row carries the type, the vintage and the
-- evidence source the bridge recorded; nothing here is inferred from a name.
-- `contains` is the code hierarchy (nation to state, state to county and
-- place), `intersects` a county and a place whose boundaries overlap, with
-- the overlap's area and its share of the place, and `adjacent` two counties
-- that share a boundary.
CREATE OR REPLACE VIEW gold_glossary.geo_relationship AS
SELECT
    parent.geo_id,
    related.geo_id AS related_geo_id,
    relationship.relationship_type,
    relationship.geography_vintage,
    relationship.overlap_area_m2,
    relationship.overlap_weight,
    relationship.evidence_source
FROM silver_ref.bridge_geo_relationship_version AS relationship
JOIN silver_ref.dim_geo_entity AS parent_entity
     ON parent_entity.geo_sk = relationship.parent_geo_sk
JOIN silver_ref.dim_geo_entity AS related_entity
     ON related_entity.geo_sk = relationship.related_geo_sk
JOIN gold_glossary.dim_geo_latest AS parent
     ON parent.geo_id = parent_entity.geo_id
    AND parent.geography_state = 'current'
JOIN gold_glossary.dim_geo_latest AS related
     ON related.geo_id = related_entity.geo_id
    AND related.geography_state = 'current'
WHERE relationship.relationship_type IN ('contains', 'intersects', 'adjacent')
  AND relationship.geography_vintage = (
        SELECT MAX(newest.geography_vintage)
        FROM silver_ref.bridge_geo_relationship_version AS newest
        WHERE newest.parent_geo_sk = relationship.parent_geo_sk
          AND newest.relationship_type = relationship.relationship_type
      );

-- ─────────────────────────────────────────────────────────────────────────────
-- Finding a place by name or by location (api-place-search)
-- ─────────────────────────────────────────────────────────────────────────────
--
-- Two contracts the catalog serves "find your place" from. Before them the web
-- read every geography page by page and matched names in the browser, and its
-- "use my location" picked the county whose centre point was nearest, which is
-- wrong near a county line and never lands on a city.
--
-- `pg_trgm` gives the fuzzy match its index; `unaccent` folds "Española" and
-- "Espanola" to the same words. Both are PostgreSQL contrib modules the pinned
-- PostGIS image ships.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- The words a name is matched by: lower case, accents removed, `&` spelled
-- "and", every run of anything else one space, and one leading space so a
-- word prefix is `LIKE '% word%'` whether or not it starts the name. The API
-- normalises a query to the same shape (`apps/api/services/place_search.py`).
CREATE OR REPLACE FUNCTION gold_glossary.search_words(name TEXT)
RETURNS TEXT
LANGUAGE sql
STABLE
PARALLEL SAFE
AS $$
    SELECT ' ' || btrim(regexp_replace(
        lower(unaccent(replace(COALESCE(name, ''), '&', ' and '))),
        '[^a-z0-9]+', ' ', 'g'
    ))
$$;

-- One row per current state, county, metro, place and ZCTA. Tracts are not
-- searched by name: their names are numbers, and a reader reaches one from its
-- county or from a location.
--
-- `short_name` drops the Census description a reader does not type ("Austin"
-- for "Austin city", "Travis" for "Travis County", the title before the comma
-- for a metro, the five digits for a ZCTA). The match is case-sensitive on
-- purpose: Census writes the description of a place or an independent city in
-- lower case ("Baltimore city"), and "Carson City" is a name, not "Carson"
-- with a description.
--
-- `geo_name` is the published name (`gold_glossary.geo_name`), and it is also
-- the text the web's address rule slugs: `stateName`, `countyName` and
-- `cityName` in `apps/web/lib/placeChapters.ts` read the state, county or
-- place name, which is what `geo_name` resolves to at those grains. So the
-- API's `page_path` and the address the web resolves come from one name.
--
-- `county_geo_id` is a place's containing county: the county holding the
-- largest share of it by the published overlap weight. A place crosses county
-- lines; this names one county, and `/related` lists every one.
--
-- A metro's `state_fips` is its principal state: the state of its most
-- populous member county. A metro can span states and has no state code.
--
-- `population` is the newest valid ACS 5-year total population
-- (`CENSUS_ACS:acs5:B01003_001`), carried with its year and metric code. It
-- ranks matches and is served labelled; it is not a second published series.
CREATE TABLE IF NOT EXISTS gold_glossary.geo_search (
    geo_id                 TEXT PRIMARY KEY,
    geo_level              TEXT NOT NULL,
    geo_name               TEXT NOT NULL,
    short_name             TEXT NOT NULL,
    name_words             TEXT NOT NULL,
    short_words            TEXT NOT NULL,
    state_fips             TEXT,
    county_fips            TEXT,
    place_fips             TEXT,
    state_name             TEXT,
    county_geo_id          TEXT,
    county_name            TEXT,
    population             NUMERIC,
    population_year        INTEGER,
    population_metric_code TEXT,
    boundary_vintage       INTEGER,
    refreshed_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_geo_search_grain
        CHECK (geo_level IN ('STATE', 'COUNTY', 'METRO', 'PLACE', 'ZCTA'))
);

CREATE INDEX IF NOT EXISTS ix_gold_glossary_geo_search_name_trgm
    ON gold_glossary.geo_search USING GIN (name_words gin_trgm_ops);
CREATE INDEX IF NOT EXISTS ix_gold_glossary_geo_search_short_trgm
    ON gold_glossary.geo_search USING GIN (short_words gin_trgm_ops);
CREATE INDEX IF NOT EXISTS ix_gold_glossary_geo_search_state
    ON gold_glossary.geo_search (state_fips, geo_level);

-- Rebuilt whole from the geography projection, the relationship view and the
-- served ACS population. A delete and an insert in one transaction rather than
-- a truncate: a truncate takes ACCESS EXCLUSIVE and every search would wait
-- behind the rebuild, where this way readers keep the previous rows until it
-- commits. Called by the `glossary_reconciliation` DAG directly after
-- `refresh_dim_geo_latest`.
DROP PROCEDURE IF EXISTS gold_glossary.refresh_geo_search();
CREATE OR REPLACE PROCEDURE gold_glossary.refresh_geo_search()
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('gold_glossary.refresh_geo_search'));

    DROP TABLE IF EXISTS pg_temp.geo_search_population;
    CREATE TEMP TABLE geo_search_population (
        geo_id      TEXT PRIMARY KEY,
        population  NUMERIC,
        year        INTEGER,
        metric_code TEXT
    ) ON COMMIT DROP;

    -- ACS is installed by its own source DDL. A warehouse without it still
    -- gets a searchable projection, ranked by name alone, rather than a
    -- failed refresh.
    IF to_regclass('gold_census.mv_acs_latest') IS NOT NULL THEN
        INSERT INTO pg_temp.geo_search_population (geo_id, population, year, metric_code)
        SELECT DISTINCT ON (served.geo_id)
               served.geo_id, served.value, served.vintage_year, served.metric_code
        FROM gold_census.mv_acs_latest AS served
        WHERE served.metric_code = 'CENSUS_ACS:acs5:B01003_001'
          AND served.value_status = 'valid'
          AND served.value IS NOT NULL
        ORDER BY served.geo_id, served.vintage_year DESC;
    END IF;

    DELETE FROM gold_glossary.geo_search;

    INSERT INTO gold_glossary.geo_search (
        geo_id, geo_level, geo_name, short_name, name_words,
        short_words, state_fips, county_fips, place_fips, state_name,
        county_geo_id, county_name, population, population_year,
        population_metric_code, boundary_vintage, refreshed_at
    )
    WITH current_geo AS (
        SELECT geo_id, geo_level, geo_name, state_fips, county_fips,
               place_fips, state_name, county_name, place_name,
               boundary_vintage
        FROM gold_glossary.dim_geography
        WHERE geography_state = 'current'
          AND geo_level IN (
                gold_glossary.geo_grain('state'),
                gold_glossary.geo_grain('county'),
                gold_glossary.geo_grain('metro'),
                gold_glossary.geo_grain('place'),
                gold_glossary.geo_grain('zcta')
              )
    ),
    place_county AS (
        SELECT DISTINCT ON (link.related_geo_id)
               link.related_geo_id AS place_geo_id,
               link.geo_id AS county_geo_id
        FROM gold_glossary.geo_relationship AS link
        JOIN current_geo AS county
             ON county.geo_id = link.geo_id
            AND county.geo_level = gold_glossary.geo_grain('county')
        WHERE link.relationship_type = 'intersects'
        ORDER BY link.related_geo_id, link.overlap_weight DESC NULLS LAST, link.geo_id
    ),
    metro_state AS (
        SELECT DISTINCT ON (link.geo_id)
               link.geo_id AS metro_geo_id,
               county.state_fips
        FROM gold_glossary.geo_relationship AS link
        JOIN current_geo AS metro
             ON metro.geo_id = link.geo_id
            AND metro.geo_level = gold_glossary.geo_grain('metro')
        JOIN current_geo AS county
             ON county.geo_id = link.related_geo_id
            AND county.geo_level = gold_glossary.geo_grain('county')
        LEFT JOIN pg_temp.geo_search_population AS pop
             ON pop.geo_id = county.geo_id
        WHERE link.relationship_type = 'contains'
        ORDER BY link.geo_id, pop.population DESC NULLS LAST, county.geo_id
    ),
    named AS (
        SELECT geo.*,
               COALESCE(NULLIF(btrim(CASE geo.geo_level
                   WHEN gold_glossary.geo_grain('place') THEN regexp_replace(
                       geo.geo_name,
                       '\s+(city|town|township|village|borough|municipality|CDP|comunidad|zona urbana)$',
                       '')
                   WHEN gold_glossary.geo_grain('county') THEN regexp_replace(
                       geo.geo_name,
                       '\s+(County|Parish|Borough|Census Area|Municipality|City and Borough|Planning Region|city)$',
                       '')
                   WHEN gold_glossary.geo_grain('metro') THEN regexp_replace(
                       split_part(geo.geo_name, ',', 1), '\s+(Metro|Micro) Area$', '')
                   WHEN gold_glossary.geo_grain('zcta') THEN regexp_replace(geo.geo_name, '^ZCTA5?\s+', '')
                   ELSE geo.geo_name
               END), ''), geo.geo_name) AS short_name
        FROM current_geo AS geo
    )
    SELECT named.geo_id,
           named.geo_level,
           named.geo_name,
           named.short_name,
           gold_glossary.search_words(named.geo_name),
           gold_glossary.search_words(named.short_name),
           COALESCE(named.state_fips, metro_state.state_fips),
           named.county_fips,
           named.place_fips,
           state.geo_name,
           place_county.county_geo_id,
           county.geo_name,
           pop.population,
           pop.year,
           pop.metric_code,
           named.boundary_vintage,
           NOW()
    FROM named
    LEFT JOIN place_county
         ON place_county.place_geo_id = named.geo_id
        AND named.geo_level = gold_glossary.geo_grain('place')
    LEFT JOIN current_geo AS county
         ON county.geo_id = place_county.county_geo_id
    LEFT JOIN metro_state
         ON metro_state.metro_geo_id = named.geo_id
    LEFT JOIN current_geo AS state
         ON state.geo_level = gold_glossary.geo_grain('state')
        AND state.state_fips = COALESCE(named.state_fips, metro_state.state_fips)
    LEFT JOIN pg_temp.geo_search_population AS pop
         ON pop.geo_id = named.geo_id;
END;
$$;

-- The current boundary of every geography that has one, for locating a point.
-- The geography view carries no geometry (a page of the catalog would be
-- megabytes), so "which place contains this point" reads here. Retired
-- boundaries are excluded: a reader standing somewhere today is in today's
-- geographies. The GiST index on `dim_geo_latest.geo_geom` answers it.
CREATE OR REPLACE VIEW gold_glossary.geo_boundary AS
SELECT latest.geo_id,
       latest.geo_level,
       gold_glossary.geo_name(
           COALESCE(latest.area_name, latest.place_name),
           latest.county_name, latest.state_name, latest.geo_id
       ) AS geo_name,
       latest.state_fips,
       latest.county_fips,
       latest.place_fips,
       latest.boundary_vintage,
       latest.geo_geom
FROM gold_glossary.dim_geo_latest AS latest
WHERE latest.geography_state = 'current'
  AND latest.geo_geom IS NOT NULL;

-- ─────────────────────────────────────────────────────────────────────────────
-- Boundaries by vintage, for the place and metro tile layers
-- (tiles-places-and-metros)
-- ─────────────────────────────────────────────────────────────────────────────
-- `dim_geo_latest` keeps one geometry per geography, the newest. A place
-- annexes, incorporates or disincorporates, and OMB redelineates metros, so a
-- map of an earlier year drawn on today's boundaries colours the wrong shape,
-- and a place that no longer exists cannot be drawn at all. This holds one row
-- per geography and boundary vintage the reference loaded, for the two grains
-- whose tile layers are versioned, and the tile functions in
-- `001_gold_contract_views.sql` serve exactly the vintage they are asked for.
--
-- The geometry is stored in Web Mercator because that is the only projection
-- a tile is cut in: transforming 32,000 place polygons on every request is
-- work the warehouse can do once per vintage. Rows carry no observation; a
-- map colours them by joining the published values on `geo_id`, so a
-- geography with no published value is drawn uncoloured, never as zero.
CREATE TABLE IF NOT EXISTS gold_glossary.tile_boundary_version (
    geo_id            TEXT NOT NULL,
    geo_level         TEXT NOT NULL,
    boundary_vintage  INTEGER NOT NULL,
    geo_name          TEXT NOT NULL,
    state_fips        TEXT,
    state_name        TEXT,
    place_fips        TEXT,
    area_code         TEXT,
    lsad              TEXT,
    latitude          DOUBLE PRECISION,
    longitude         DOUBLE PRECISION,
    geometry_checksum TEXT NOT NULL,
    tile_geom         geometry(MultiPolygon, 3857) NOT NULL,
    refreshed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (geo_id, boundary_vintage),
    CONSTRAINT ck_tile_boundary_version_grain
        CHECK (geo_level IN ('PLACE', 'METRO'))
);

CREATE INDEX IF NOT EXISTS ix_gold_glossary_tile_boundary_version_geom
    ON gold_glossary.tile_boundary_version USING GIST (tile_geom);
-- Which vintages a grain holds, and the newest one, are read on every tile
-- request that names no vintage.
CREATE INDEX IF NOT EXISTS ix_gold_glossary_tile_boundary_version_vintage
    ON gold_glossary.tile_boundary_version (geo_level, boundary_vintage);

-- Brought in line with the reference incrementally: a boundary is rewritten
-- only when its checksum changed, and one the reference no longer holds is
-- removed. Comparing checksums first means a run that finds nothing new
-- reads no geometry; a full rebuild would rewrite every polygon of every
-- vintage on every glossary run. Called by the `glossary_reconciliation` DAG
-- after `refresh_dim_geo_latest`.
DROP PROCEDURE IF EXISTS gold_glossary.refresh_tile_boundaries();
CREATE OR REPLACE PROCEDURE gold_glossary.refresh_tile_boundaries()
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('gold_glossary.refresh_tile_boundaries'));

    DROP TABLE IF EXISTS pg_temp.tile_boundary_choice;
    -- One geometry per geography and vintage (valid, the 500k cartographic
    -- resolution where the reference holds several, the newest load) and the
    -- attributes it is served with. No geometry is read here.
    CREATE TEMP TABLE tile_boundary_choice ON COMMIT DROP AS
    WITH boundary_choice AS (
        SELECT DISTINCT ON (boundary.geo_sk, boundary.boundary_vintage)
               boundary.geo_geometry_sk,
               boundary.geo_sk,
               boundary.boundary_vintage,
               boundary.geometry_checksum,
               entity.geo_id,
               entity.state_fips,
               entity.place_fips,
               entity.area_code,
               gold_glossary.geo_grain(entity.geo_type) AS geo_level
        FROM silver_ref.dim_geo_geometry_version AS boundary
        JOIN silver_ref.dim_geo_entity AS entity
             ON entity.geo_sk = boundary.geo_sk
            AND entity.geo_type IN ('place', 'metro')
        WHERE boundary.is_valid
        ORDER BY boundary.geo_sk, boundary.boundary_vintage,
                 (boundary.resolution = '500k') DESC,
                 boundary.ingested_at DESC, boundary.geo_geometry_sk DESC
    )
    SELECT choice.*,
           -- The name the reference gave it at that vintage: the newest
           -- attribute version not later than the boundary, else the
           -- nearest one after it.
           gold_glossary.geo_name(attribute.name, NULL, state.name, choice.geo_id)
               AS geo_name,
           state.name AS state_name,
           attribute.lsad,
           attribute.latitude,
           attribute.longitude
    FROM boundary_choice AS choice
    LEFT JOIN LATERAL (
        SELECT version.name, version.lsad, version.latitude, version.longitude
        FROM silver_ref.dim_geo_entity_version AS version
        WHERE version.geo_sk = choice.geo_sk
        ORDER BY (version.geography_vintage <= choice.boundary_vintage) DESC,
                 ABS(version.geography_vintage - choice.boundary_vintage),
                 version.ingested_at DESC, version.geo_version_sk DESC
        LIMIT 1
    ) AS attribute ON TRUE
    LEFT JOIN silver_ref.dim_geo_current AS state
         ON state.geo_type = 'state'
        AND state.state_fips = choice.state_fips;

    DELETE FROM gold_glossary.tile_boundary_version AS served
    WHERE NOT EXISTS (
        SELECT 1 FROM pg_temp.tile_boundary_choice AS choice
        WHERE choice.geo_id = served.geo_id
          AND choice.boundary_vintage = served.boundary_vintage
    );

    -- Only what changed reads its geometry.
    INSERT INTO gold_glossary.tile_boundary_version (
        geo_id, geo_level, boundary_vintage, geo_name, state_fips, state_name,
        place_fips, area_code, lsad, latitude, longitude, geometry_checksum,
        tile_geom, refreshed_at
    )
    SELECT choice.geo_id, choice.geo_level, choice.boundary_vintage,
           choice.geo_name, choice.state_fips, choice.state_name,
           choice.place_fips, choice.area_code, choice.lsad,
           choice.latitude, choice.longitude, choice.geometry_checksum,
           ST_Multi(ST_CollectionExtract(ST_Transform(boundary.geom, 3857), 3)),
           NOW()
    FROM pg_temp.tile_boundary_choice AS choice
    JOIN silver_ref.dim_geo_geometry_version AS boundary
         ON boundary.geo_geometry_sk = choice.geo_geometry_sk
    WHERE NOT EXISTS (
        SELECT 1 FROM gold_glossary.tile_boundary_version AS served
        WHERE served.geo_id = choice.geo_id
          AND served.boundary_vintage = choice.boundary_vintage
          AND (served.geo_level, served.geo_name, served.state_fips,
               served.state_name, served.place_fips, served.area_code,
               served.lsad, served.latitude, served.longitude,
               served.geometry_checksum)
              IS NOT DISTINCT FROM
              (choice.geo_level, choice.geo_name, choice.state_fips,
               choice.state_name, choice.place_fips, choice.area_code,
               choice.lsad, choice.latitude, choice.longitude,
               choice.geometry_checksum)
    )
    ON CONFLICT (geo_id, boundary_vintage) DO UPDATE
    SET geo_level = EXCLUDED.geo_level,
        geo_name = EXCLUDED.geo_name,
        state_fips = EXCLUDED.state_fips,
        state_name = EXCLUDED.state_name,
        place_fips = EXCLUDED.place_fips,
        area_code = EXCLUDED.area_code,
        lsad = EXCLUDED.lsad,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        geometry_checksum = EXCLUDED.geometry_checksum,
        tile_geom = EXCLUDED.tile_geom,
        refreshed_at = NOW();
END;
$$;
