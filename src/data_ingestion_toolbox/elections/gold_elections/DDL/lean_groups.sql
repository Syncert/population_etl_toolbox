-- Lean groups (elections-and-economic-performance, E-3): how counties that
-- leaned each way at a window's start fared over it, one measure at a time,
-- net of the nation. Applied by the bootstrap manifest in the `gold` phase
-- and re-applied by the DAGs' `ensure_elections_schema` task; this file is
-- the only definition of these relations.
--
-- ADR-0014, rule by rule:
--   1. Every change is the county's change minus the nation's over the same
--      window. Levels (income, output, jobs) change in percent growth, rates
--      (unemployment, poverty) in points. Income is real, deflated with the
--      national PCE price index.
--   2. One row per measure; nothing here adds measures together, scores or
--      ranks them.
--   3. The lean is the presidential vote cast in the window's first year (an
--      election year), never a later one.
--   4. Peer groups are named rows: the same state, rural-urban type,
--      starting level or ERS industry dependence.
--   5. A group is its middle county and the middle half of its counties,
--      with the count; no population weighting.
--
-- The rebuild is a function rather than views so this file does not depend
-- on the order the other sources' gold DDL is applied in: PL/pgSQL resolves
-- `gold_bea`, `gold_bls`, `gold_bls_qcew`, `gold_census_sae`, `gold_fred` and
-- `gold_usda_ers` when it runs, not when it is created.
--
-- The annual PCE price level is internal to this rebuild (the mean of the
-- twelve monthly PCEPI values of a complete year). It deflates, and is never
-- published: the site does not roll a monthly series to a year for serving.

CREATE SCHEMA IF NOT EXISTS gold_elections;

-- The measures a lean group compares. `change_kind` decides the arithmetic:
-- `growth` is percent growth of a level, `points` the difference of a rate.
CREATE OR REPLACE VIEW gold_elections.lean_measure_definition AS
SELECT measure, measure_label, change_kind, change_unit, value_unit, source_label,
       observation_basis, display_order
FROM (VALUES
    ('real_income_per_person', 'Income per person, after inflation', 'growth', 'percent growth',
     'dollars of the window''s last year', 'BEA CAINC1 line 3, deflated with the national PCE price index (FRED PCEPI)',
     'Per capita personal income as BEA publishes it, converted to the prices of the window''s last '
     || 'year with the national PCE price index (the mean of its twelve months).', 1),
    ('real_gdp', 'Economic output (real GDP)', 'growth', 'percent growth',
     'thousands of chained 2017 dollars', 'BEA CAGDP1 line 1',
     'Real gross domestic product as BEA publishes it, in chained 2017 dollars. BEA publishes '
     || 'county GDP from 2001.', 2),
    ('jobs', 'Jobs (annual average)', 'growth', 'percent growth',
     'jobs', 'BLS QCEW, total covered employment, all industries',
     'Annual average employment covered by unemployment insurance, all industries and owners, '
     || 'counted where people work.', 3),
    ('unemployment_rate', 'Unemployment rate', 'points', 'percentage points',
     'percent of the labor force', 'BLS LAUS annual averages',
     'BLS''s annual average unemployment rate for the county. The nation''s rate is the 50 states '
     || 'and DC''s annual average unemployed divided by their annual average labor force.', 4),
    ('poverty_rate', 'People in poverty', 'points', 'percentage points',
     'percent of people', 'Census SAIPE',
     'Census Small Area Income and Poverty Estimates: the share of people of all ages below the '
     || 'poverty line.', 5)
) AS definition(measure, measure_label, change_kind, change_unit, value_unit, source_label,
                observation_basis, display_order);

-- The four lean bands, from the two-party margin (Republican share minus
-- Democratic share, in points) of the presidential vote in the window's
-- first year. An exact tie leans neither way and is in no band.
CREATE OR REPLACE VIEW gold_elections.lean_band_definition AS
SELECT lean_band, lean_label, lower_margin, upper_margin, display_order
FROM (VALUES
    ('R20_plus', 'Republican by 20 points or more', 20::NUMERIC, NULL::NUMERIC, 1),
    ('R0_20', 'Republican by less than 20 points', 0::NUMERIC, 20::NUMERIC, 2),
    ('D0_20', 'Democratic by less than 20 points', -20::NUMERIC, 0::NUMERIC, 3),
    ('D20_plus', 'Democratic by 20 points or more', NULL::NUMERIC, -20::NUMERIC, 4)
) AS band(lean_band, lean_label, lower_margin, upper_margin, display_order);

-- The peer groups a lean comparison can be narrowed to (ADR-0014 rule 4).
CREATE OR REPLACE VIEW gold_elections.lean_peer_definition AS
SELECT peer_type, peer_label, peer_basis, display_order
FROM (VALUES
    ('all', 'All counties', 'Every county with a vote and both ends of the window.', 1),
    ('rural_urban', 'Same rural-urban type',
     'USDA ERS Rural-Urban Continuum Codes, 2023 edition (the only edition loaded), grouped as '
     || 'large metro (1), other metro (2-3), nonmetro next to a metro (4, 6, 8) and remote '
     || 'nonmetro (5, 7, 9). Earlier windows use the 2023 codes too.', 2),
    ('state', 'Same state', 'Counties in the same state.', 3),
    ('starting_level', 'Similar starting level',
     'Quarter of all counties by the measure''s value in the window''s first year.', 4),
    ('industry', 'Same industry dependence',
     'USDA ERS County Typology Codes, 2025 edition: the industry a county depends on, or none.', 5)
) AS peer(peer_type, peer_label, peer_basis, display_order);

-- One row per completed or skipped rebuild; the fingerprint is what the
-- inputs looked like, so an unchanged warehouse is not rebuilt.
CREATE TABLE IF NOT EXISTS gold_elections.lean_refresh (
    refresh_id BIGSERIAL PRIMARY KEY,
    input_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('rebuilt', 'unchanged')),
    place_rows INTEGER,
    group_rows INTEGER,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL DEFAULT CLOCK_TIMESTAMP()
);

-- Each county's change over each window, against the nation's.
CREATE TABLE IF NOT EXISTS gold_elections.lean_place_change (
    window_start_year SMALLINT NOT NULL,
    window_end_year SMALLINT NOT NULL,
    measure TEXT NOT NULL,
    geo_id TEXT NOT NULL,
    state_fips TEXT NOT NULL,
    lean_margin NUMERIC NOT NULL,
    lean_band TEXT NOT NULL,
    start_value NUMERIC NOT NULL,
    end_value NUMERIC NOT NULL,
    place_change NUMERIC NOT NULL,
    nation_change NUMERIC NOT NULL,
    net_change NUMERIC NOT NULL,
    rural_urban_group TEXT,
    starting_level_group TEXT NOT NULL,
    industry_group TEXT,
    industry_label TEXT,
    refresh_id BIGINT NOT NULL,
    PRIMARY KEY (window_start_year, window_end_year, measure, geo_id),
    CHECK (window_end_year > window_start_year),
    CHECK (lean_band IN ('R20_plus', 'R0_20', 'D0_20', 'D20_plus'))
);

CREATE INDEX IF NOT EXISTS lean_place_change_geo_idx
    ON gold_elections.lean_place_change (geo_id, measure);

-- Each lean band within each peer group: count, middle and middle half of
-- the counties' changes net of the nation.
CREATE TABLE IF NOT EXISTS gold_elections.lean_group_change (
    window_start_year SMALLINT NOT NULL,
    window_end_year SMALLINT NOT NULL,
    measure TEXT NOT NULL,
    peer_type TEXT NOT NULL,
    peer_group TEXT NOT NULL,
    peer_group_label TEXT NOT NULL,
    lean_band TEXT NOT NULL,
    place_count INTEGER NOT NULL CHECK (place_count > 0),
    net_change_p25 NUMERIC NOT NULL,
    net_change_median NUMERIC NOT NULL,
    net_change_p75 NUMERIC NOT NULL,
    nation_change NUMERIC NOT NULL,
    refresh_id BIGINT NOT NULL,
    PRIMARY KEY (window_start_year, window_end_year, measure, peer_type, peer_group, lean_band),
    CHECK (net_change_p25 <= net_change_median AND net_change_median <= net_change_p75)
);

-- Read the inputs into temporary tables dropped at commit: `lean_input`
-- (measure, geo_id, year, value, deflate), `lean_pce` (year, price level),
-- `lean_vote` (county, election year, margin), `lean_peer` (county's
-- rural-urban type and industry dependence) and `lean_state_name`.
-- Counties and the nation only; valid values only.
CREATE OR REPLACE FUNCTION gold_elections.load_lean_inputs()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    _rows INTEGER;
BEGIN
    DROP TABLE IF EXISTS pg_temp.lean_input;
    DROP TABLE IF EXISTS pg_temp.lean_pce;
    DROP TABLE IF EXISTS pg_temp.lean_vote;
    DROP TABLE IF EXISTS pg_temp.lean_peer;
    DROP TABLE IF EXISTS pg_temp.lean_state_name;

    -- The lean: each county's presidential two-party margin, 2000 onward.
    CREATE TEMP TABLE lean_vote ON COMMIT DROP AS
    SELECT latest.geo_id, latest.year, latest.value AS margin
    FROM gold_elections.observation_latest AS latest
    WHERE latest.metric_key = 'president_two_party_margin'
      AND latest.geo_type = 'county'
      AND latest.year >= 2000
      AND latest.value IS NOT NULL;

    -- Peer classifications, the newest edition of each.
    CREATE TEMP TABLE lean_peer ON COMMIT DROP AS
    WITH rucc AS (
        SELECT DISTINCT ON (latest.geo_id) latest.geo_id,
               CASE WHEN latest.value = 1 THEN 'large_metro'
                    WHEN latest.value IN (2, 3) THEN 'other_metro'
                    WHEN latest.value IN (4, 6, 8) THEN 'nonmetro_adjacent'
                    WHEN latest.value IN (5, 7, 9) THEN 'nonmetro_remote'
               END AS rural_urban_group
        FROM gold_usda_ers.observation_latest AS latest
        WHERE latest.metric_key = 'rural_urban_continuum_code'
          AND latest.value_status = 'valid'
        ORDER BY latest.geo_id, latest.year DESC
    ),
    industry AS (
        SELECT DISTINCT ON (latest.geo_id) latest.geo_id,
               latest.value::INTEGER::TEXT AS industry_group,
               CASE WHEN latest.value = 0 THEN 'Not dependent on one industry'
                    ELSE COALESCE(NULLIF(latest.code_label, ''), 'Industry code ' || latest.value::INTEGER)
               END AS industry_label
        FROM gold_usda_ers.observation_latest AS latest
        WHERE latest.metric_key = 'industry_dependence'
          AND latest.value_status = 'valid'
        ORDER BY latest.geo_id, latest.year DESC
    )
    SELECT COALESCE(rucc.geo_id, industry.geo_id) AS geo_id,
           rucc.rural_urban_group, industry.industry_group, industry.industry_label
    FROM rucc
    FULL JOIN industry ON industry.geo_id = rucc.geo_id;

    CREATE TEMP TABLE lean_state_name ON COMMIT DROP AS
    SELECT geo.state_fips, MIN(geo.state_name) AS state_name
    FROM gold_glossary.dim_geo_latest AS geo
    WHERE geo.geo_id = 'state:' || geo.state_fips
    GROUP BY geo.state_fips;

    -- The year's price level: the mean of its twelve months, only for a year
    -- with all twelve.
    CREATE TEMP TABLE lean_pce ON COMMIT DROP AS
    SELECT EXTRACT(YEAR FROM observation.observation_date)::INTEGER AS year,
           AVG(observation.value) AS price_level
    FROM gold_fred.fact_fred_observation AS observation
    JOIN gold_fred.dim_fred_series AS series ON series.fred_series_sk = observation.fred_series_sk
    WHERE series.series_id = 'PCEPI'
      AND observation.value IS NOT NULL
    GROUP BY 1
    HAVING COUNT(DISTINCT EXTRACT(MONTH FROM observation.observation_date)) = 12;

    CREATE TEMP TABLE lean_input (
        measure TEXT NOT NULL,
        geo_id TEXT NOT NULL,
        year INTEGER NOT NULL,
        value NUMERIC NOT NULL,
        deflate BOOLEAN NOT NULL,
        PRIMARY KEY (measure, geo_id, year)
    ) ON COMMIT DROP;

    INSERT INTO lean_input
    SELECT CASE latest.metric_key WHEN 'CAINC1:3' THEN 'real_income_per_person' ELSE 'real_gdp' END,
           latest.geo_id, latest.year, latest.value, latest.metric_key = 'CAINC1:3'
    FROM gold_bea.observation_latest AS latest
    WHERE latest.metric_key IN ('CAINC1:3', 'CAGDP1:1')
      AND latest.geo_type IN ('county', 'nation')
      AND latest.value_status = 'valid'
      AND latest.value IS NOT NULL;

    INSERT INTO lean_input
    SELECT 'jobs', latest.geo_id, latest.year, latest.value, FALSE
    FROM gold_bls_qcew.observation_latest AS latest
    WHERE latest.metric_key = 'annual_avg_employment:10:0'
      AND latest.period = 'a'
      AND latest.geo_type IN ('county', 'nation')
      AND latest.value_status = 'valid'
      AND latest.value IS NOT NULL;

    INSERT INTO lean_input
    SELECT 'poverty_rate', latest.geo_id, latest.estimate_year, latest.value, FALSE
    FROM gold_census_sae.estimate_latest AS latest
    WHERE latest.metric_key = 'saipe:SAEPOVRTALL'
      AND latest.geo_type IN ('county', 'nation')
      AND latest.value_status = 'valid'
      AND latest.value IS NOT NULL;

    -- LAUS annual averages (`M13`, provider facts): the county rate (measure
    -- 03) as published; the nation from the states' unemployed (04) over
    -- their labor force (06), only for a year all 51 report both.
    INSERT INTO lean_input
    SELECT 'unemployment_rate', annual.geo_id, annual.year, annual.value, FALSE
    FROM gold_bls.provider_annual_average AS annual
    WHERE annual.series_id LIKE 'LAUCN%'
      AND RIGHT(annual.series_id, 2) = '03'
      AND annual.geo_id LIKE 'state:%|county:%'
      AND annual.value_status = 'valid'
      AND annual.value IS NOT NULL;

    INSERT INTO lean_input
    SELECT 'unemployment_rate', 'us:1', state_total.year,
           ROUND(100.0 * state_total.unemployed / state_total.labor_force, 1), FALSE
    FROM (
        SELECT annual.year,
               SUM(annual.value) FILTER (WHERE RIGHT(annual.series_id, 2) = '04') AS unemployed,
               SUM(annual.value) FILTER (WHERE RIGHT(annual.series_id, 2) = '06') AS labor_force,
               COUNT(DISTINCT annual.geo_id) FILTER (WHERE RIGHT(annual.series_id, 2) = '04') AS unemployed_states,
               COUNT(DISTINCT annual.geo_id) FILTER (WHERE RIGHT(annual.series_id, 2) = '06') AS labor_force_states
        FROM gold_bls.provider_annual_average AS annual
        WHERE annual.series_id LIKE 'LAUST%'
          AND RIGHT(annual.series_id, 2) IN ('04', '06')
          AND annual.geo_id ~ '^state:[0-9]{2}$'
          AND annual.geo_id <> 'state:72'
          AND annual.value_status = 'valid'
          AND annual.value IS NOT NULL
        GROUP BY annual.year
    ) AS state_total
    WHERE state_total.unemployed_states = 51
      AND state_total.labor_force_states = 51
      AND state_total.labor_force > 0;

    SELECT COUNT(*) INTO _rows FROM lean_input;
    RETURN _rows;
END;
$$;

-- Rebuild both tables from the temporary tables `load_lean_inputs` fills
-- (a test fills them by hand). Returns the number of county rows written.
CREATE OR REPLACE FUNCTION gold_elections.build_lean_groups(p_refresh_id BIGINT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    _rows INTEGER;
BEGIN
    DELETE FROM gold_elections.lean_group_change;
    DELETE FROM gold_elections.lean_place_change;

    INSERT INTO gold_elections.lean_place_change
    WITH election_year AS (
        SELECT DISTINCT year FROM pg_temp.lean_vote
    ),
    window_span AS (
        SELECT first_year.year AS start_year, last_year.year AS end_year
        FROM election_year AS first_year
        JOIN election_year AS last_year ON last_year.year > first_year.year
    ),
    -- Each input in the prices of the window's last year, when it deflates.
    valued AS (
        SELECT span.start_year, span.end_year, start_value.measure, start_value.geo_id,
               CASE WHEN start_value.deflate
                    THEN start_value.value * end_price.price_level / start_price.price_level
                    ELSE start_value.value END AS start_value,
               end_value.value AS end_value,
               definition.change_kind
        FROM window_span AS span
        JOIN pg_temp.lean_input AS start_value ON start_value.year = span.start_year
        JOIN pg_temp.lean_input AS end_value
          ON end_value.measure = start_value.measure
         AND end_value.geo_id = start_value.geo_id
         AND end_value.year = span.end_year
        JOIN gold_elections.lean_measure_definition AS definition
          ON definition.measure = start_value.measure
        LEFT JOIN pg_temp.lean_pce AS start_price ON start_price.year = span.start_year
        LEFT JOIN pg_temp.lean_pce AS end_price ON end_price.year = span.end_year
        WHERE NOT start_value.deflate
           OR (start_price.price_level IS NOT NULL AND end_price.price_level IS NOT NULL)
    ),
    changed AS (
        SELECT valued.*,
               CASE valued.change_kind
                   WHEN 'growth' THEN 100.0 * (valued.end_value / valued.start_value - 1)
                   ELSE valued.end_value - valued.start_value
               END AS place_change
        FROM valued
        WHERE valued.change_kind = 'points' OR valued.start_value > 0
    )
    SELECT changed.start_year, changed.end_year, changed.measure, changed.geo_id,
           SUBSTRING(changed.geo_id FROM '^state:([0-9]{2})\|') AS state_fips,
           lean.margin,
           CASE WHEN lean.margin >= 20 THEN 'R20_plus'
                WHEN lean.margin > 0 THEN 'R0_20'
                WHEN lean.margin > -20 THEN 'D0_20'
                ELSE 'D20_plus' END,
           ROUND(changed.start_value, 4),
           ROUND(changed.end_value, 4),
           ROUND(changed.place_change, 4),
           ROUND(nation.place_change, 4),
           ROUND(changed.place_change - nation.place_change, 4),
           peer.rural_urban_group,
           NTILE(4) OVER (PARTITION BY changed.start_year, changed.end_year, changed.measure
                          ORDER BY changed.start_value, changed.geo_id)::TEXT,
           peer.industry_group,
           peer.industry_label,
           p_refresh_id
    FROM changed
    JOIN changed AS nation
      ON nation.start_year = changed.start_year
     AND nation.end_year = changed.end_year
     AND nation.measure = changed.measure
     AND nation.geo_id = 'us:1'
    JOIN pg_temp.lean_vote AS lean ON lean.geo_id = changed.geo_id AND lean.year = changed.start_year
    LEFT JOIN pg_temp.lean_peer AS peer ON peer.geo_id = changed.geo_id
    WHERE changed.geo_id LIKE 'state:%|county:%'
      AND lean.margin <> 0;

    GET DIAGNOSTICS _rows = ROW_COUNT;

    INSERT INTO gold_elections.lean_group_change
    WITH membership AS (
        SELECT place.*, 'all'::TEXT AS peer_type, 'all'::TEXT AS peer_group,
               'All counties'::TEXT AS peer_group_label
        FROM gold_elections.lean_place_change AS place
        UNION ALL
        SELECT place.*, 'state', place.state_fips,
               COALESCE(state_name.state_name, 'State ' || place.state_fips)
        FROM gold_elections.lean_place_change AS place
        LEFT JOIN pg_temp.lean_state_name AS state_name ON state_name.state_fips = place.state_fips
        UNION ALL
        SELECT place.*, 'rural_urban', place.rural_urban_group,
               CASE place.rural_urban_group
                   WHEN 'large_metro' THEN 'Large metro'
                   WHEN 'other_metro' THEN 'Other metro'
                   WHEN 'nonmetro_adjacent' THEN 'Nonmetro next to a metro'
                   ELSE 'Remote nonmetro' END
        FROM gold_elections.lean_place_change AS place
        WHERE place.rural_urban_group IS NOT NULL
        UNION ALL
        SELECT place.*, 'starting_level', place.starting_level_group,
               CASE place.starting_level_group
                   WHEN '1' THEN 'Lowest quarter at the start'
                   WHEN '2' THEN 'Second quarter at the start'
                   WHEN '3' THEN 'Third quarter at the start'
                   ELSE 'Highest quarter at the start' END
        FROM gold_elections.lean_place_change AS place
        UNION ALL
        SELECT place.*, 'industry', place.industry_group, place.industry_label
        FROM gold_elections.lean_place_change AS place
        WHERE place.industry_group IS NOT NULL
    )
    SELECT membership.window_start_year, membership.window_end_year, membership.measure,
           membership.peer_type, membership.peer_group, MIN(membership.peer_group_label),
           membership.lean_band,
           COUNT(*)::INTEGER,
           ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY membership.net_change)::NUMERIC, 4),
           ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY membership.net_change)::NUMERIC, 4),
           ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY membership.net_change)::NUMERIC, 4),
           MIN(membership.nation_change),
           p_refresh_id
    FROM membership
    GROUP BY membership.window_start_year, membership.window_end_year, membership.measure,
             membership.peer_type, membership.peer_group, membership.lean_band;

    RETURN _rows;
END;
$$;

-- What the inputs look like now, cheaply: the newest publication and the
-- published count of each source's control table, and the newest silver
-- write of the two sources without one.
CREATE OR REPLACE FUNCTION gold_elections.lean_input_fingerprint()
RETURNS TEXT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    _parts TEXT;
BEGIN
    SELECT CONCAT_WS('|',
        (SELECT COUNT(*) || '@' || COALESCE(MAX(published_at)::TEXT, '') FROM control.elections_run WHERE status = 'published'),
        (SELECT COUNT(*) || '@' || COALESCE(MAX(published_at)::TEXT, '') FROM control.bea_table_capture WHERE status = 'published'),
        (SELECT COUNT(*) || '@' || COALESCE(MAX(published_at)::TEXT, '') FROM control.bls_qcew_slice WHERE status = 'published'),
        (SELECT COUNT(*) || '@' || COALESCE(MAX(published_at)::TEXT, '') FROM control.census_sae_slice WHERE status = 'published'),
        (SELECT COUNT(*) || '@' || COALESCE(MAX(published_at)::TEXT, '') FROM control.usda_ers_file WHERE status = 'published'),
        (SELECT COALESCE(MAX(ingested_at)::TEXT, '') FROM silver_bls.fact_labor_statistics),
        (SELECT COALESCE(MAX(ingested_at)::TEXT, '') FROM silver_fred.fact_economic_indicators WHERE series_id = 'PCEPI'),
        -- The definitions are part of the input: a change to this file
        -- rebuilds even when no source published.
        (SELECT md5(STRING_AGG(definition::TEXT, ',' ORDER BY definition.measure))
           FROM gold_elections.lean_measure_definition AS definition)
    ) INTO _parts;
    RETURN md5(_parts);
END;
$$;

-- Rebuild the lean groups when any input changed since the last rebuild,
-- or always when `p_force`. Returns the refresh row's status.
CREATE OR REPLACE FUNCTION gold_elections.refresh_lean_groups(p_force BOOLEAN DEFAULT FALSE)
RETURNS TABLE (refresh_id BIGINT, status TEXT, place_rows INTEGER, group_rows INTEGER)
LANGUAGE plpgsql
AS $$
DECLARE
    _started TIMESTAMPTZ := CLOCK_TIMESTAMP();
    _fingerprint TEXT;
    _refresh_id BIGINT;
    _place_rows INTEGER;
    _group_rows INTEGER;
BEGIN
    -- One rebuild at a time; a second caller waits rather than interleaves.
    PERFORM pg_advisory_xact_lock(hashtext('gold_elections.refresh_lean_groups'));
    _fingerprint := gold_elections.lean_input_fingerprint();

    IF NOT p_force AND EXISTS (
        SELECT 1 FROM (
            SELECT refresh.input_fingerprint
            FROM gold_elections.lean_refresh AS refresh
            WHERE refresh.status = 'rebuilt'
            ORDER BY refresh.refresh_id DESC
            LIMIT 1
        ) AS last_rebuild
        WHERE last_rebuild.input_fingerprint = _fingerprint
    ) THEN
        INSERT INTO gold_elections.lean_refresh (input_fingerprint, status, started_at)
        VALUES (_fingerprint, 'unchanged', _started)
        RETURNING lean_refresh.refresh_id INTO _refresh_id;
        RETURN QUERY SELECT _refresh_id, 'unchanged'::TEXT, NULL::INTEGER, NULL::INTEGER;
        RETURN;
    END IF;

    INSERT INTO gold_elections.lean_refresh (input_fingerprint, status, started_at)
    VALUES (_fingerprint, 'rebuilt', _started)
    RETURNING lean_refresh.refresh_id INTO _refresh_id;

    PERFORM gold_elections.load_lean_inputs();
    _place_rows := gold_elections.build_lean_groups(_refresh_id);
    SELECT COUNT(*) INTO _group_rows FROM gold_elections.lean_group_change;

    UPDATE gold_elections.lean_refresh AS refresh
    SET place_rows = _place_rows, group_rows = _group_rows, finished_at = CLOCK_TIMESTAMP()
    WHERE refresh.refresh_id = _refresh_id;

    DROP TABLE IF EXISTS pg_temp.lean_input;
    DROP TABLE IF EXISTS pg_temp.lean_pce;
    DROP TABLE IF EXISTS pg_temp.lean_vote;
    DROP TABLE IF EXISTS pg_temp.lean_peer;
    DROP TABLE IF EXISTS pg_temp.lean_state_name;

    -- The lean: each county's presidential two-party margin, 2000 onward.
    CREATE TEMP TABLE lean_vote ON COMMIT DROP AS
    SELECT latest.geo_id, latest.year, latest.value AS margin
    FROM gold_elections.observation_latest AS latest
    WHERE latest.metric_key = 'president_two_party_margin'
      AND latest.geo_type = 'county'
      AND latest.year >= 2000
      AND latest.value IS NOT NULL;

    -- Peer classifications, the newest edition of each.
    CREATE TEMP TABLE lean_peer ON COMMIT DROP AS
    WITH rucc AS (
        SELECT DISTINCT ON (latest.geo_id) latest.geo_id,
               CASE WHEN latest.value = 1 THEN 'large_metro'
                    WHEN latest.value IN (2, 3) THEN 'other_metro'
                    WHEN latest.value IN (4, 6, 8) THEN 'nonmetro_adjacent'
                    WHEN latest.value IN (5, 7, 9) THEN 'nonmetro_remote'
               END AS rural_urban_group
        FROM gold_usda_ers.observation_latest AS latest
        WHERE latest.metric_key = 'rural_urban_continuum_code'
          AND latest.value_status = 'valid'
        ORDER BY latest.geo_id, latest.year DESC
    ),
    industry AS (
        SELECT DISTINCT ON (latest.geo_id) latest.geo_id,
               latest.value::INTEGER::TEXT AS industry_group,
               CASE WHEN latest.value = 0 THEN 'Not dependent on one industry'
                    ELSE COALESCE(NULLIF(latest.code_label, ''), 'Industry code ' || latest.value::INTEGER)
               END AS industry_label
        FROM gold_usda_ers.observation_latest AS latest
        WHERE latest.metric_key = 'industry_dependence'
          AND latest.value_status = 'valid'
        ORDER BY latest.geo_id, latest.year DESC
    )
    SELECT COALESCE(rucc.geo_id, industry.geo_id) AS geo_id,
           rucc.rural_urban_group, industry.industry_group, industry.industry_label
    FROM rucc
    FULL JOIN industry ON industry.geo_id = rucc.geo_id;

    CREATE TEMP TABLE lean_state_name ON COMMIT DROP AS
    SELECT geo.state_fips, MIN(geo.state_name) AS state_name
    FROM gold_glossary.dim_geo_latest AS geo
    WHERE geo.geo_id = 'state:' || geo.state_fips
    GROUP BY geo.state_fips;

    RETURN QUERY SELECT _refresh_id, 'rebuilt'::TEXT, _place_rows, _group_rows;
END;
$$;

-- The serving role reads the tables; only the warehouse owner rebuilds them.
REVOKE ALL ON FUNCTION gold_elections.load_lean_inputs() FROM PUBLIC;
REVOKE ALL ON FUNCTION gold_elections.build_lean_groups(BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION gold_elections.refresh_lean_groups(BOOLEAN) FROM PUBLIC;

COMMENT ON TABLE gold_elections.lean_place_change IS
    'Derived (ADR-0014): each county''s change over a window between presidential elections, '
    'net of the nation, with its lean at the window''s start. A comparison, not a cause.';
COMMENT ON TABLE gold_elections.lean_group_change IS
    'Derived (ADR-0014): counties grouped by lean at the window''s start and by peer group; '
    'count, middle and middle half of their change net of the nation. No combined score.';
