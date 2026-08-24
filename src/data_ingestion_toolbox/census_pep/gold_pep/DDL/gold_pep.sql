-- census_pep/gold_pep/DDL/gold_pep.sql
-- Gold analytics layer for Census PEP (Population Estimates) objects.
-- Per-source serving table with PEP-specific columns only.

CREATE SCHEMA IF NOT EXISTS gold_pep;
CREATE EXTENSION IF NOT EXISTS postgis;

-- ============================================================
-- PEP METADATA DIMENSIONS
-- ============================================================

CREATE TABLE IF NOT EXISTS gold_pep.dim_pep_table (
    pep_table_sk        BIGSERIAL PRIMARY KEY,
    dataset_code        TEXT NOT NULL DEFAULT 'pep',
    vintage_year        INTEGER NOT NULL,
    table_id            TEXT NOT NULL,
    table_title         TEXT,
    concept             TEXT,
    universe            TEXT,
    reference_url       TEXT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dataset_code, vintage_year, table_id)
);

CREATE TABLE IF NOT EXISTS gold_pep.dim_pep_variable (
    pep_variable_sk        BIGSERIAL PRIMARY KEY,
    pep_table_sk           BIGINT NOT NULL REFERENCES gold_pep.dim_pep_table(pep_table_sk),
    dataset_code           TEXT NOT NULL DEFAULT 'pep',
    vintage_year           INTEGER NOT NULL,
    variable_code          TEXT NOT NULL,
    variable_label         TEXT,
    concept                TEXT,
    universe               TEXT,
    value_role             TEXT NOT NULL CHECK (value_role IN ('ESTIMATE', 'MOE', 'ANNOTATION')),
    is_publishable_default BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dataset_code, vintage_year, variable_code)
);

-- ============================================================
-- PEP FACT VIEW (source of truth)
-- ============================================================

CREATE OR REPLACE VIEW gold_pep.fact_pep_observation AS
SELECT
    s.geo_id,
    CASE
        WHEN s.geo_id LIKE 'us:%'     THEN 'NATIONAL'
        WHEN s.geo_id LIKE 'state:%'  THEN 'STATE'
        WHEN s.geo_id LIKE 'county:%' THEN 'COUNTY'
        WHEN s.geo_id LIKE 'place:%'  THEN 'PLACE'
        ELSE 'NATIONAL'
    END AS geo_level,
    s.time_sk,
    MAKE_DATE(s.estimate_year, 1, 1) AS observation_date,
    s.duration_start,
    s.duration_end,
    NULL::BIGINT AS pep_table_sk,
    NULL::BIGINT AS pep_variable_sk,
    s.dataset     AS dataset_code,
    s.estimate_year AS vintage_year,
    s.estimate_value,
    s.margin_of_error,
    s.margin_of_error_pct,
    NULL::TEXT AS estimate_annotation,
    NULL::TEXT AS moe_annotation,
    CURRENT_DATE AS as_of_date,
    s.ingested_at AS updated_at
FROM silver_pep.fact_population s
WHERE s.estimate_value IS NOT NULL
  AND s.variable_code IS NOT NULL
  AND s.variable_code <> '';

-- ============================================================
-- PEP SERVING TABLE (Source-First: PEP-specific columns only)
-- ============================================================

CREATE TABLE IF NOT EXISTS gold_pep.rpt_pep_observations (
    source_code                TEXT NOT NULL DEFAULT 'CENSUS_PEP',
    observation_date           DATE NOT NULL,
    duration_start             DATE,
    duration_end               DATE,
    time_sk                    INTEGER,
    as_of_date                 DATE NOT NULL,
    updated_at                 TIMESTAMPTZ NOT NULL,
    geo_id                     TEXT NOT NULL,
    geo_level                  TEXT NOT NULL,
    state_fips                 TEXT,
    county_fips                TEXT,
    place_fips                 TEXT,
    state_name                 TEXT,
    county_name                TEXT,
    place_name                 TEXT,
    geo_latitude               DOUBLE PRECISION,
    geo_longitude              DOUBLE PRECISION,
    -- PEP values (all PEP estimates are point estimates)
    value                      NUMERIC NOT NULL,
    dataset_code               TEXT NOT NULL DEFAULT 'pep',
    vintage_year               INTEGER NOT NULL,
    table_id                   TEXT NOT NULL,
    table_title                TEXT,
    variable_code              TEXT NOT NULL,
    variable_label             TEXT,
    concept                    TEXT,
    universe                   TEXT,
    is_publishable_default     BOOLEAN,
    estimate_value             NUMERIC NOT NULL,
    margin_of_error            NUMERIC,
    margin_of_error_pct        NUMERIC,
    estimate_annotation        TEXT,
    moe_annotation             TEXT,
    value_type                 TEXT,
    units                      TEXT,
    -- Metric catalog association
    metric_code                TEXT,
    metric_display_name        TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_rpt_pep_observations_nk
    ON gold_pep.rpt_pep_observations (
        geo_id,
        observation_date,
        dataset_code,
        vintage_year,
        variable_code,
        COALESCE(metric_code, '')
    );

CREATE INDEX IF NOT EXISTS ix_rpt_pep_observations_source_geo_date
    ON gold_pep.rpt_pep_observations (source_code, geo_id, observation_date);

CREATE INDEX IF NOT EXISTS ix_rpt_pep_observations_metric_date
    ON gold_pep.rpt_pep_observations (metric_code, observation_date);

CREATE INDEX IF NOT EXISTS ix_rpt_pep_observations_dataset_vintage
    ON gold_pep.rpt_pep_observations (dataset_code, vintage_year);

CREATE INDEX IF NOT EXISTS ix_rpt_pep_observations_metric_geo_date
    ON gold_pep.rpt_pep_observations (metric_code, geo_id, observation_date);

CREATE INDEX IF NOT EXISTS ix_rpt_pep_observations_updated_at
    ON gold_pep.rpt_pep_observations (updated_at DESC);

CREATE INDEX IF NOT EXISTS ix_rpt_pep_latest_selection
    ON gold_pep.rpt_pep_observations (
        geo_id,
        variable_code,
        metric_code,
        observation_date DESC,
        updated_at DESC,
        vintage_year DESC
    );

-- ============================================================
-- PEP MATERIALIZED VIEW (latest per geo/variable)
-- ============================================================

CREATE TABLE IF NOT EXISTS gold_pep.mv_pep_latest
    (LIKE gold_pep.rpt_pep_observations INCLUDING DEFAULTS INCLUDING STORAGE INCLUDING COMMENTS);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_pep_latest
    ON gold_pep.mv_pep_latest (
        geo_id,
        dataset_code,
        vintage_year,
        variable_code,
        COALESCE(metric_code, '')
    );

CREATE INDEX IF NOT EXISTS ix_mv_pep_latest_source_metric
    ON gold_pep.mv_pep_latest (source_code, metric_code);

CREATE INDEX IF NOT EXISTS ix_mv_pep_latest_vintage
    ON gold_pep.mv_pep_latest (dataset_code, vintage_year);

CREATE INDEX IF NOT EXISTS ix_mv_pep_latest_metric_geo
    ON gold_pep.mv_pep_latest (metric_code, geo_id);

-- ============================================================
-- PEP REFRESH PROCEDURES
-- ============================================================

DROP PROCEDURE IF EXISTS gold_pep.refresh_rpt_pep_observations(DATE, DATE);
CREATE OR REPLACE PROCEDURE gold_pep.refresh_rpt_pep_observations(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_started_at TIMESTAMPTZ := clock_timestamp();
    v_deleted_rows BIGINT;
    v_inserted_rows BIGINT;
    v_affected_keys BIGINT;
BEGIN
    RAISE NOTICE '[PEP RPT CHUNK] status=STARTED start=% end=%', p_start_date, p_end_date;

    DROP TABLE IF EXISTS pg_temp.gold_pep_affected_keys;
    CREATE TEMP TABLE gold_pep_affected_keys (
        geo_id        TEXT NOT NULL,
        variable_code TEXT NOT NULL,
        metric_code   TEXT NOT NULL,
        PRIMARY KEY (geo_id, variable_code, metric_code)
    ) ON COMMIT DROP;

    -- Step 1: Truncate and repopulate affected keys
    DELETE FROM gold_pep_affected_keys;
    INSERT INTO gold_pep_affected_keys (geo_id, variable_code, metric_code)
    SELECT DISTINCT d.geo_id, d.variable_code, COALESCE(d.metric_code, '')
    FROM gold_pep.rpt_pep_observations d
    WHERE (p_start_date IS NULL OR d.observation_date >= p_start_date)
      AND (p_end_date IS NULL OR d.observation_date <= p_end_date)
    ON CONFLICT DO NOTHING;

    -- Step 2: Truncate and repopulate the reporting table for the window
    TRUNCATE gold_pep.rpt_pep_observations;

    INSERT INTO gold_pep.rpt_pep_observations (
        source_code,
        observation_date,
        duration_start,
        duration_end,
        time_sk,
        as_of_date,
        updated_at,
        geo_id,
        geo_level,
        state_fips,
        county_fips,
        place_fips,
        state_name,
        county_name,
        place_name,
        geo_latitude,
        geo_longitude,
        value,
        dataset_code,
        vintage_year,
        table_id,
        table_title,
        variable_code,
        variable_label,
        concept,
        universe,
        is_publishable_default,
        estimate_value,
        margin_of_error,
        margin_of_error_pct,
        estimate_annotation,
        moe_annotation,
        value_type,
        units,
        metric_code,
        metric_display_name
    )
    SELECT
        'CENSUS_PEP' AS source_code,
        MAKE_DATE(f.estimate_year, 1, 1) AS observation_date,
        f.duration_start,
        f.duration_end,
        f.time_sk,
        CURRENT_DATE AS as_of_date,
        f.ingested_at AS updated_at,
        f.geo_id,
        CASE
            WHEN f.geo_id LIKE 'us:%'     THEN 'NATIONAL'
            WHEN f.geo_id LIKE 'state:%'  THEN 'STATE'
            WHEN f.geo_id LIKE 'county:%' THEN 'COUNTY'
            WHEN f.geo_id LIKE 'place:%'  THEN 'PLACE'
            ELSE 'NATIONAL'
        END AS geo_level,
        NULLIF(SPLIT_PART(f.geo_id, ':', 2), '') AS state_fips,
        NULLIF(SPLIT_PART(f.geo_id, ':', 3), '') AS county_fips,
        NULLIF(SPLIT_PART(f.geo_id, ':', 4), '') AS place_fips,
        gl.state_name,
        gl.county_name,
        gl.place_name,
        gl.geography::DOUBLE PRECISION,
        gl.geography::DOUBLE PRECISION,
        f.estimate_value::NUMERIC AS value,
        f.dataset AS dataset_code,
        f.estimate_year AS vintage_year,
        f.table_id,
        COALESCE(dt.table_title, f.variable_label) AS table_title,
        f.variable_code,
        f.variable_label,
        f.variable_concept,
        f.universe,
        true AS is_publishable_default,
        f.estimate_value::NUMERIC,
        f.margin_of_error::NUMERIC,
        f.margin_of_error_pct,
        f.estimate_annotation,
        f.moe_annotation,
        'ESTIMATE' AS value_type,
        'persons' AS units,
        COALESCE(m.metric_code, '') AS metric_code,
        m.metric_display_name
    FROM silver_pep.fact_population f
    LEFT JOIN gold_pep.dim_pep_table dt
        ON dt.dataset_code = f.dataset
       AND dt.vintage_year = f.estimate_year
       AND dt.table_id = f.table_id
    LEFT JOIN gold_pep.dim_pep_variable dv
        ON dv.dataset_code = f.dataset
       AND dv.vintage_year = f.estimate_year
       AND dv.variable_code = f.variable_code
       AND dv.pep_table_sk = dt.pep_table_sk
    LEFT JOIN silver_ref.dim_geo gl
        ON gl.geo_id = f.geo_id
    LEFT JOIN gold_ddc.metric_catalog m
        ON m.variable_code = f.variable_code
       AND m.dataset_code = f.dataset
       AND m.vintage_year = f.estimate_year
    WHERE (p_start_date IS NULL OR MAKE_DATE(f.estimate_year, 1, 1) >= p_start_date)
      AND (p_end_date IS NULL OR MAKE_DATE(f.estimate_year, 1, 1) <= p_end_date)
      AND f.estimate_value IS NOT NULL;
    GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;

    INSERT INTO gold_pep_affected_keys (geo_id, variable_code, metric_code)
    SELECT DISTINCT d.geo_id, d.variable_code, COALESCE(d.metric_code, '')
    FROM gold_pep.rpt_pep_observations d
    WHERE (p_start_date IS NULL OR d.observation_date >= p_start_date)
      AND (p_end_date IS NULL OR d.observation_date <= p_end_date)
    ON CONFLICT DO NOTHING;

    SELECT COUNT(*) INTO v_affected_keys FROM gold_pep_affected_keys;
    RAISE NOTICE
        '[PEP RPT CHUNK] status=COMPLETE start=% end=% deleted_rows=% inserted_rows=% affected_keys=% duration_ms=%',
        p_start_date,
        p_end_date,
        v_deleted_rows,
        v_inserted_rows,
        v_affected_keys,
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::NUMERIC(18,2);
END;
$$;

DROP PROCEDURE IF EXISTS gold_pep.refresh_mv_pep_latest();
CREATE OR REPLACE PROCEDURE gold_pep.refresh_mv_pep_latest(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_started_at TIMESTAMPTZ := clock_timestamp();
    v_deleted_rows BIGINT;
    v_inserted_rows BIGINT;
BEGIN
    RAISE NOTICE '[PEP LATEST CHUNK] status=STARTED start=% end=%', p_start_date, p_end_date;

    IF to_regclass('pg_temp.gold_pep_affected_keys') IS NULL THEN
        CREATE TEMP TABLE gold_pep_affected_keys (
            geo_id        TEXT NOT NULL,
            variable_code TEXT NOT NULL,
            metric_code   TEXT NOT NULL,
            PRIMARY KEY (geo_id, variable_code, metric_code)
        ) ON COMMIT DROP;

        INSERT INTO gold_pep_affected_keys (geo_id, variable_code, metric_code)
        SELECT DISTINCT d.geo_id, d.variable_code, COALESCE(d.metric_code, '')
        FROM gold_pep.rpt_pep_observations d
        WHERE (p_start_date IS NULL OR d.observation_date >= p_start_date)
          AND (p_end_date IS NULL OR d.observation_date <= p_end_date)
        ON CONFLICT DO NOTHING;
    END IF;

    ANALYZE gold_pep_affected_keys;

    DELETE FROM gold_pep.mv_pep_latest m
    USING gold_pep_affected_keys k
    WHERE m.geo_id = k.geo_id
      AND m.variable_code = k.variable_code
      AND m.metric_code = k.metric_code;
    GET DIAGNOSTICS v_deleted_rows = ROW_COUNT;

    INSERT INTO gold_pep.mv_pep_latest
    SELECT latest.*
    FROM gold_pep_affected_keys k
    CROSS JOIN LATERAL (
        SELECT d.*
        FROM gold_pep.rpt_pep_observations d
        WHERE d.geo_id = k.geo_id
          AND d.variable_code = k.variable_code
          AND d.metric_code = k.metric_code
        ORDER BY
            d.observation_date DESC,
            d.updated_at DESC,
            d.vintage_year DESC
        LIMIT 1
    ) latest;
    GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;

    RAISE NOTICE
        '[PEP LATEST CHUNK] status=COMPLETE start=% end=% deleted_rows=% inserted_rows=% duration_ms=%',
        p_start_date,
        p_end_date,
        v_deleted_rows,
        v_inserted_rows,
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::NUMERIC(18,2);
END;
$$;

DROP PROCEDURE IF EXISTS gold_pep.refresh_dashboard_serving_layer_pep(DATE, DATE);
DROP PROCEDURE IF EXISTS gold_pep.refresh_dashboard_serving_layer_pep(DATE, DATE, BOOLEAN);
CREATE OR REPLACE PROCEDURE gold_pep.refresh_dashboard_serving_layer_pep(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL,
    p_force_full BOOLEAN DEFAULT FALSE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_started_at TIMESTAMPTZ := clock_timestamp();
    v_step_started TIMESTAMPTZ;
    v_watermark TIMESTAMPTZ;
    v_high_watermark TIMESTAMPTZ;
    v_effective_start DATE;
    v_effective_end DATE;
BEGIN
    SET LOCAL statement_timeout = '90min';
    SET LOCAL lock_timeout = '30s';

    INSERT INTO control.serving_refresh_state (
        source_code,
        last_silver_ingested_at,
        last_refresh_completed_at
    )
    SELECT
        'CENSUS_PEP',
        COALESCE(MAX(r.updated_at), '-infinity'::TIMESTAMPTZ),
        CASE WHEN COUNT(*) > 0 THEN NOW() ELSE NULL END
    FROM gold_pep.rpt_pep_observations r
    ON CONFLICT (source_code) DO NOTHING;

    SELECT last_silver_ingested_at
      INTO v_watermark
      FROM control.serving_refresh_state
     WHERE source_code = 'CENSUS_PEP'
     FOR UPDATE;

    UPDATE control.serving_refresh_state
       SET last_refresh_started_at = v_started_at,
           updated_at = NOW()
     WHERE source_code = 'CENSUS_PEP';

    SELECT
        MAX(s.ingested_at),
        MIN(MAKE_DATE(s.estimate_year, 1, 1)),
        MAX(MAKE_DATE(s.estimate_year, 1, 1))
      INTO v_high_watermark, v_effective_start, v_effective_end
      FROM silver_pep.fact_population s
     WHERE s.estimate_value IS NOT NULL
       AND (p_start_date IS NULL OR MAKE_DATE(s.estimate_year, 1, 1) >= p_start_date)
       AND (p_end_date IS NULL OR MAKE_DATE(s.estimate_year, 1, 1) <= p_end_date)
       AND (p_force_full OR s.ingested_at > v_watermark);

    IF v_effective_start IS NULL THEN
        UPDATE control.serving_refresh_state
           SET last_refresh_completed_at = clock_timestamp(),
               updated_at = NOW()
         WHERE source_code = 'CENSUS_PEP';
        RAISE NOTICE '[PEP DASHBOARD REFRESH] no changed silver rows after watermark=%', v_watermark;
        RETURN;
    END IF;

    RAISE NOTICE '[PEP DASHBOARD REFRESH] start window_start=% window_end=% watermark=% force_full=%',
        v_effective_start, v_effective_end, v_watermark, p_force_full;

    v_step_started := clock_timestamp();
    CALL gold_pep.refresh_rpt_pep_observations(v_effective_start, v_effective_end);
    RAISE NOTICE
        '[PEP DASHBOARD REFRESH] step=refresh_rpt_pep_observations duration_ms=%',
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_step_started)) * 1000)::NUMERIC(18,2);

    v_step_started := clock_timestamp();
    CALL gold_pep.refresh_mv_pep_latest(v_effective_start, v_effective_end);
    RAISE NOTICE
        '[PEP DASHBOARD REFRESH] step=refresh_mv_pep_latest duration_ms=%',
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_step_started)) * 1000)::NUMERIC(18,2);

    UPDATE control.serving_refresh_state
       SET last_silver_ingested_at = CASE
               WHEN p_force_full AND (p_start_date IS NOT NULL OR p_end_date IS NOT NULL)
                   THEN v_watermark
               ELSE GREATEST(v_watermark, v_high_watermark)
           END,
           last_refresh_completed_at = clock_timestamp(),
           last_window_start = v_effective_start,
           last_window_end = v_effective_end,
           updated_at = NOW()
     WHERE source_code = 'CENSUS_PEP';

    RAISE NOTICE
        '[PEP DASHBOARD REFRESH] completed total_duration_ms=%',
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::NUMERIC(18,2);
END;
$$;
