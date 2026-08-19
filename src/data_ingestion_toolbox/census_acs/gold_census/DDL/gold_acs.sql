-- census_acs/gold_census/DDL/gold_acs.sql
-- REFACTORED: Source-First Architecture
-- Subject-scoped gold DDL for ACS objects — no unified wide table.
-- Per-source serving table with ACS-specific columns only.

CREATE SCHEMA IF NOT EXISTS gold_census;
CREATE EXTENSION IF NOT EXISTS postgis;

-- Shared glossary objects are owned by the ordered warehouse bootstrap.
-- This source component only owns relations in gold_census.

CREATE TABLE IF NOT EXISTS gold_census.dim_acs_table (
    acs_table_sk      BIGSERIAL PRIMARY KEY,
    dataset_code      TEXT NOT NULL CHECK (dataset_code IN ('acs1', 'acs5')),
    vintage_year      INTEGER NOT NULL,
    table_id          TEXT NOT NULL,
    table_title       TEXT,
    concept           TEXT,
    universe          TEXT,
    survey_span_years INTEGER NOT NULL CHECK (survey_span_years IN (1, 5)),
    reference_url     TEXT,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dataset_code, vintage_year, table_id)
);

CREATE TABLE IF NOT EXISTS gold_census.dim_acs_variable (
    acs_variable_sk        BIGSERIAL PRIMARY KEY,
    acs_table_sk           BIGINT NOT NULL REFERENCES gold_census.dim_acs_table(acs_table_sk),
    dataset_code           TEXT NOT NULL CHECK (dataset_code IN ('acs1', 'acs5')),
    vintage_year           INTEGER NOT NULL,
    variable_code          TEXT NOT NULL,
    variable_label         TEXT,
    concept                TEXT,
    universe               TEXT,
    value_role             TEXT NOT NULL CHECK (value_role IN ('ESTIMATE', 'MOE', 'ANNOTATION')),
    denominator_hint       TEXT,
    is_publishable_default BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dataset_code, vintage_year, variable_code)
);

-- ============================================================
-- ACS FACT VIEW (source of truth)
-- ============================================================

CREATE OR REPLACE VIEW gold_census.fact_acs_observation AS
SELECT
    s.geo_id,
    CASE
        WHEN LOWER(s.geo_level) = 'us'     THEN 'NATIONAL'
        WHEN LOWER(s.geo_level) = 'state'  THEN 'STATE'
        WHEN LOWER(s.geo_level) = 'county' THEN 'COUNTY'
        WHEN s.geo_id = 'us:1'             THEN 'NATIONAL'
        WHEN s.geo_id LIKE 'state:%|county:%' THEN 'COUNTY'
        WHEN s.geo_id LIKE 'state:%'       THEN 'STATE'
        ELSE 'NATIONAL'
    END AS geo_level,
    s.time_sk,
    MAKE_DATE(s.estimate_year, 1, 1) AS observation_date,
    s.duration_start,
    s.duration_end,
    av.acs_table_sk,
    av.acs_variable_sk,
    s.dataset     AS dataset_code,
    s.estimate_year AS vintage_year,
    s.estimate_value,
    s.margin_of_error,
    s.margin_of_error_pct,
    NULL::TEXT AS estimate_annotation,
    NULL::TEXT AS moe_annotation,
    CURRENT_DATE AS as_of_date,
    s.ingested_at AS updated_at
FROM silver_census.fact_demographics s
JOIN gold_census.dim_acs_variable av
    ON av.dataset_code  = s.dataset
   AND av.vintage_year  = s.estimate_year
   AND av.variable_code = s.variable_code
WHERE s.estimate_value IS NOT NULL
  AND s.variable_code IS NOT NULL
  AND s.variable_code <> '';

-- ============================================================
-- ACS-SCOPED SERVING TABLE (Source-First: ACS-specific columns only)
-- ============================================================

CREATE TABLE IF NOT EXISTS gold_census.rpt_acs_observations (
    source_code                TEXT NOT NULL DEFAULT 'CENSUS_ACS',
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
    state_name                 TEXT,
    county_name                TEXT,
    geo_latitude               DOUBLE PRECISION,
    geo_longitude              DOUBLE PRECISION,
    -- ACS-specific columns (no NULLs for these)
    value                      NUMERIC NOT NULL,
    dataset_code               TEXT NOT NULL CHECK (dataset_code IN ('acs1', 'acs5')),
    vintage_year               INTEGER NOT NULL,
    table_id                   TEXT NOT NULL,
    table_title                TEXT,
    variable_code              TEXT NOT NULL,
    variable_label             TEXT,
    concept                    TEXT,
    universe                   TEXT,
    denominator_hint           TEXT,
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

CREATE UNIQUE INDEX IF NOT EXISTS uq_rpt_acs_observations_nk
    ON gold_census.rpt_acs_observations (
        geo_id,
        observation_date,
        dataset_code,
        vintage_year,
        variable_code,
        COALESCE(metric_code, '')
    );

CREATE INDEX IF NOT EXISTS ix_rpt_acs_observations_source_geo_date
    ON gold_census.rpt_acs_observations (source_code, geo_id, observation_date);

CREATE INDEX IF NOT EXISTS ix_rpt_acs_observations_metric_date
    ON gold_census.rpt_acs_observations (metric_code, observation_date);

CREATE INDEX IF NOT EXISTS ix_rpt_acs_observations_dataset_vintage
    ON gold_census.rpt_acs_observations (dataset_code, vintage_year);

CREATE INDEX IF NOT EXISTS ix_rpt_acs_observations_metric_geo_date
    ON gold_census.rpt_acs_observations (metric_code, geo_id, observation_date);

CREATE INDEX IF NOT EXISTS ix_rpt_acs_observations_updated_at
    ON gold_census.rpt_acs_observations (updated_at DESC);

CREATE INDEX IF NOT EXISTS ix_rpt_acs_latest_selection
    ON gold_census.rpt_acs_observations (
        geo_id,
        variable_code,
        metric_code,
        observation_date DESC,
        updated_at DESC,
        (CASE dataset_code WHEN 'acs1' THEN 1 WHEN 'acs5' THEN 2 ELSE 9 END),
        vintage_year DESC
    );

-- ============================================================
-- ACS MATERIALIZED VIEW (Per-source latest)
-- ============================================================

CREATE TABLE IF NOT EXISTS gold_census.mv_acs_latest
    (LIKE gold_census.rpt_acs_observations INCLUDING DEFAULTS INCLUDING STORAGE INCLUDING COMMENTS);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_acs_latest
    ON gold_census.mv_acs_latest (
        geo_id,
        dataset_code,
        vintage_year,
        variable_code,
        COALESCE(metric_code, '')
    );

CREATE INDEX IF NOT EXISTS ix_mv_acs_latest_source_metric
    ON gold_census.mv_acs_latest (source_code, metric_code);

CREATE INDEX IF NOT EXISTS ix_mv_acs_latest_vintage
    ON gold_census.mv_acs_latest (dataset_code, vintage_year);

CREATE INDEX IF NOT EXISTS ix_mv_acs_latest_metric_geo
    ON gold_census.mv_acs_latest (metric_code, geo_id);

-- ============================================================
-- ACS REFRESH PROCEDURES
-- ============================================================

DROP PROCEDURE IF EXISTS gold_census.refresh_rpt_acs_observations(DATE, DATE);
CREATE OR REPLACE PROCEDURE gold_census.refresh_rpt_acs_observations(
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
    RAISE NOTICE '[ACS RPT CHUNK] status=STARTED start=% end=%', p_start_date, p_end_date;

    DROP TABLE IF EXISTS pg_temp.gold_acs_affected_keys;
    CREATE TEMP TABLE gold_acs_affected_keys (
        geo_id        TEXT NOT NULL,
        variable_code TEXT NOT NULL,
        metric_code   TEXT NOT NULL,
        PRIMARY KEY (geo_id, variable_code, metric_code)
    ) ON COMMIT DROP;

    -- Capture old keys as well as new keys so a source-side deletion removes a
    -- now-stale latest row.
    INSERT INTO gold_acs_affected_keys (geo_id, variable_code, metric_code)
    SELECT DISTINCT d.geo_id, d.variable_code, d.metric_code
    FROM gold_census.rpt_acs_observations d
    WHERE (p_start_date IS NULL OR d.observation_date >= p_start_date)
      AND (p_end_date IS NULL OR d.observation_date <= p_end_date)
    ON CONFLICT DO NOTHING;

    DELETE FROM gold_census.rpt_acs_observations
    WHERE (p_start_date IS NULL OR observation_date >= p_start_date)
      AND (p_end_date IS NULL OR observation_date <= p_end_date);
    GET DIAGNOSTICS v_deleted_rows = ROW_COUNT;

    INSERT INTO gold_census.rpt_acs_observations (
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
        state_name,
        county_name,
        geo_latitude,
        geo_longitude,
        metric_code,
        metric_display_name,
        value,
        dataset_code,
        vintage_year,
        table_id,
        table_title,
        variable_code,
        variable_label,
        concept,
        universe,
        denominator_hint,
        is_publishable_default,
        estimate_value,
        margin_of_error,
        margin_of_error_pct,
        estimate_annotation,
        moe_annotation
    )
    SELECT
        'CENSUS_ACS',
        ao.observation_date,
        ao.duration_start,
        ao.duration_end,
        ao.time_sk,
        ao.as_of_date,
        ao.updated_at,
        ao.geo_id,
        COALESCE(gl.geo_level, ao.geo_level),
        gl.state_fips,
        gl.county_fips,
        gl.state_name,
        gl.county_name,
        gl.latitude,
        gl.longitude,
        'ACS:' || ao.dataset_code || ':' || v.variable_code,
        v.variable_label,
        ao.estimate_value,
        ao.dataset_code,
        ao.vintage_year,
        t.table_id,
        t.table_title,
        v.variable_code,
        v.variable_label,
        COALESCE(v.concept,   t.concept),
        COALESCE(v.universe,  t.universe),
        v.denominator_hint,
        v.is_publishable_default,
        ao.estimate_value,
        ao.margin_of_error,
        ao.margin_of_error_pct,
        ao.estimate_annotation,
        ao.moe_annotation
    FROM gold_census.fact_acs_observation ao
    JOIN gold_census.dim_acs_table    t  ON t.acs_table_sk    = ao.acs_table_sk
    JOIN gold_census.dim_acs_variable v  ON v.acs_variable_sk = ao.acs_variable_sk
    LEFT JOIN silver_ref.dim_geo gl ON gl.geo_id = ao.geo_id
    WHERE (p_start_date IS NULL OR ao.observation_date >= p_start_date)
      AND (p_end_date IS NULL OR ao.observation_date <= p_end_date);
    GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;

    INSERT INTO gold_acs_affected_keys (geo_id, variable_code, metric_code)
    SELECT DISTINCT d.geo_id, d.variable_code, d.metric_code
    FROM gold_census.rpt_acs_observations d
    WHERE (p_start_date IS NULL OR d.observation_date >= p_start_date)
      AND (p_end_date IS NULL OR d.observation_date <= p_end_date)
    ON CONFLICT DO NOTHING;

    SELECT COUNT(*) INTO v_affected_keys FROM gold_acs_affected_keys;
    RAISE NOTICE
        '[ACS RPT CHUNK] status=COMPLETE start=% end=% deleted_rows=% inserted_rows=% affected_keys=% duration_ms=%',
        p_start_date,
        p_end_date,
        v_deleted_rows,
        v_inserted_rows,
        v_affected_keys,
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::NUMERIC(18,2);
END;
$$;

DROP PROCEDURE IF EXISTS gold_census.refresh_mv_acs_latest();
CREATE OR REPLACE PROCEDURE gold_census.refresh_mv_acs_latest(
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
    RAISE NOTICE '[ACS LATEST CHUNK] status=STARTED start=% end=%', p_start_date, p_end_date;

    IF to_regclass('pg_temp.gold_acs_affected_keys') IS NULL THEN
        CREATE TEMP TABLE gold_acs_affected_keys (
            geo_id        TEXT NOT NULL,
            variable_code TEXT NOT NULL,
            metric_code   TEXT NOT NULL,
            PRIMARY KEY (geo_id, variable_code, metric_code)
        ) ON COMMIT DROP;

        INSERT INTO gold_acs_affected_keys (geo_id, variable_code, metric_code)
        SELECT DISTINCT d.geo_id, d.variable_code, d.metric_code
        FROM gold_census.rpt_acs_observations d
        WHERE (p_start_date IS NULL OR d.observation_date >= p_start_date)
          AND (p_end_date IS NULL OR d.observation_date <= p_end_date)
        ON CONFLICT DO NOTHING;
    END IF;

    DELETE FROM gold_census.mv_acs_latest m
    USING gold_acs_affected_keys k
    WHERE m.geo_id = k.geo_id
      AND m.variable_code = k.variable_code
      AND m.metric_code = k.metric_code;
    GET DIAGNOSTICS v_deleted_rows = ROW_COUNT;

    INSERT INTO gold_census.mv_acs_latest
    SELECT DISTINCT ON (d.geo_id, d.variable_code, d.metric_code)
        d.*
    FROM gold_census.rpt_acs_observations d
    JOIN gold_acs_affected_keys k
      ON k.geo_id = d.geo_id
     AND k.variable_code = d.variable_code
     AND k.metric_code = d.metric_code
    ORDER BY
        d.geo_id,
        d.variable_code,
        d.metric_code,
        d.observation_date DESC,
        d.updated_at DESC,
        CASE d.dataset_code WHEN 'acs1' THEN 1 WHEN 'acs5' THEN 2 ELSE 9 END,
        d.vintage_year DESC;
    GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;

    RAISE NOTICE
        '[ACS LATEST CHUNK] status=COMPLETE start=% end=% deleted_rows=% inserted_rows=% duration_ms=%',
        p_start_date,
        p_end_date,
        v_deleted_rows,
        v_inserted_rows,
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::NUMERIC(18,2);
END;
$$;

DROP PROCEDURE IF EXISTS gold_census.refresh_dashboard_serving_layer_acs(DATE, DATE);
DROP PROCEDURE IF EXISTS gold_census.refresh_dashboard_serving_layer_acs(DATE, DATE, BOOLEAN);
CREATE OR REPLACE PROCEDURE gold_census.refresh_dashboard_serving_layer_acs(
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
        'CENSUS_ACS',
        COALESCE(MAX(r.updated_at), '-infinity'::TIMESTAMPTZ),
        CASE WHEN COUNT(*) > 0 THEN NOW() ELSE NULL END
    FROM gold_census.rpt_acs_observations r
    ON CONFLICT (source_code) DO NOTHING;

    SELECT last_silver_ingested_at
      INTO v_watermark
      FROM control.serving_refresh_state
     WHERE source_code = 'CENSUS_ACS'
     FOR UPDATE;

    UPDATE control.serving_refresh_state
       SET last_refresh_started_at = v_started_at,
           updated_at = NOW()
     WHERE source_code = 'CENSUS_ACS';

    SELECT
        MAX(s.ingested_at),
        MIN(MAKE_DATE(s.estimate_year, 1, 1)),
        MAX(MAKE_DATE(s.estimate_year, 1, 1))
      INTO v_high_watermark, v_effective_start, v_effective_end
      FROM silver_census.fact_demographics s
     WHERE s.estimate_value IS NOT NULL
       AND (p_start_date IS NULL OR MAKE_DATE(s.estimate_year, 1, 1) >= p_start_date)
       AND (p_end_date IS NULL OR MAKE_DATE(s.estimate_year, 1, 1) <= p_end_date)
       AND (p_force_full OR s.ingested_at > v_watermark);

    IF v_effective_start IS NULL THEN
        UPDATE control.serving_refresh_state
           SET last_refresh_completed_at = clock_timestamp(),
               updated_at = NOW()
         WHERE source_code = 'CENSUS_ACS';
        RAISE NOTICE '[ACS DASHBOARD REFRESH] no changed silver rows after watermark=%', v_watermark;
        RETURN;
    END IF;

    RAISE NOTICE '[ACS DASHBOARD REFRESH] start window_start=% window_end=% watermark=% force_full=%',
        v_effective_start, v_effective_end, v_watermark, p_force_full;

    v_step_started := clock_timestamp();
    CALL gold_census.refresh_rpt_acs_observations(v_effective_start, v_effective_end);
    RAISE NOTICE
        '[ACS DASHBOARD REFRESH] step=refresh_rpt_acs_observations duration_ms=%',
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_step_started)) * 1000)::NUMERIC(18,2);

    v_step_started := clock_timestamp();
    CALL gold_census.refresh_mv_acs_latest(v_effective_start, v_effective_end);
    RAISE NOTICE
        '[ACS DASHBOARD REFRESH] step=refresh_mv_acs_latest duration_ms=%',
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
     WHERE source_code = 'CENSUS_ACS';

    RAISE NOTICE
        '[ACS DASHBOARD REFRESH] completed total_duration_ms=%',
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::NUMERIC(18,2);
END;
$$;
