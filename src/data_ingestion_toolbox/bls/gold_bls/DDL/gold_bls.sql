-- bls/gold_bls/DDL/gold_bls.sql
-- REFACTORED: Source-First Architecture
-- Subject-scoped gold DDL for BLS objects — no unified wide table.
-- Serving tables contain BLS-specific columns only; no NULL pollution.

CREATE SCHEMA IF NOT EXISTS gold_bls;
CREATE EXTENSION IF NOT EXISTS postgis;

-- Shared glossary objects are owned by the ordered warehouse bootstrap.
-- This source component only owns relations in gold_bls.

CREATE TABLE IF NOT EXISTS gold_bls.dim_bls_survey (
    bls_survey_sk      BIGSERIAL PRIMARY KEY,
    program_code       TEXT NOT NULL UNIQUE,
    survey_name        TEXT NOT NULL,
    survey_universe    TEXT,
    observation_basis  TEXT NOT NULL CHECK (observation_basis IN ('PEOPLE', 'JOBS', 'PRICES', 'FLOWS')),
    primary_concept    TEXT,
    id_construction_type TEXT,
    comparison_warning TEXT,
    reference_url      TEXT,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold_bls.dim_bls_series (
    bls_series_sk              BIGSERIAL PRIMARY KEY,
    bls_survey_sk              BIGINT NOT NULL REFERENCES gold_bls.dim_bls_survey(bls_survey_sk),
    program_code               TEXT NOT NULL,
    series_id                  TEXT NOT NULL UNIQUE,
    series_title               TEXT,
    measure_name               TEXT,
    measure_category           TEXT NOT NULL CHECK (
        measure_category IN (
            'EMPLOYMENT', 'UNEMPLOYMENT', 'LABOR_FORCE', 'PARTICIPATION', 'POPULATION',
            'EARNINGS', 'HOURS', 'PRICE_INDEX', 'OPENINGS', 'HIRES', 'QUITS', 'LAYOFFS', 'SEPARATIONS',
            'OTHER'
        )
    ),
    unit_of_measure            TEXT,
    value_type                 TEXT NOT NULL CHECK (value_type IN ('LEVEL', 'RATE', 'INDEX', 'PERCENT', 'CURRENCY', 'RATIO', 'OTHER')),
    seasonal_adjustment_status TEXT,
    geographic_level           TEXT,
    gold_metric_name           TEXT,
    analytic_role              TEXT,
    semantic_notes             TEXT,
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- BLS FACT VIEW (unchanged — source of truth)
-- ============================================================

CREATE OR REPLACE VIEW gold_bls.fact_bls_observation AS
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
    s.period_date,
    s.duration_start,
    s.duration_end,
    sr.bls_survey_sk,
    sr.bls_series_sk,
    UPPER(s.program) AS program_code,
    s.value,
    s.period AS period_code,
    s.seasonal_adjustment AS seasonal_adjustment_status,
    sv.observation_basis,
    sr.measure_category,
    sr.value_type,
    CURRENT_DATE       AS as_of_date,
    s.ingested_at      AS updated_at
FROM silver_bls.fact_labor_statistics s
JOIN gold_bls.dim_bls_series sr ON sr.series_id = s.series_id
JOIN gold_bls.dim_bls_survey sv ON sv.bls_survey_sk = sr.bls_survey_sk
WHERE s.value IS NOT NULL
  AND s.series_id IS NOT NULL
  AND s.series_id <> '';

-- ============================================================
-- BLS-SCOPED SERVING TABLE (Source-First: BLS-specific columns only)
-- ============================================================

CREATE TABLE IF NOT EXISTS gold_bls.rpt_bls_observations (
    source_code                TEXT NOT NULL DEFAULT 'BLS',
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
    -- BLS-specific columns (no NULLs for these)
    series_id                  TEXT NOT NULL,
    program_code               TEXT NOT NULL,
    survey_name                TEXT,
    series_title               TEXT,
    measure_name               TEXT,
    measure_category           TEXT,
    observation_basis          TEXT,
    units                      TEXT,
    value                      NUMERIC NOT NULL,
    value_type                 TEXT,
    seasonal_adjustment_status TEXT,
    gold_metric_name           TEXT,
    comparison_warning         TEXT,
    -- Metric catalog association
    metric_code                TEXT,
    metric_display_name        TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_rpt_bls_observations_nk
    ON gold_bls.rpt_bls_observations (
        geo_id,
        observation_date,
        series_id,
        COALESCE(metric_code, '')
    );

CREATE INDEX IF NOT EXISTS ix_rpt_bls_observations_source_geo_date
    ON gold_bls.rpt_bls_observations (source_code, geo_id, observation_date);

CREATE INDEX IF NOT EXISTS ix_rpt_bls_observations_metric_date
    ON gold_bls.rpt_bls_observations (metric_code, observation_date);

CREATE INDEX IF NOT EXISTS ix_rpt_bls_observations_obs_brin
    ON gold_bls.rpt_bls_observations USING BRIN (observation_date);

CREATE INDEX IF NOT EXISTS ix_rpt_bls_observations_metric_geo_date
    ON gold_bls.rpt_bls_observations (metric_code, geo_id, observation_date);

CREATE INDEX IF NOT EXISTS ix_rpt_bls_observations_updated_at
    ON gold_bls.rpt_bls_observations (updated_at DESC);

CREATE INDEX IF NOT EXISTS ix_rpt_bls_latest_selection
    ON gold_bls.rpt_bls_observations (
        geo_id,
        series_id,
        metric_code,
        observation_date DESC,
        updated_at DESC
    );

-- ============================================================
-- BLS MATERIALIZED VIEW (Per-source latest)
-- ============================================================

CREATE TABLE IF NOT EXISTS gold_bls.mv_bls_latest
    (LIKE gold_bls.rpt_bls_observations INCLUDING DEFAULTS INCLUDING STORAGE INCLUDING COMMENTS);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_bls_latest
    ON gold_bls.mv_bls_latest (
        geo_id,
        series_id,
        COALESCE(metric_code, '')
    );

CREATE INDEX IF NOT EXISTS ix_mv_bls_latest_source_metric
    ON gold_bls.mv_bls_latest (source_code, metric_code);

CREATE INDEX IF NOT EXISTS ix_mv_bls_latest_observation_date
    ON gold_bls.mv_bls_latest (observation_date);

CREATE INDEX IF NOT EXISTS ix_mv_bls_latest_metric_geo
    ON gold_bls.mv_bls_latest (metric_code, geo_id);

-- ============================================================
-- BLS REFRESH PROCEDURES (Updated to populate per-source table)
-- ============================================================

DROP PROCEDURE IF EXISTS gold_bls.refresh_rpt_bls_observations(DATE, DATE);
CREATE OR REPLACE PROCEDURE gold_bls.refresh_rpt_bls_observations(
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
    RAISE NOTICE '[BLS RPT CHUNK] status=STARTED start=% end=%', p_start_date, p_end_date;

    DROP TABLE IF EXISTS pg_temp.gold_bls_affected_keys;
    CREATE TEMP TABLE gold_bls_affected_keys (
        geo_id      TEXT NOT NULL,
        series_id   TEXT NOT NULL,
        metric_code TEXT NOT NULL,
        PRIMARY KEY (geo_id, series_id, metric_code)
    ) ON COMMIT DROP;

    INSERT INTO gold_bls_affected_keys (geo_id, series_id, metric_code)
    SELECT DISTINCT d.geo_id, d.series_id, d.metric_code
    FROM gold_bls.rpt_bls_observations d
    WHERE (p_start_date IS NULL OR d.observation_date >= p_start_date)
      AND (p_end_date IS NULL OR d.observation_date <= p_end_date)
    ON CONFLICT DO NOTHING;

    DELETE FROM gold_bls.rpt_bls_observations
    WHERE (p_start_date IS NULL OR observation_date >= p_start_date)
      AND (p_end_date IS NULL OR observation_date <= p_end_date);
    GET DIAGNOSTICS v_deleted_rows = ROW_COUNT;

    INSERT INTO gold_bls.rpt_bls_observations (
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
        series_id,
        program_code,
        survey_name,
        series_title,
        measure_name,
        measure_category,
        observation_basis,
        units,
        value,
        value_type,
        seasonal_adjustment_status,
        gold_metric_name,
        comparison_warning,
        metric_code,
        metric_display_name
    )
    SELECT
        'BLS',
        b.period_date,
        b.duration_start,
        b.duration_end,
        b.time_sk,
        b.as_of_date,
        b.updated_at,
        b.geo_id,
        COALESCE(gl.geo_level, b.geo_level),
        gl.state_fips,
        gl.county_fips,
        gl.state_name,
        gl.county_name,
        gl.latitude,
        gl.longitude,
        bs.series_id,
        b.program_code,
        s.survey_name,
        bs.series_title,
        bs.measure_name,
        b.measure_category,
        COALESCE(b.observation_basis, s.observation_basis),
        bs.unit_of_measure,
        b.value,
        b.value_type,
        COALESCE(b.seasonal_adjustment_status, bs.seasonal_adjustment_status),
        bs.gold_metric_name,
        s.comparison_warning,
        'BLS:' || bs.series_id,
        COALESCE(bs.gold_metric_name, bs.series_title)
    FROM gold_bls.fact_bls_observation b
    JOIN gold_bls.dim_bls_survey s  ON s.bls_survey_sk  = b.bls_survey_sk
    JOIN gold_bls.dim_bls_series bs ON bs.bls_series_sk = b.bls_series_sk
    LEFT JOIN silver_ref.dim_geo gl ON gl.geo_id = b.geo_id
    WHERE (p_start_date IS NULL OR b.period_date >= p_start_date)
      AND (p_end_date IS NULL OR b.period_date <= p_end_date);
    GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;

    INSERT INTO gold_bls_affected_keys (geo_id, series_id, metric_code)
    SELECT DISTINCT d.geo_id, d.series_id, d.metric_code
    FROM gold_bls.rpt_bls_observations d
    WHERE (p_start_date IS NULL OR d.observation_date >= p_start_date)
      AND (p_end_date IS NULL OR d.observation_date <= p_end_date)
    ON CONFLICT DO NOTHING;

    SELECT COUNT(*) INTO v_affected_keys FROM gold_bls_affected_keys;
    RAISE NOTICE
        '[BLS RPT CHUNK] status=COMPLETE start=% end=% deleted_rows=% inserted_rows=% affected_keys=% duration_ms=%',
        p_start_date,
        p_end_date,
        v_deleted_rows,
        v_inserted_rows,
        v_affected_keys,
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::NUMERIC(18,2);
END;
$$;

DROP PROCEDURE IF EXISTS gold_bls.refresh_mv_bls_latest();
CREATE OR REPLACE PROCEDURE gold_bls.refresh_mv_bls_latest(
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
    RAISE NOTICE '[BLS LATEST CHUNK] status=STARTED start=% end=%', p_start_date, p_end_date;

    -- Always rebuild the BLS slice — bounded by N_series × N_geos, not N_observations.
    IF to_regclass('pg_temp.gold_bls_affected_keys') IS NULL THEN
        CREATE TEMP TABLE gold_bls_affected_keys (
            geo_id      TEXT NOT NULL,
            series_id   TEXT NOT NULL,
            metric_code TEXT NOT NULL,
            PRIMARY KEY (geo_id, series_id, metric_code)
        ) ON COMMIT DROP;

        INSERT INTO gold_bls_affected_keys (geo_id, series_id, metric_code)
        SELECT DISTINCT d.geo_id, d.series_id, d.metric_code
        FROM gold_bls.rpt_bls_observations d
        WHERE (p_start_date IS NULL OR d.observation_date >= p_start_date)
          AND (p_end_date IS NULL OR d.observation_date <= p_end_date)
        ON CONFLICT DO NOTHING;
    END IF;

    DELETE FROM gold_bls.mv_bls_latest m
    USING gold_bls_affected_keys k
    WHERE m.geo_id = k.geo_id
      AND m.series_id = k.series_id
      AND m.metric_code = k.metric_code;
    GET DIAGNOSTICS v_deleted_rows = ROW_COUNT;

    INSERT INTO gold_bls.mv_bls_latest
    SELECT DISTINCT ON (d.geo_id, d.series_id, d.metric_code)
        d.*
    FROM gold_bls.rpt_bls_observations d
    JOIN gold_bls_affected_keys k
      ON k.geo_id = d.geo_id
     AND k.series_id = d.series_id
     AND k.metric_code = d.metric_code
    ORDER BY
        d.geo_id,
        d.series_id,
        d.metric_code,
        d.observation_date DESC,
        d.updated_at DESC;
    GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;

    RAISE NOTICE
        '[BLS LATEST CHUNK] status=COMPLETE start=% end=% deleted_rows=% inserted_rows=% duration_ms=%',
        p_start_date,
        p_end_date,
        v_deleted_rows,
        v_inserted_rows,
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::NUMERIC(18,2);

END;
$$;

DROP PROCEDURE IF EXISTS gold_bls.refresh_dashboard_serving_layer_bls(DATE, DATE);
DROP PROCEDURE IF EXISTS gold_bls.refresh_dashboard_serving_layer_bls(DATE, DATE, BOOLEAN);
CREATE OR REPLACE PROCEDURE gold_bls.refresh_dashboard_serving_layer_bls(
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
    SET LOCAL statement_timeout = '60min';
    SET LOCAL lock_timeout = '30s';

    INSERT INTO control.serving_refresh_state (
        source_code,
        last_silver_ingested_at,
        last_refresh_completed_at
    )
    SELECT
        'BLS',
        COALESCE(MAX(r.updated_at), '-infinity'::TIMESTAMPTZ),
        CASE WHEN COUNT(*) > 0 THEN NOW() ELSE NULL END
    FROM gold_bls.rpt_bls_observations r
    ON CONFLICT (source_code) DO NOTHING;

    SELECT last_silver_ingested_at
      INTO v_watermark
      FROM control.serving_refresh_state
     WHERE source_code = 'BLS'
     FOR UPDATE;

    UPDATE control.serving_refresh_state
       SET last_refresh_started_at = v_started_at,
           updated_at = NOW()
     WHERE source_code = 'BLS';

    SELECT MAX(s.ingested_at), MIN(s.period_date), MAX(s.period_date)
      INTO v_high_watermark, v_effective_start, v_effective_end
      FROM silver_bls.fact_labor_statistics s
     WHERE s.value IS NOT NULL
       AND (p_start_date IS NULL OR s.period_date >= p_start_date)
       AND (p_end_date IS NULL OR s.period_date <= p_end_date)
       AND (p_force_full OR s.ingested_at > v_watermark);

    IF v_effective_start IS NULL THEN
        UPDATE control.serving_refresh_state
           SET last_refresh_completed_at = clock_timestamp(),
               updated_at = NOW()
         WHERE source_code = 'BLS';
        RAISE NOTICE '[BLS DASHBOARD REFRESH] no changed silver rows after watermark=%', v_watermark;
        RETURN;
    END IF;

    RAISE NOTICE '[BLS DASHBOARD REFRESH] start window_start=% window_end=% watermark=% force_full=%',
        v_effective_start, v_effective_end, v_watermark, p_force_full;

    v_step_started := clock_timestamp();
    CALL gold_bls.refresh_rpt_bls_observations(v_effective_start, v_effective_end);
    RAISE NOTICE
        '[BLS DASHBOARD REFRESH] step=refresh_rpt_bls_observations duration_ms=%',
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_step_started)) * 1000)::NUMERIC(18,2);

    v_step_started := clock_timestamp();
    CALL gold_bls.refresh_mv_bls_latest(v_effective_start, v_effective_end);
    RAISE NOTICE
        '[BLS DASHBOARD REFRESH] step=refresh_mv_bls_latest duration_ms=%',
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
     WHERE source_code = 'BLS';

    RAISE NOTICE
        '[BLS DASHBOARD REFRESH] completed total_duration_ms=%',
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::NUMERIC(18,2);
END;
$$;
