-- Migration: Optimize gold dashboard refresh procedures
-- Date: 2026-04-24
-- Scope: Procedure-only migration (no table/view DDL changes)
-- Purpose:
--   1) Remove loop batching in rpt refresh procedures (single-pass inserts)
--   2) Remove candidates-table double writes in latest refresh procedures
--   3) Keep existing atomic swap behavior and full refresh entry point

BEGIN;

DROP PROCEDURE IF EXISTS gold.refresh_rpt_acs_observation_dashboard();
CREATE OR REPLACE PROCEDURE gold.refresh_rpt_acs_observation_dashboard()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS gold.rpt_acs_observation_dashboard__staging;
    DROP TABLE IF EXISTS gold.rpt_acs_observation_dashboard__old;

    CREATE TABLE gold.rpt_acs_observation_dashboard__staging
        (LIKE gold.rpt_acs_observation_dashboard INCLUDING ALL);

    INSERT INTO gold.rpt_acs_observation_dashboard__staging (
            source_code,
            observation_date,
            duration_start,
            duration_end,
            time_sk,
            geo_id,
            geo_level,
            state_fips,
            county_fips,
            state_name,
            county_name,
            geo_latitude,
            geo_longitude,
            geo_geom,
            geo_polygon_geojson,
            as_of_date,
            updated_at,
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
            moe_annotation,
            metric_code,
            metric_display_name,
            dashboard_suitability,
            business_definition,
            caveats,
            comparability_group,
            do_not_compare_with,
            recommended_aggregation,
            owner_team
    )
    WITH geo_base AS (
            SELECT DISTINCT ON (g.geo_id)
                g.geo_id,
                g.geo_level,
                LPAD(g.state_fips::TEXT, 2, '0') AS state_fips,
                CASE
                    WHEN g.county_fips IS NOT NULL THEN LPAD(g.county_fips::TEXT, 3, '0')
                    ELSE NULL
                END AS county_fips,
                g.state_name,
                g.county_name,
                g.latitude,
                g.longitude,
                g.geom,
                g.geo_polygon_geojson
            FROM gold.dim_geo g
            WHERE g.is_active = TRUE
            ORDER BY g.geo_id, g.source_year DESC NULLS LAST, g.ingested_at DESC
    )
    SELECT
            'CENSUS_ACS' AS source_code,
            ao.observation_date,
            ao.duration_start,
            ao.duration_end,
            ao.time_sk,
            ao.geo_id,
            COALESCE(gb.geo_level, ao.geo_level) AS geo_level,
            COALESCE(gb.state_fips, ao.state_id) AS state_fips,
            COALESCE(gb.county_fips, RIGHT(ao.county_id, 3)) AS county_fips,
            COALESCE(gb.state_name, ao.state_name) AS state_name,
            COALESCE(gb.county_name, ao.county_name) AS county_name,
            COALESCE(gb.latitude, ao.geo_latitude) AS geo_latitude,
            COALESCE(gb.longitude, ao.geo_longitude) AS geo_longitude,
            COALESCE(gb.geom, ao.geo_geom) AS geo_geom,
            COALESCE(gb.geo_polygon_geojson, CASE WHEN ao.geo_geom IS NOT NULL THEN ST_AsGeoJSON(ao.geo_geom)::TEXT ELSE NULL END) AS geo_polygon_geojson,
            ao.as_of_date,
            ao.updated_at,
            ao.dataset_code,
            ao.vintage_year,
            t.table_id,
            t.table_title,
            v.variable_code,
            v.variable_label,
            COALESCE(v.concept, t.concept) AS concept,
            COALESCE(v.universe, t.universe) AS universe,
            v.denominator_hint,
            v.is_publishable_default,
            ao.estimate_value,
            ao.margin_of_error,
            ao.margin_of_error_pct,
            ao.estimate_annotation,
            ao.moe_annotation,
            COALESCE(mc.metric_code, 'ACS:' || ao.dataset_code || ':' || v.variable_code) AS metric_code,
            COALESCE(mc.metric_display_name, v.variable_label) AS metric_display_name,
            COALESCE(mc.dashboard_suitability, 'EXPERIMENTAL') AS dashboard_suitability,
            mc.business_definition,
            mc.caveats,
            mc.comparability_group,
            COALESCE(mc.do_not_compare_with, ARRAY[]::TEXT[]) AS do_not_compare_with,
            mc.recommended_aggregation,
            mc.owner_team
        FROM gold.fact_acs_observation ao
        JOIN gold.dim_acs_table t
            ON t.acs_table_sk = ao.acs_table_sk
        JOIN gold.dim_acs_variable v
            ON v.acs_variable_sk = ao.acs_variable_sk
        LEFT JOIN geo_base gb
            ON gb.geo_id = ao.geo_id
        LEFT JOIN gold.bridge_metric_acs_variable bma
            ON bma.acs_variable_sk = ao.acs_variable_sk
        LEFT JOIN gold.dim_metric_catalog mc
            ON mc.metric_catalog_sk = bma.metric_catalog_sk
           AND mc.is_active = TRUE;

    ANALYZE gold.rpt_acs_observation_dashboard__staging;

    LOCK TABLE gold.rpt_acs_observation_dashboard IN ACCESS EXCLUSIVE MODE;

    ALTER TABLE gold.rpt_acs_observation_dashboard
        RENAME TO rpt_acs_observation_dashboard__old;

    ALTER TABLE gold.rpt_acs_observation_dashboard__staging
        RENAME TO rpt_acs_observation_dashboard;

    DROP TABLE gold.rpt_acs_observation_dashboard__old;
END;
$$;

DROP PROCEDURE IF EXISTS gold.refresh_rpt_bls_observation_dashboard();
CREATE OR REPLACE PROCEDURE gold.refresh_rpt_bls_observation_dashboard()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS gold.rpt_bls_observation_dashboard__staging;
    DROP TABLE IF EXISTS gold.rpt_bls_observation_dashboard__old;

    CREATE TABLE gold.rpt_bls_observation_dashboard__staging
        (LIKE gold.rpt_bls_observation_dashboard INCLUDING ALL);

    INSERT INTO gold.rpt_bls_observation_dashboard__staging (
            source_code,
            observation_date,
            duration_start,
            duration_end,
            time_sk,
            geo_id,
            geo_level,
            state_fips,
            county_fips,
            state_name,
            county_name,
            geo_latitude,
            geo_longitude,
            geo_geom,
            geo_polygon_geojson,
            as_of_date,
            updated_at,
            program_code,
            survey_name,
            series_id,
            series_title,
            gold_metric_name,
            measure_name,
            measure_category,
            value_type,
            unit_of_measure,
            seasonal_adjustment_status,
            observation_basis,
            value,
            metric_code,
            metric_display_name,
            dashboard_suitability,
            business_definition,
            metric_caveats,
            comparison_warning,
            comparability_group,
            recommended_aggregation,
            owner_team
    )
    WITH geo_base AS (
            SELECT DISTINCT ON (g.geo_id)
                g.geo_id,
                g.geo_level,
                LPAD(g.state_fips::TEXT, 2, '0') AS state_fips,
                CASE
                    WHEN g.county_fips IS NOT NULL THEN LPAD(g.county_fips::TEXT, 3, '0')
                    ELSE NULL
                END AS county_fips,
                g.state_name,
                g.county_name,
                g.latitude,
                g.longitude,
                g.geom,
                g.geo_polygon_geojson
            FROM gold.dim_geo g
            WHERE g.is_active = TRUE
            ORDER BY g.geo_id, g.source_year DESC NULLS LAST, g.ingested_at DESC
    )
    SELECT
            'BLS' AS source_code,
            b.period_date AS observation_date,
            b.duration_start,
            b.duration_end,
            b.time_sk,
            b.geo_id,
            COALESCE(gb.geo_level, b.geo_level) AS geo_level,
            COALESCE(gb.state_fips, b.state_id) AS state_fips,
            COALESCE(gb.county_fips, RIGHT(b.county_id, 3)) AS county_fips,
            COALESCE(gb.state_name, b.state_name) AS state_name,
            COALESCE(gb.county_name, b.county_name) AS county_name,
            COALESCE(gb.latitude, b.geo_latitude) AS geo_latitude,
            COALESCE(gb.longitude, b.geo_longitude) AS geo_longitude,
            COALESCE(gb.geom, b.geo_geom) AS geo_geom,
            COALESCE(gb.geo_polygon_geojson, CASE WHEN b.geo_geom IS NOT NULL THEN ST_AsGeoJSON(b.geo_geom)::TEXT ELSE NULL END) AS geo_polygon_geojson,
            b.as_of_date,
            b.updated_at,
            b.program_code,
            s.survey_name,
            bs.series_id,
            bs.series_title,
            bs.gold_metric_name,
            bs.measure_name,
            b.measure_category,
            b.value_type,
            bs.unit_of_measure,
            COALESCE(b.seasonal_adjustment_status, bs.seasonal_adjustment_status) AS seasonal_adjustment_status,
            COALESCE(b.observation_basis, s.observation_basis) AS observation_basis,
            b.value,
            COALESCE(mc.metric_code, 'BLS:' || bs.series_id) AS metric_code,
            COALESCE(mc.metric_display_name, bs.gold_metric_name, bs.series_title) AS metric_display_name,
            COALESCE(mc.dashboard_suitability, 'EXPERIMENTAL') AS dashboard_suitability,
            mc.business_definition,
            mc.caveats AS metric_caveats,
            s.comparison_warning,
            mc.comparability_group,
            mc.recommended_aggregation,
            mc.owner_team
        FROM gold.fact_bls_observation b
        JOIN gold.dim_bls_survey s
            ON s.bls_survey_sk = b.bls_survey_sk
        JOIN gold.dim_bls_series bs
            ON bs.bls_series_sk = b.bls_series_sk
        LEFT JOIN geo_base gb
            ON gb.geo_id = b.geo_id
        LEFT JOIN gold.bridge_metric_bls_series bms
            ON bms.bls_series_sk = b.bls_series_sk
        LEFT JOIN gold.dim_metric_catalog mc
            ON mc.metric_catalog_sk = bms.metric_catalog_sk
           AND mc.is_active = TRUE;

    ANALYZE gold.rpt_bls_observation_dashboard__staging;

    LOCK TABLE gold.rpt_bls_observation_dashboard IN ACCESS EXCLUSIVE MODE;

    ALTER TABLE gold.rpt_bls_observation_dashboard
        RENAME TO rpt_bls_observation_dashboard__old;

    ALTER TABLE gold.rpt_bls_observation_dashboard__staging
        RENAME TO rpt_bls_observation_dashboard;

    DROP TABLE gold.rpt_bls_observation_dashboard__old;
END;
$$;

DROP PROCEDURE IF EXISTS gold.refresh_rpt_fred_observation_dashboard();
CREATE OR REPLACE PROCEDURE gold.refresh_rpt_fred_observation_dashboard()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS gold.rpt_fred_observation_dashboard__staging;
    DROP TABLE IF EXISTS gold.rpt_fred_observation_dashboard__old;

    CREATE TABLE gold.rpt_fred_observation_dashboard__staging
        (LIKE gold.rpt_fred_observation_dashboard INCLUDING ALL);

    INSERT INTO gold.rpt_fred_observation_dashboard__staging (
            source_code,
            observation_date,
            duration_start,
            duration_end,
            time_sk,
            geo_id,
            geo_level,
            state_fips,
            county_fips,
            state_name,
            county_name,
            geo_latitude,
            geo_longitude,
            geo_geom,
            geo_polygon_geojson,
            as_of_date,
            updated_at,
            series_id,
            series_title,
            source_provider,
            original_source_name,
            is_primary_source_series,
            is_republished_series,
            frequency,
            units,
            seasonal_adjustment,
            transformation_method,
            realtime_start,
            realtime_end,
            value,
            metric_code,
            metric_display_name,
            dashboard_suitability,
            business_definition,
            caveats,
            comparability_group,
            do_not_compare_with,
            recommended_aggregation,
            owner_team
    )
    WITH geo_base AS (
            SELECT DISTINCT ON (g.geo_id)
                g.geo_id,
                g.geo_level,
                LPAD(g.state_fips::TEXT, 2, '0') AS state_fips,
                CASE
                    WHEN g.county_fips IS NOT NULL THEN LPAD(g.county_fips::TEXT, 3, '0')
                    ELSE NULL
                END AS county_fips,
                g.state_name,
                g.county_name,
                g.latitude,
                g.longitude,
                g.geom,
                g.geo_polygon_geojson
            FROM gold.dim_geo g
            WHERE g.is_active = TRUE
            ORDER BY g.geo_id, g.source_year DESC NULLS LAST, g.ingested_at DESC
    )
    SELECT
            'FRED' AS source_code,
            f.observation_date,
            f.duration_start,
            f.duration_end,
            f.time_sk,
            'us:1' AS geo_id,
            COALESCE(gb.geo_level, f.geo_level, 'NATIONAL') AS geo_level,
            gb.state_fips,
            gb.county_fips,
            gb.state_name,
            gb.county_name,
            gb.latitude AS geo_latitude,
            gb.longitude AS geo_longitude,
            gb.geom AS geo_geom,
            gb.geo_polygon_geojson,
            f.as_of_date,
            f.updated_at,
            fs.series_id,
            fs.series_title,
            COALESCE(f.source_provider, fs.source_provider) AS source_provider,
            fs.original_source_name,
            fs.is_primary_source_series,
            fs.is_republished_series,
            COALESCE(f.frequency, fs.frequency) AS frequency,
            COALESCE(f.units, fs.units) AS units,
            COALESCE(f.seasonal_adjustment, fs.seasonal_adjustment) AS seasonal_adjustment,
            COALESCE(fs.transformation_method, f.transform_applied) AS transformation_method,
            f.realtime_start,
            f.realtime_end,
            f.value,
            COALESCE(mc.metric_code, 'FRED:' || fs.series_id) AS metric_code,
            COALESCE(mc.metric_display_name, fs.series_title) AS metric_display_name,
            COALESCE(mc.dashboard_suitability, 'EXPERIMENTAL') AS dashboard_suitability,
            mc.business_definition,
            mc.caveats,
            mc.comparability_group,
            COALESCE(mc.do_not_compare_with, ARRAY[]::TEXT[]) AS do_not_compare_with,
            mc.recommended_aggregation,
            mc.owner_team
        FROM gold.fact_fred_observation f
        JOIN gold.dim_fred_series fs
            ON fs.fred_series_sk = f.fred_series_sk
        LEFT JOIN geo_base gb
            ON gb.geo_id = 'us:1'
        LEFT JOIN gold.bridge_metric_fred_series bmf
            ON bmf.fred_series_sk = f.fred_series_sk
        LEFT JOIN gold.dim_metric_catalog mc
            ON mc.metric_catalog_sk = bmf.metric_catalog_sk
           AND mc.is_active = TRUE;

    ANALYZE gold.rpt_fred_observation_dashboard__staging;

    LOCK TABLE gold.rpt_fred_observation_dashboard IN ACCESS EXCLUSIVE MODE;

    ALTER TABLE gold.rpt_fred_observation_dashboard
        RENAME TO rpt_fred_observation_dashboard__old;

    ALTER TABLE gold.rpt_fred_observation_dashboard__staging
        RENAME TO rpt_fred_observation_dashboard;

    DROP TABLE gold.rpt_fred_observation_dashboard__old;
END;
$$;

DROP PROCEDURE IF EXISTS gold.refresh_mv_acs_latest_dashboard();
CREATE OR REPLACE PROCEDURE gold.refresh_mv_acs_latest_dashboard()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS gold.mv_acs_latest_dashboard__staging;
    DROP TABLE IF EXISTS gold.mv_acs_latest_dashboard__old;

    CREATE TABLE gold.mv_acs_latest_dashboard__staging
        (LIKE gold.mv_acs_latest_dashboard INCLUDING ALL);

    INSERT INTO gold.mv_acs_latest_dashboard__staging (
        source_code,
        observation_date,
        duration_start,
        duration_end,
        time_sk,
        geo_id,
        geo_level,
        state_fips,
        county_fips,
        state_name,
        county_name,
        geo_latitude,
        geo_longitude,
        geo_geom,
        geo_polygon_geojson,
        as_of_date,
        updated_at,
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
        moe_annotation,
        metric_code,
        metric_display_name,
        dashboard_suitability,
        business_definition,
        caveats,
        comparability_group,
        do_not_compare_with,
        recommended_aggregation,
        owner_team
    )
    SELECT DISTINCT ON (d.geo_id, d.variable_code, d.metric_code)
        d.source_code,
        d.observation_date,
        d.duration_start,
        d.duration_end,
        d.time_sk,
        d.geo_id,
        d.geo_level,
        d.state_fips,
        d.county_fips,
        d.state_name,
        d.county_name,
        d.geo_latitude,
        d.geo_longitude,
        d.geo_geom,
        d.geo_polygon_geojson,
        d.as_of_date,
        d.updated_at,
        d.dataset_code,
        d.vintage_year,
        d.table_id,
        d.table_title,
        d.variable_code,
        d.variable_label,
        d.concept,
        d.universe,
        d.denominator_hint,
        d.is_publishable_default,
        d.estimate_value,
        d.margin_of_error,
        d.margin_of_error_pct,
        d.estimate_annotation,
        d.moe_annotation,
        d.metric_code,
        d.metric_display_name,
        d.dashboard_suitability,
        d.business_definition,
        d.caveats,
        d.comparability_group,
        d.do_not_compare_with,
        d.recommended_aggregation,
        d.owner_team
    FROM gold.rpt_acs_observation_dashboard d
    ORDER BY
        d.geo_id,
        d.variable_code,
        d.metric_code,
        d.observation_date DESC,
        d.updated_at DESC,
        CASE d.dataset_code WHEN 'acs1' THEN 1 WHEN 'acs5' THEN 2 ELSE 9 END,
        d.vintage_year DESC;

    ANALYZE gold.mv_acs_latest_dashboard__staging;

    LOCK TABLE gold.mv_acs_latest_dashboard IN ACCESS EXCLUSIVE MODE;

    ALTER TABLE gold.mv_acs_latest_dashboard
        RENAME TO mv_acs_latest_dashboard__old;

    ALTER TABLE gold.mv_acs_latest_dashboard__staging
        RENAME TO mv_acs_latest_dashboard;

    DROP TABLE gold.mv_acs_latest_dashboard__old;
END;
$$;

DROP PROCEDURE IF EXISTS gold.refresh_mv_bls_latest_dashboard();
CREATE OR REPLACE PROCEDURE gold.refresh_mv_bls_latest_dashboard()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS gold.mv_bls_latest_dashboard__staging;
    DROP TABLE IF EXISTS gold.mv_bls_latest_dashboard__old;

    CREATE TABLE gold.mv_bls_latest_dashboard__staging
        (LIKE gold.mv_bls_latest_dashboard INCLUDING ALL);

    INSERT INTO gold.mv_bls_latest_dashboard__staging (
        source_code,
        observation_date,
        duration_start,
        duration_end,
        time_sk,
        geo_id,
        geo_level,
        state_fips,
        county_fips,
        state_name,
        county_name,
        geo_latitude,
        geo_longitude,
        geo_geom,
        geo_polygon_geojson,
        as_of_date,
        updated_at,
        program_code,
        survey_name,
        series_id,
        series_title,
        gold_metric_name,
        measure_name,
        measure_category,
        value_type,
        unit_of_measure,
        seasonal_adjustment_status,
        observation_basis,
        value,
        metric_code,
        metric_display_name,
        dashboard_suitability,
        business_definition,
        metric_caveats,
        comparison_warning,
        comparability_group,
        recommended_aggregation,
        owner_team
    )
    SELECT DISTINCT ON (d.geo_id, d.series_id, d.metric_code)
        d.source_code,
        d.observation_date,
        d.duration_start,
        d.duration_end,
        d.time_sk,
        d.geo_id,
        d.geo_level,
        d.state_fips,
        d.county_fips,
        d.state_name,
        d.county_name,
        d.geo_latitude,
        d.geo_longitude,
        d.geo_geom,
        d.geo_polygon_geojson,
        d.as_of_date,
        d.updated_at,
        d.program_code,
        d.survey_name,
        d.series_id,
        d.series_title,
        d.gold_metric_name,
        d.measure_name,
        d.measure_category,
        d.value_type,
        d.unit_of_measure,
        d.seasonal_adjustment_status,
        d.observation_basis,
        d.value,
        d.metric_code,
        d.metric_display_name,
        d.dashboard_suitability,
        d.business_definition,
        d.metric_caveats,
        d.comparison_warning,
        d.comparability_group,
        d.recommended_aggregation,
        d.owner_team
    FROM gold.rpt_bls_observation_dashboard d
    ORDER BY
        d.geo_id,
        d.series_id,
        d.metric_code,
        d.observation_date DESC,
        d.updated_at DESC;

    ANALYZE gold.mv_bls_latest_dashboard__staging;

    LOCK TABLE gold.mv_bls_latest_dashboard IN ACCESS EXCLUSIVE MODE;

    ALTER TABLE gold.mv_bls_latest_dashboard
        RENAME TO mv_bls_latest_dashboard__old;

    ALTER TABLE gold.mv_bls_latest_dashboard__staging
        RENAME TO mv_bls_latest_dashboard;

    DROP TABLE gold.mv_bls_latest_dashboard__old;
END;
$$;

DROP PROCEDURE IF EXISTS gold.refresh_mv_fred_latest_dashboard();
CREATE OR REPLACE PROCEDURE gold.refresh_mv_fred_latest_dashboard()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS gold.mv_fred_latest_dashboard__staging;
    DROP TABLE IF EXISTS gold.mv_fred_latest_dashboard__old;

    CREATE TABLE gold.mv_fred_latest_dashboard__staging
        (LIKE gold.mv_fred_latest_dashboard INCLUDING ALL);

    INSERT INTO gold.mv_fred_latest_dashboard__staging (
        source_code,
        observation_date,
        duration_start,
        duration_end,
        time_sk,
        geo_id,
        geo_level,
        state_fips,
        county_fips,
        state_name,
        county_name,
        geo_latitude,
        geo_longitude,
        geo_geom,
        geo_polygon_geojson,
        as_of_date,
        updated_at,
        series_id,
        series_title,
        source_provider,
        original_source_name,
        is_primary_source_series,
        is_republished_series,
        frequency,
        units,
        seasonal_adjustment,
        transformation_method,
        realtime_start,
        realtime_end,
        value,
        metric_code,
        metric_display_name,
        dashboard_suitability,
        business_definition,
        caveats,
        comparability_group,
        do_not_compare_with,
        recommended_aggregation,
        owner_team
    )
    SELECT DISTINCT ON (d.geo_id, d.series_id, d.metric_code)
        d.source_code,
        d.observation_date,
        d.duration_start,
        d.duration_end,
        d.time_sk,
        d.geo_id,
        d.geo_level,
        d.state_fips,
        d.county_fips,
        d.state_name,
        d.county_name,
        d.geo_latitude,
        d.geo_longitude,
        d.geo_geom,
        d.geo_polygon_geojson,
        d.as_of_date,
        d.updated_at,
        d.series_id,
        d.series_title,
        d.source_provider,
        d.original_source_name,
        d.is_primary_source_series,
        d.is_republished_series,
        d.frequency,
        d.units,
        d.seasonal_adjustment,
        d.transformation_method,
        d.realtime_start,
        d.realtime_end,
        d.value,
        d.metric_code,
        d.metric_display_name,
        d.dashboard_suitability,
        d.business_definition,
        d.caveats,
        d.comparability_group,
        d.do_not_compare_with,
        d.recommended_aggregation,
        d.owner_team
    FROM gold.rpt_fred_observation_dashboard d
    ORDER BY
        d.geo_id,
        d.series_id,
        d.metric_code,
        d.observation_date DESC,
        d.realtime_start DESC NULLS LAST,
        d.realtime_end DESC NULLS LAST,
        d.updated_at DESC;

    ANALYZE gold.mv_fred_latest_dashboard__staging;

    LOCK TABLE gold.mv_fred_latest_dashboard IN ACCESS EXCLUSIVE MODE;

    ALTER TABLE gold.mv_fred_latest_dashboard
        RENAME TO mv_fred_latest_dashboard__old;

    ALTER TABLE gold.mv_fred_latest_dashboard__staging
        RENAME TO mv_fred_latest_dashboard;

    DROP TABLE gold.mv_fred_latest_dashboard__old;
END;
$$;

DROP PROCEDURE IF EXISTS gold.refresh_dashboard_serving_layer();
CREATE OR REPLACE PROCEDURE gold.refresh_dashboard_serving_layer()
LANGUAGE plpgsql
AS $$
BEGIN
    SET LOCAL statement_timeout = 0;

    CALL gold.refresh_rpt_acs_observation_dashboard();
    CALL gold.refresh_rpt_bls_observation_dashboard();
    CALL gold.refresh_rpt_fred_observation_dashboard();
    CALL gold.refresh_mv_acs_latest_dashboard();
    CALL gold.refresh_mv_bls_latest_dashboard();
    CALL gold.refresh_mv_fred_latest_dashboard();
END;
$$;

COMMIT;
