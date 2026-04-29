"""
Gold analytics layer — BLS subject transform.

Handles fetching BLS silver data for a given month and upserting
into the shared gold.fact_metrics table.

BLS data is monthly; the latest period_date within each calendar month
is selected per (geo_id, series_id).
"""
from __future__ import annotations

import logging
import pathlib
from datetime import date

from airflow.providers.postgres.hooks.postgres import PostgresHook

from bls.config import CONFIG
from utility.gold_schema import ensure_gold_schema_from_files

logger = logging.getLogger(__name__)

_DDL_PATH = pathlib.Path(__file__).parent / "DDL" / "gold_bls.sql"
_SCHEMA_COMPONENT = "gold_ddl_bls"
_REQUIRED_RELATIONS = (
    "gold.dim_geo",
    "gold.dim_source_system",
    "gold.dim_metric_catalog",
    "gold.dim_geo_latest",
    "gold.dim_bls_survey",
    "gold.dim_bls_series",
    "gold.fact_bls_observation",
    "gold.rpt_bls_observation_dashboard",
    "gold.mv_bls_latest_dashboard",
)
_REQUIRED_PROCEDURES = (
    "gold.refresh_dim_geo_latest()",
    "gold.refresh_rpt_bls_observation_dashboard(date,date)",
    "gold.refresh_mv_bls_latest_dashboard(date,date)",
    "gold.refresh_dashboard_serving_layer_bls(date,date)",
)


def _get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def ensure_bls_gold_schema(hook: PostgresHook | None = None) -> None:
    if hook is None:
        hook = _get_hook()

    ensure_gold_schema_from_files(
        ddl_files=[_DDL_PATH],
        component_name=_SCHEMA_COMPONENT,
        required_relations=_REQUIRED_RELATIONS,
        required_procedures=_REQUIRED_PROCEDURES,
        hook=hook,
    )


def _seed_bls_metric_catalog(cur) -> int:
    """Populate dim_metric_catalog and bridge_metric_bls_series from dim_bls_series."""
    cur.execute(
        """
        INSERT INTO gold.dim_metric_catalog (
            metric_code,
            metric_display_name,
            source_code,
            source_object_type,
            business_definition,
            caveats,
            valid_geo_grains,
            valid_time_grains,
            dashboard_suitability,
            comparability_group,
            do_not_compare_with,
            recommended_aggregation,
            owner_team,
            is_active
        )
        SELECT
            'BLS:' || s.series_id AS metric_code,
            COALESCE(NULLIF(s.gold_metric_name, ''), NULLIF(s.series_title, ''), s.series_id) AS metric_display_name,
            'BLS' AS source_code,
            'BLS_SERIES' AS source_object_type,
            COALESCE(
                NULLIF(s.semantic_notes, ''),
                'BLS curated metric sourced from ' || sv.survey_name || '.'
            ) AS business_definition,
            sv.comparison_warning AS caveats,
            CASE
                WHEN s.geographic_level = 'COUNTY' THEN ARRAY['COUNTY']::TEXT[]
                WHEN s.geographic_level = 'STATE' THEN ARRAY['STATE']::TEXT[]
                WHEN s.geographic_level IN ('US', 'NATIONAL') THEN ARRAY['NATIONAL']::TEXT[]
                ELSE ARRAY['NATIONAL', 'STATE', 'COUNTY']::TEXT[]
            END AS valid_geo_grains,
            ARRAY['MONTHLY']::TEXT[] AS valid_time_grains,
            CASE
                WHEN s.analytic_role IN ('HEADLINE', 'LABOR_SLACK_CONTEXT', 'LABOR_INCOME_CONTEXT', 'INFLATION_CONTEXT', 'FLOW_CONTEXT')
                    THEN 'PUBLIC_SAFE'
                ELSE 'INTERNAL_ONLY'
            END AS dashboard_suitability,
            sv.program_code || ':' || s.measure_category AS comparability_group,
            CASE
                WHEN sv.program_code = 'LN' THEN ARRAY['BLS:CES0000000001', 'BLS:CES0500000001']::TEXT[]
                WHEN sv.program_code = 'CE' AND s.measure_category = 'EMPLOYMENT' THEN ARRAY['BLS:LNS12000000', 'BLS:LNS14000000']::TEXT[]
                WHEN sv.program_code = 'CU' THEN ARRAY['BLS:LNS14000000', 'BLS:CES0000000001']::TEXT[]
                WHEN sv.program_code = 'JT' THEN ARRAY['BLS:CES0000000001', 'BLS:LNS12000000']::TEXT[]
                ELSE ARRAY[]::TEXT[]
            END AS do_not_compare_with,
            'LAST' AS recommended_aggregation,
            'data-eng' AS owner_team,
            TRUE AS is_active
        FROM gold.dim_bls_series s
        JOIN gold.dim_bls_survey sv
          ON sv.bls_survey_sk = s.bls_survey_sk
        ON CONFLICT (metric_code)
        DO UPDATE SET
            metric_display_name = EXCLUDED.metric_display_name,
            business_definition = EXCLUDED.business_definition,
            caveats = EXCLUDED.caveats,
            valid_geo_grains = EXCLUDED.valid_geo_grains,
            valid_time_grains = EXCLUDED.valid_time_grains,
            dashboard_suitability = EXCLUDED.dashboard_suitability,
            comparability_group = EXCLUDED.comparability_group,
            do_not_compare_with = EXCLUDED.do_not_compare_with,
            recommended_aggregation = EXCLUDED.recommended_aggregation,
            owner_team = EXCLUDED.owner_team,
            is_active = EXCLUDED.is_active,
            updated_at = NOW();
        """
    )

    cur.execute(
        """
        INSERT INTO gold.bridge_metric_bls_series (metric_catalog_sk, bls_series_sk)
        SELECT c.metric_catalog_sk, s.bls_series_sk
        FROM gold.dim_metric_catalog c
        JOIN gold.dim_bls_series s
          ON c.metric_code = 'BLS:' || s.series_id
        ON CONFLICT (metric_catalog_sk, bls_series_sk) DO NOTHING;
        """
    )

    cur.execute(
        """
        SELECT COUNT(*)
        FROM gold.dim_metric_catalog
        WHERE source_code = 'BLS'
        """
    )
    return cur.fetchone()[0]


def _fetch_bls_for_month(hook: PostgresHook, month_start: date) -> list[tuple]:
    """Return BLS observation rows from silver for the given month_start.

    Selects the latest period_date per (geo_id, series_id) within the month.
    """
    sql = """
        WITH ranked AS (
            SELECT
                f.geo_id,
                f.series_id,
                f.program,
                f.value,
                f.period_date,
                f.duration_start,
                f.duration_end,
                f.period,
                f.measure_code,
                f.measure_name,
                f.seasonal_adjustment,
                f.geo_level,
                COALESCE(NULLIF(bs.title, ''), NULLIF(f.measure_name, ''), f.series_id) AS series_title,
                ROW_NUMBER() OVER (
                    PARTITION BY f.geo_id, f.series_id
                    ORDER BY f.period_date DESC
                )                                         AS rn
            FROM silver_bls.fact_labor_statistics f
            LEFT JOIN raw_bls.bls_series bs
                ON f.series_id = bs.series_id
               AND f.program = bs.program
            WHERE date_trunc('month', f.period_date)::date = %s
              AND f.value IS NOT NULL
              AND f.series_id IS NOT NULL
              AND f.series_id != ''
        )
        SELECT
            geo_id,
            series_id,
            program,
            value,
            period_date,
            duration_start,
            duration_end,
            period,
            measure_code,
            measure_name,
            seasonal_adjustment,
            geo_level,
            series_title
        FROM ranked
        WHERE rn = 1
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (month_start,))
        rows = cur.fetchall()
    logger.info("BLS fetch for %s: %d rows", month_start, len(rows))
    return rows


def refresh_bls_elements(hook: PostgresHook | None = None) -> int:
    """Sync BLS source-specific metadata into gold.dim_bls_survey and gold.dim_bls_series."""
    if hook is None:
        hook = _get_hook()

    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gold.dim_bls_survey (
                program_code, survey_name, survey_universe, observation_basis,
                primary_concept, id_construction_type, comparison_warning, reference_url
            )
            VALUES
                ('LA', 'Local Area Unemployment Statistics', 'Residence-based civilian labor force', 'PEOPLE',
                 'Local labor market conditions', 'Program+Area+Measure',
                 'Do not compare directly with CES payroll jobs; LAUS is residence-based people counts/rates.',
                 'https://www.bls.gov/lau/'),
                ('LN', 'Current Population Survey', 'Civilian noninstitutional population', 'PEOPLE',
                 'Household labor force status', 'Fixed series coding',
                 'CPS household estimates are not equivalent to CES establishment employment.',
                 'https://www.bls.gov/cps/'),
                ('CE', 'Current Employment Statistics', 'Nonfarm establishments', 'JOBS',
                 'Payroll employment, earnings, and hours', 'Fixed series coding',
                 'CES measures jobs at establishments, not employed persons.',
                 'https://www.bls.gov/ces/'),
                ('CU', 'Consumer Price Index', 'Urban consumers', 'PRICES',
                 'Consumer price inflation', 'Fixed series coding',
                 'CPI index values are not directly comparable to level/count labor statistics.',
                 'https://www.bls.gov/cpi/'),
                ('JT', 'Job Openings and Labor Turnover Survey', 'Nonfarm establishments', 'FLOWS',
                 'Labor market flows (openings, hires, quits, layoffs, separations)', 'Fixed series coding',
                 'JOLTS flow measures are not equivalent to stock employment levels.',
                 'https://www.bls.gov/jlt/')
            ON CONFLICT (program_code)
            DO UPDATE SET
                survey_name = EXCLUDED.survey_name,
                survey_universe = EXCLUDED.survey_universe,
                observation_basis = EXCLUDED.observation_basis,
                primary_concept = EXCLUDED.primary_concept,
                id_construction_type = EXCLUDED.id_construction_type,
                comparison_warning = EXCLUDED.comparison_warning,
                reference_url = EXCLUDED.reference_url,
                updated_at = NOW();
            """
        )

        cur.execute(
            """
            INSERT INTO gold.dim_bls_series (
                bls_survey_sk,
                program_code,
                series_id,
                series_title,
                measure_name,
                measure_category,
                unit_of_measure,
                value_type,
                seasonal_adjustment_status,
                geographic_level,
                gold_metric_name,
                analytic_role,
                semantic_notes
            )
            SELECT DISTINCT ON (f.series_id)
                s.bls_survey_sk,
                UPPER(f.program) AS program_code,
                f.series_id,
                COALESCE(NULLIF(bs.title, ''), NULLIF(f.measure_name, ''), f.series_id) AS series_title,
                f.measure_name,
                CASE
                    WHEN f.series_id = 'LNS14000000' THEN 'UNEMPLOYMENT'
                    WHEN f.series_id = 'LNS13000000' THEN 'UNEMPLOYMENT'
                    WHEN f.series_id = 'LNS12000000' THEN 'EMPLOYMENT'
                    WHEN f.series_id = 'LNS11000000' THEN 'LABOR_FORCE'
                    WHEN f.series_id = 'LNS11300000' THEN 'PARTICIPATION'
                    WHEN f.series_id = 'LNS12300000' THEN 'PARTICIPATION'
                    WHEN f.series_id = 'LNS15000000' THEN 'POPULATION'
                    WHEN f.series_id = 'LNS13327709' THEN 'UNEMPLOYMENT'
                    WHEN f.series_id = 'LNS13025703' THEN 'UNEMPLOYMENT'
                    WHEN f.series_id = 'CES0000000001' THEN 'EMPLOYMENT'
                    WHEN f.series_id = 'CES0500000001' THEN 'EMPLOYMENT'
                    WHEN f.series_id = 'CES0500000002' THEN 'HOURS'
                    WHEN f.series_id IN ('CES0500000003', 'CES0500000008') THEN 'EARNINGS'
                    WHEN f.series_id IN ('CUUR0000SA0', 'CUUR0000SA0L1E', 'CWUR0000SA0') THEN 'PRICE_INDEX'
                    WHEN f.series_id = 'JTS000000000000000JOL' THEN 'OPENINGS'
                    WHEN f.series_id = 'JTS000000000000000HIR' THEN 'HIRES'
                    WHEN f.series_id = 'JTS000000000000000QUR' THEN 'QUITS'
                    WHEN f.series_id = 'JTS000000000000000LDL' THEN 'LAYOFFS'
                    WHEN f.series_id = 'JTS000000000000000TSL' THEN 'SEPARATIONS'
                    WHEN f.series_id = 'JTS000000000000000OSL' THEN 'SEPARATIONS'
                    WHEN f.program IN ('la', 'ln', 'ce') AND LOWER(COALESCE(bs.title, '')) LIKE '%unemploy%' THEN 'UNEMPLOYMENT'
                    WHEN f.program IN ('la', 'ln', 'ce') AND LOWER(COALESCE(bs.title, '')) LIKE '%labor force%' THEN 'LABOR_FORCE'
                    WHEN f.program IN ('la', 'ln', 'ce') AND LOWER(COALESCE(bs.title, '')) LIKE '%participation%' THEN 'PARTICIPATION'
                    WHEN f.program IN ('la', 'ln', 'ce') AND LOWER(COALESCE(bs.title, '')) LIKE '%population%' THEN 'POPULATION'
                    WHEN f.program = 'ce' AND LOWER(COALESCE(bs.title, '')) LIKE '%hour%' THEN 'HOURS'
                    WHEN f.program = 'ce' AND LOWER(COALESCE(bs.title, '')) LIKE '%earn%' THEN 'EARNINGS'
                    WHEN f.program = 'cu' THEN 'PRICE_INDEX'
                    WHEN f.program = 'jt' AND LOWER(COALESCE(bs.title, '')) LIKE '%openings%' THEN 'OPENINGS'
                    WHEN f.program = 'jt' AND LOWER(COALESCE(bs.title, '')) LIKE '%hires%' THEN 'HIRES'
                    WHEN f.program = 'jt' AND LOWER(COALESCE(bs.title, '')) LIKE '%quits%' THEN 'QUITS'
                    WHEN f.program = 'jt' AND LOWER(COALESCE(bs.title, '')) LIKE '%layoff%' THEN 'LAYOFFS'
                    WHEN f.program = 'jt' AND LOWER(COALESCE(bs.title, '')) LIKE '%separation%' THEN 'SEPARATIONS'
                    WHEN f.program IN ('la', 'ln', 'ce') THEN 'EMPLOYMENT'
                    ELSE 'OTHER'
                END AS measure_category,
                CASE
                    WHEN f.program = 'la' AND f.measure_code IN ('03','07','08') THEN 'Percent'
                    WHEN f.program = 'la' AND f.measure_code IN ('04','05','06','09') THEN 'Persons'
                    WHEN f.series_id IN ('LNS14000000', 'LNS11300000', 'LNS12300000', 'LNS13327709') THEN 'Percent'
                    WHEN f.series_id IN ('LNS13000000', 'LNS12000000', 'LNS11000000', 'LNS15000000', 'LNS13025703') THEN 'Thousands of Persons'
                    WHEN f.program = 'cu' THEN 'Index 1982-1984=100'
                    WHEN f.series_id IN ('CES0000000001', 'CES0500000001') THEN 'Thousands of Persons'
                    WHEN f.series_id = 'CES0500000002' THEN 'Hours'
                    WHEN f.series_id = 'CES0500000003' THEN 'Dollars per Hour'
                    WHEN f.series_id = 'CES0500000008' THEN 'Dollars per Week'
                    WHEN f.program = 'jt' THEN 'Level in Thousands'
                    WHEN f.program = 'ln' AND LOWER(COALESCE(bs.title, '')) LIKE '%rate%' THEN 'Percent'
                    WHEN f.program = 'ln' AND LOWER(COALESCE(bs.title, '')) LIKE '%level%' THEN 'Thousands of Persons'
                    ELSE NULL
                END AS unit_of_measure,
                CASE
                    WHEN f.program = 'cu' THEN 'INDEX'
                    WHEN f.series_id IN ('LNS14000000', 'LNS11300000', 'LNS13327709') THEN 'RATE'
                    WHEN f.series_id = 'LNS12300000' THEN 'RATIO'
                    WHEN f.series_id IN ('CES0500000003', 'CES0500000008') THEN 'CURRENCY'
                    WHEN LOWER(COALESCE(bs.title, '')) LIKE '%rate%' THEN 'RATE'
                    WHEN LOWER(COALESCE(bs.title, '')) LIKE '%percent%' THEN 'PERCENT'
                    ELSE 'LEVEL'
                END AS value_type,
                f.seasonal_adjustment,
                UPPER(COALESCE(f.geo_level, 'US')),
                CASE
                    WHEN f.series_id = 'LNS14000000' THEN 'National Unemployment Rate'
                    WHEN f.series_id = 'LNS13000000' THEN 'National Unemployment Level'
                    WHEN f.series_id = 'LNS12000000' THEN 'National Employment Level'
                    WHEN f.series_id = 'LNS11000000' THEN 'National Labor Force Level'
                    WHEN f.series_id = 'LNS11300000' THEN 'National Labor Force Participation Rate'
                    WHEN f.series_id = 'LNS12300000' THEN 'National Employment Population Ratio'
                    WHEN f.series_id = 'LNS15000000' THEN 'National Not In Labor Force Level'
                    WHEN f.series_id = 'LNS13327709' THEN 'National U6 Underutilization Rate'
                    WHEN f.series_id = 'LNS13025703' THEN 'National Long Term Unemployment Level'
                    WHEN f.series_id = 'CES0000000001' THEN 'Total Nonfarm Payroll Employment'
                    WHEN f.series_id = 'CES0500000001' THEN 'Total Private Employment'
                    WHEN f.series_id = 'CES0500000002' THEN 'Average Weekly Hours Total Private'
                    WHEN f.series_id = 'CES0500000003' THEN 'Average Hourly Earnings Total Private'
                    WHEN f.series_id = 'CES0500000008' THEN 'Average Weekly Earnings Total Private'
                    WHEN f.series_id = 'CUUR0000SA0' THEN 'CPI U All Items'
                    WHEN f.series_id = 'CUUR0000SA0L1E' THEN 'Core CPI U All Items Less Food And Energy'
                    WHEN f.series_id = 'CWUR0000SA0' THEN 'CPI W All Items'
                    WHEN f.series_id = 'JTS000000000000000JOL' THEN 'JOLTS Job Openings'
                    WHEN f.series_id = 'JTS000000000000000HIR' THEN 'JOLTS Hires'
                    WHEN f.series_id = 'JTS000000000000000QUR' THEN 'JOLTS Quits'
                    WHEN f.series_id = 'JTS000000000000000LDL' THEN 'JOLTS Layoffs And Discharges'
                    WHEN f.series_id = 'JTS000000000000000TSL' THEN 'JOLTS Total Separations'
                    WHEN f.series_id = 'JTS000000000000000OSL' THEN 'JOLTS Other Separations'
                    ELSE NULL::TEXT
                END AS gold_metric_name,
                CASE
                    WHEN f.series_id IN ('LNS14000000', 'CES0000000001', 'CUUR0000SA0', 'JTS000000000000000JOL') THEN 'HEADLINE'
                    WHEN f.series_id IN ('LNS11300000', 'LNS12300000', 'LNS15000000', 'LNS13327709', 'LNS13025703') THEN 'LABOR_SLACK_CONTEXT'
                    WHEN f.series_id IN ('CES0500000002', 'CES0500000003', 'CES0500000008') THEN 'LABOR_INCOME_CONTEXT'
                    WHEN f.series_id IN ('CUUR0000SA0L1E', 'CWUR0000SA0') THEN 'INFLATION_CONTEXT'
                    WHEN f.series_id IN ('JTS000000000000000HIR', 'JTS000000000000000QUR', 'JTS000000000000000LDL', 'JTS000000000000000TSL', 'JTS000000000000000OSL') THEN 'FLOW_CONTEXT'
                    ELSE 'SUPPORTING'
                END AS analytic_role,
                CASE
                    WHEN f.series_id IN ('LNS12000000', 'LNS13000000', 'LNS14000000', 'LNS11000000', 'LNS11300000', 'LNS12300000', 'LNS15000000', 'LNS13327709', 'LNS13025703')
                        THEN 'CPS household survey series. These are people-based labor-force measures and should not be compared directly to CES payroll jobs.'
                    WHEN f.series_id IN ('CES0000000001', 'CES0500000001', 'CES0500000002', 'CES0500000003', 'CES0500000008')
                        THEN 'CES establishment/payroll survey series. These measure jobs, hours, and earnings at employers, not employed persons.'
                    WHEN f.series_id IN ('CUUR0000SA0', 'CUUR0000SA0L1E', 'CWUR0000SA0')
                        THEN 'CPI price index series. These are inflation context measures and are not directly comparable to labor levels or rates.'
                    WHEN f.series_id IN ('JTS000000000000000JOL', 'JTS000000000000000HIR', 'JTS000000000000000QUR', 'JTS000000000000000LDL', 'JTS000000000000000TSL', 'JTS000000000000000OSL')
                        THEN 'JOLTS labor-flow series. These represent openings or turnover flows rather than employment stock measures.'
                    ELSE 'Preserve survey-specific interpretation; avoid cross-survey equivalence by label similarity.'
                END AS semantic_notes
            FROM silver_bls.fact_labor_statistics f
            LEFT JOIN raw_bls.bls_series bs
                ON bs.series_id = f.series_id
               AND bs.program = f.program
            JOIN gold.dim_bls_survey s
              ON s.program_code = UPPER(f.program)
            WHERE f.series_id IS NOT NULL
              AND f.series_id <> ''
            ORDER BY f.series_id, f.period_date DESC
            ON CONFLICT (series_id)
            DO UPDATE SET
                bls_survey_sk = EXCLUDED.bls_survey_sk,
                program_code = EXCLUDED.program_code,
                series_title = EXCLUDED.series_title,
                measure_name = EXCLUDED.measure_name,
                measure_category = EXCLUDED.measure_category,
                unit_of_measure = EXCLUDED.unit_of_measure,
                value_type = EXCLUDED.value_type,
                seasonal_adjustment_status = EXCLUDED.seasonal_adjustment_status,
                geographic_level = EXCLUDED.geographic_level,
                gold_metric_name = EXCLUDED.gold_metric_name,
                analytic_role = EXCLUDED.analytic_role,
                semantic_notes = EXCLUDED.semantic_notes,
                updated_at = NOW();
            """
        )

        cur.execute("SELECT COUNT(*) FROM gold.dim_bls_series")
        row_count = cur.fetchone()[0]
        catalog_count = _seed_bls_metric_catalog(cur)
        conn.commit()

    logger.info(
        "refresh_bls_elements: dim_bls_series row_count=%d, bls_metric_catalog_count=%d",
        row_count,
        catalog_count,
    )
    return row_count


def _upsert_bls_rows(hook: PostgresHook, rows: list[tuple]) -> int:
    """Upsert BLS observation rows into gold.fact_bls_observation."""
    if not rows:
        return 0

    sql = """
        INSERT INTO gold.fact_bls_observation (
            geo_id, geo_level, state_id, state_name, county_id, county_name,
            geo_latitude, geo_longitude, geo_geom,
            time_sk, period_date, duration_start, duration_end,
            bls_survey_sk, bls_series_sk, program_code,
            value, period_code, seasonal_adjustment_status,
            observation_basis, measure_category, value_type,
            as_of_date
        )
        SELECT
            r.geo_id,
            CASE
                WHEN d.geo_level = 'us' THEN 'NATIONAL'
                WHEN d.geo_level = 'state' THEN 'STATE'
                WHEN d.geo_level = 'county' THEN 'COUNTY'
                WHEN r.geo_id = 'us:1' THEN 'NATIONAL'
                WHEN r.geo_id LIKE 'state:%%|county:%%' THEN 'COUNTY'
                WHEN r.geo_id LIKE 'state:%%' THEN 'STATE'
                ELSE 'NATIONAL'
            END AS geo_level,
            CASE WHEN d.state_fips IS NOT NULL THEN LPAD(d.state_fips::TEXT, 2, '0') ELSE NULL END AS state_id,
            d.state_name,
            CASE
                WHEN d.state_fips IS NOT NULL AND d.county_fips IS NOT NULL
                THEN CONCAT(LPAD(d.state_fips::TEXT, 2, '0'), LPAD(d.county_fips::TEXT, 3, '0'))
                ELSE NULL
            END AS county_id,
            d.county_name,
            d.latitude,
            d.longitude,
            d.geom,
            t.time_sk,
            r.period_date,
            r.duration_start,
            r.duration_end,
            sv.bls_survey_sk,
            sr.bls_series_sk,
            UPPER(r.program) AS program_code,
            r.value,
            r.period,
            r.seasonal_adjustment,
            sv.observation_basis,
            sr.measure_category,
            sr.value_type,
            CURRENT_DATE
        FROM (
            VALUES %s
        ) AS r(
            geo_id,
            series_id,
            program,
            value,
            period_date,
            duration_start,
            duration_end,
            period,
            measure_code,
            measure_name,
            seasonal_adjustment,
            geo_level,
            series_title
        )
        JOIN gold.dim_bls_series sr
          ON sr.series_id = r.series_id
        JOIN gold.dim_bls_survey sv
          ON sv.bls_survey_sk = sr.bls_survey_sk
        LEFT JOIN silver_ref.dim_geo d
          ON d.geo_id = r.geo_id
        LEFT JOIN silver_ref.dim_time t
          ON t.date_key = r.period_date
        ON CONFLICT (geo_id, period_date, bls_series_sk)
        DO UPDATE SET
            geo_level = EXCLUDED.geo_level,
            state_id = EXCLUDED.state_id,
            state_name = EXCLUDED.state_name,
            county_id = EXCLUDED.county_id,
            county_name = EXCLUDED.county_name,
            geo_latitude = EXCLUDED.geo_latitude,
            geo_longitude = EXCLUDED.geo_longitude,
            geo_geom = EXCLUDED.geo_geom,
            time_sk = EXCLUDED.time_sk,
            duration_start = EXCLUDED.duration_start,
            duration_end = EXCLUDED.duration_end,
            bls_survey_sk = EXCLUDED.bls_survey_sk,
            program_code = EXCLUDED.program_code,
            value = EXCLUDED.value,
            period_code = EXCLUDED.period_code,
            seasonal_adjustment_status = EXCLUDED.seasonal_adjustment_status,
            observation_basis = EXCLUDED.observation_basis,
            measure_category = EXCLUDED.measure_category,
            value_type = EXCLUDED.value_type,
            as_of_date = EXCLUDED.as_of_date,
            updated_at = NOW()
    """

    from psycopg2.extras import execute_values

    with hook.get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows)
        row_count = cur.rowcount
        conn.commit()

    return row_count


def merge_bls_shard(shard: dict, hook: PostgresHook | None = None) -> dict:
    """Process one month shard for BLS: fetch and upsert to gold.fact_bls_observation.

    Args:
        shard: dict with key "month_start" (ISO date string).
        hook:  optional PostgresHook; created from CONFIG if not provided.

    Returns:
        dict with keys: month_start, input_rows, output_rows, source_system,
                        sample_observation_dates.
    """
    if hook is None:
        hook = _get_hook()

    month_start = date.fromisoformat(shard["month_start"])
    logger.info("[BLS GOLD] Processing shard %s", month_start)

    rows = _fetch_bls_for_month(hook, month_start)
    output_rows = _upsert_bls_rows(hook, rows)

    sample_observation_dates: list[str] = []
    for r in rows[:5]:
        obs = r[4]
        if obs is not None:
            sample_observation_dates.append(
                obs.isoformat() if hasattr(obs, "isoformat") else str(obs)
            )

    logger.info(
        "[BLS GOLD] Shard %s: input=%d output=%d",
        month_start, len(rows), output_rows,
    )
    return {
        "month_start": month_start.isoformat(),
        "input_rows": len(rows),
        "output_rows": output_rows,
        "source_system": "BLS",
        "sample_observation_dates": sample_observation_dates,
    }
