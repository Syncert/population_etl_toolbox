"""The serving-refresh chunk configuration for each union-served source.

These declarations used to sit inline in the three ingestion DAGs. The
operator-triggered full re-serve (``serving_full_reserve``) drives the same
sources through the same chunked driver, and a second copy of a relation name,
a procedure name, or a chunk plan is exactly the kind of thing that drifts
silently and re-serves the wrong years. One definition, two callers.
"""

from __future__ import annotations

from data_ingestion_toolbox.utility.gold_schema import ServingRefreshChunkConfig


BLS_CHUNK_CONFIG = ServingRefreshChunkConfig(
    source_code="BLS",
    log_label="BLS",
    report_table="gold_bls.rpt_bls_observations",
    report_date_column="observation_date",
    changed_chunks_sql="""
        SELECT
            MAKE_DATE(s.year, 1, 1) AS chunk_start,
            MAKE_DATE(s.year, 12, 31) AS chunk_end,
            MAX(s.ingested_at) AS target_watermark
        FROM silver_bls.fact_labor_statistics s
        WHERE s.value IS NOT NULL
          AND s.ingested_at > %s
        GROUP BY s.year
        ORDER BY s.year
    """,
    all_chunks_sql="""
        -- One pass over silver for every year's watermark. A correlated
        -- subquery per year reads the whole fact table once per year, which on
        -- ACS meant twenty scans of tens of millions of rows and a planning
        -- step that had not returned after ten minutes.
        WITH silver_years AS (
            SELECT s.year AS observation_year,
                   MAX(s.ingested_at) AS target_watermark
            FROM silver_bls.fact_labor_statistics s
            WHERE s.value IS NOT NULL
            GROUP BY s.year
        ),
        bounds AS (
            SELECT
                LEAST(
                    (SELECT MIN(observation_year) FROM silver_years),
                    (SELECT EXTRACT(YEAR FROM MIN(r.observation_date))::INT FROM gold_bls.rpt_bls_observations r)
                ) AS first_year,
                GREATEST(
                    (SELECT MAX(observation_year) FROM silver_years),
                    (SELECT EXTRACT(YEAR FROM MAX(r.observation_date))::INT FROM gold_bls.rpt_bls_observations r)
                ) AS last_year
        )
        SELECT
            MAKE_DATE(y::INT, 1, 1) AS chunk_start,
            MAKE_DATE(y::INT, 12, 31) AS chunk_end,
            COALESCE(silver_years.target_watermark, TIMESTAMPTZ 'epoch') AS target_watermark
        FROM bounds
        CROSS JOIN LATERAL generate_series(bounds.first_year, bounds.last_year) AS y
        LEFT JOIN silver_years ON silver_years.observation_year = y::INT
        WHERE bounds.first_year IS NOT NULL
        ORDER BY y
    """,
    report_procedure="gold_bls.refresh_rpt_bls_observations",
    latest_procedure="gold_bls.refresh_mv_bls_latest",
    latest_table="gold_bls.mv_bls_latest",
    statement_timeout="60min",
    full_statement_timeout="90min",
)

ACS_CHUNK_CONFIG = ServingRefreshChunkConfig(
    source_code="CENSUS_ACS",
    log_label="ACS",
    report_table="gold_census.rpt_acs_observations",
    report_date_column="observation_date",
    changed_chunks_sql="""
        SELECT
            MAKE_DATE(s.estimate_year, 1, 1) AS chunk_start,
            MAKE_DATE(s.estimate_year, 12, 31) AS chunk_end,
            MAX(s.ingested_at) AS target_watermark
        FROM silver_census.fact_demographics s
        WHERE s.estimate_value IS NOT NULL
          AND s.ingested_at > %s
        GROUP BY s.estimate_year
        ORDER BY s.estimate_year
    """,
    all_chunks_sql="""
        -- One pass over silver for every year's watermark. A correlated
        -- subquery per year reads the whole fact table once per year, which on
        -- ACS meant twenty scans of tens of millions of rows and a planning
        -- step that had not returned after ten minutes.
        WITH silver_years AS (
            SELECT s.estimate_year AS observation_year,
                   MAX(s.ingested_at) AS target_watermark
            FROM silver_census.fact_demographics s
            WHERE s.estimate_value IS NOT NULL
            GROUP BY s.estimate_year
        ),
        bounds AS (
            SELECT
                LEAST(
                    (SELECT MIN(observation_year) FROM silver_years),
                    (SELECT EXTRACT(YEAR FROM MIN(r.observation_date))::INT FROM gold_census.rpt_acs_observations r)
                ) AS first_year,
                GREATEST(
                    (SELECT MAX(observation_year) FROM silver_years),
                    (SELECT EXTRACT(YEAR FROM MAX(r.observation_date))::INT FROM gold_census.rpt_acs_observations r)
                ) AS last_year
        )
        SELECT
            MAKE_DATE(y::INT, 1, 1) AS chunk_start,
            MAKE_DATE(y::INT, 12, 31) AS chunk_end,
            COALESCE(silver_years.target_watermark, TIMESTAMPTZ 'epoch') AS target_watermark
        FROM bounds
        CROSS JOIN LATERAL generate_series(bounds.first_year, bounds.last_year) AS y
        LEFT JOIN silver_years ON silver_years.observation_year = y::INT
        WHERE bounds.first_year IS NOT NULL
        ORDER BY y
    """,
    report_procedure="gold_census.refresh_rpt_acs_observations",
    latest_procedure="gold_census.refresh_mv_acs_latest",
    latest_table="gold_census.mv_acs_latest",
    statement_timeout="90min",
    full_statement_timeout="120min",
)

FRED_CHUNK_CONFIG = ServingRefreshChunkConfig(
    source_code="FRED",
    log_label="FRED",
    report_table="gold_fred.rpt_fred_observations",
    report_date_column="observation_date",
    changed_chunks_sql="""
        SELECT
            MAKE_DATE(EXTRACT(YEAR FROM s.observation_date)::INTEGER, 1, 1)
                AS chunk_start,
            MAKE_DATE(EXTRACT(YEAR FROM s.observation_date)::INTEGER, 12, 31)
                AS chunk_end,
            MAX(s.ingested_at) AS target_watermark
        FROM silver_fred.fact_economic_indicators s
        WHERE s.is_missing = FALSE
          AND s.ingested_at > %s
        GROUP BY EXTRACT(YEAR FROM s.observation_date)
        ORDER BY EXTRACT(YEAR FROM s.observation_date)
    """,
    all_chunks_sql="""
        -- One pass over silver for every year's watermark. A correlated
        -- subquery per year reads the whole fact table once per year, which on
        -- ACS meant twenty scans of tens of millions of rows and a planning
        -- step that had not returned after ten minutes.
        WITH silver_years AS (
            SELECT EXTRACT(YEAR FROM s.observation_date)::INT AS observation_year,
                   MAX(s.ingested_at) AS target_watermark
            FROM silver_fred.fact_economic_indicators s
            WHERE s.is_missing = FALSE
            GROUP BY EXTRACT(YEAR FROM s.observation_date)::INT
        ),
        bounds AS (
            SELECT
                LEAST(
                    (SELECT MIN(observation_year) FROM silver_years),
                    (SELECT EXTRACT(YEAR FROM MIN(r.observation_date))::INT FROM gold_fred.rpt_fred_observations r)
                ) AS first_year,
                GREATEST(
                    (SELECT MAX(observation_year) FROM silver_years),
                    (SELECT EXTRACT(YEAR FROM MAX(r.observation_date))::INT FROM gold_fred.rpt_fred_observations r)
                ) AS last_year
        )
        SELECT
            MAKE_DATE(y::INT, 1, 1) AS chunk_start,
            MAKE_DATE(y::INT, 12, 31) AS chunk_end,
            COALESCE(silver_years.target_watermark, TIMESTAMPTZ 'epoch') AS target_watermark
        FROM bounds
        CROSS JOIN LATERAL generate_series(bounds.first_year, bounds.last_year) AS y
        LEFT JOIN silver_years ON silver_years.observation_year = y::INT
        WHERE bounds.first_year IS NOT NULL
        ORDER BY y
    """,
    report_procedure="gold_fred.refresh_rpt_fred_observations",
    latest_procedure="gold_fred.refresh_mv_fred_latest",
    latest_table="gold_fred.mv_fred_latest",
    statement_timeout="30min",
    full_statement_timeout="60min",
)

#: Every source the operator-triggered full re-serve can rewrite, keyed by
#: the source_code an operator names in the DAG run conf.
FULL_RESERVE_CONFIGS = {
    "BLS": BLS_CHUNK_CONFIG,
    "CENSUS_ACS": ACS_CHUNK_CONFIG,
    "FRED": FRED_CHUNK_CONFIG,
}
