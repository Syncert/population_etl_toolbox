"""The serving-refresh chunk configuration for each union-served source.

These declarations used to sit inline in the three ingestion DAGs. The
operator-triggered full re-serve (``serving_full_reserve``) drives the same
sources through the same chunked driver, and a second copy of a relation name,
a procedure name, or a chunk plan is exactly the kind of thing that drifts
silently and re-serves the wrong years. One definition, two callers.
"""

from __future__ import annotations

from data_ingestion_toolbox.utility.gold_schema import (
    GrainStep,
    ServingRefreshChunkConfig,
)


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
    # A chunk is one calendar year, and ACS years are not alike: 2024 carries
    # the place and tract grains, about 86 million facts against 4 to 8
    # million in an earlier year, and its report refresh alone runs for hours
    # on the internal stack. A 90-minute limit failed that year every time.
    statement_timeout="600min",
    full_statement_timeout="600min",
    # The year is served one grain group at a time (backfill plan,
    # Deliverable 5), so a place year never holds that year's county, state
    # and nation rows. Limits per step: the area grains were served inside 90
    # minutes a year before places and tracts existed; a tract year (five
    # tables, about 11M revision rows) inside the 120 minutes the year chunk
    # had before 2024; a full-list place year (2024, about 159M revision rows)
    # needed hours, and keeps the 600 minutes. Each step's duration is logged
    # (`step=... duration_seconds=`), and the backfill records them per year.
    # A grain step is skipped when the transform's per-slice ledger says no
    # slice of that year and grain changed a row since the chunk was last
    # served. Slice keys are `dataset/geo_level[/state]`; the step names the
    # served levels. A grain-year with no ledger row, or a ledger row older
    # than the column recording changes, counts as changed.
    #
    # A small year is transformed as whole-year slices (`*`, or `dataset/*`)
    # that name no level, so the ledger cannot say which grain changed. When
    # one changed since the last serve, the step asks silver itself: did a
    # fact at its levels change after the watermark the last complete serve
    # published through (`served_through`)? Without that, every grain of a
    # small year was re-served whenever any grain changed: a 2000 tract load
    # re-served the year's area and place grains, and the next area load would
    # re-serve 1.4 million unchanged tract rows (25 minutes).
    step_changed_sql="""
        WITH grains AS (
            SELECT CASE level
                       WHEN 'NATIONAL' THEN 'us'
                       ELSE LOWER(level)
                   END AS slice_level
            FROM UNNEST(%(geo_levels)s::TEXT[]) AS level
        ), ledger AS (
            SELECT s.last_changed_at
            FROM silver_census.transform_slice_state AS s
            JOIN grains AS g ON g.slice_level = split_part(s.slice_key, '/', 2)
            WHERE s.year = %(year)s
        ), whole_year AS (
            SELECT s.last_changed_at
            FROM silver_census.transform_slice_state AS s
            WHERE s.year = %(year)s
              AND (s.slice_key = '*' OR split_part(s.slice_key, '/', 2) = '*')
        )
        SELECT (NOT EXISTS (SELECT 1 FROM ledger)
                AND NOT EXISTS (SELECT 1 FROM whole_year))
            OR EXISTS (SELECT 1 FROM ledger WHERE last_changed_at > %(since)s)
            OR (
                EXISTS (SELECT 1 FROM whole_year WHERE last_changed_at > %(since)s)
                AND (
                    %(served_through)s::TIMESTAMPTZ IS NULL
                    OR EXISTS (
                        SELECT 1
                        FROM silver_census.fact_demographics AS fact
                        WHERE fact.ingested_at > %(served_through)s::TIMESTAMPTZ
                          AND fact.estimate_year = %(year)s
                          AND fact.geo_level IN (SELECT slice_level FROM grains)
                    )
                )
            )
    """,
    grain_steps=(
        GrainStep("area", ("NATIONAL", "STATE", "COUNTY", "METRO"), "90min"),
        GrainStep("tract", ("TRACT",), "120min"),
        GrainStep("place", ("PLACE",), "600min"),
    ),
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
