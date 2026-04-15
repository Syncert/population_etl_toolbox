"""
Gold analytics layer — shared utilities.

Provides schema management, shard list computation, and the core upsert helper
used by subject-specific gold transforms (census_acs/gold_census, bls/gold_bls,
fred/gold_fred).

Gold fetch tuple shape:
(
    geo_id, element_id, source_system, element_name, value,
    observation_date, observation_end, duration_start, duration_end,
    period_type, acs_dataset, margin_of_error, margin_of_error_pct,
    survey_concept, unit_of_measure, value_semantics,
    seasonal_adjustment, is_seasonally_adjusted, is_saar
)
"""
from __future__ import annotations

import logging
import pathlib
import re
from datetime import date
from typing import Any

import psycopg2.extras
from airflow.providers.postgres.hooks.postgres import PostgresHook

from gold.config import CONFIG

logger = logging.getLogger(__name__)

_DDL_PATH = pathlib.Path(__file__).parent / "DDL" / "gold.sql"

# Indices for tuples returned by _fetch_*_for_month functions.
_F_GEO_ID = 0
_F_ELEMENT_ID = 1
_F_SOURCE_SYSTEM = 2
_F_ELEMENT_NAME = 3
_F_VALUE = 4
_F_OBSERVATION_DATE = 5
_F_OBSERVATION_END = 6
_F_DURATION_START = 7
_F_DURATION_END = 8
_F_PERIOD_TYPE = 9
_F_ACS_DATASET = 10
_F_MARGIN_OF_ERROR = 11
_F_MARGIN_OF_ERROR_PCT = 12
_F_SURVEY_CONCEPT = 13
_F_UNIT_OF_MEASURE = 14
_F_VALUE_SEMANTICS = 15
_F_SEASONAL_ADJUSTMENT = 16
_F_IS_SEASONALLY_ADJUSTED = 17
_F_IS_SAAR = 18


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


_STATE_GEO_RE = re.compile(r"^state:(\d{1,2})$")
_COUNTY_GEO_RE = re.compile(r"^state:(\d{1,2})\|county:(\d{1,3})$")


def _normalize_geo_id(geo_id: str | None) -> str | None:
    """Normalize geo_id to canonical lowercase + zero-padded FIPS format."""
    if geo_id is None:
        return None

    gid = str(geo_id).strip().lower()
    if not gid:
        return None
    if gid == "us:1":
        return gid

    m_state = _STATE_GEO_RE.match(gid)
    if m_state:
        return f"state:{m_state.group(1).zfill(2)}"

    m_county = _COUNTY_GEO_RE.match(gid)
    if m_county:
        return (
            f"state:{m_county.group(1).zfill(2)}"
            f"|county:{m_county.group(2).zfill(3)}"
        )

    return gid


def _derive_geo_level(geo_id: str, dim_geo_level: str | None) -> str:
    """Resolve analyst-facing geography level with stable fallback logic."""
    if dim_geo_level:
        normalized = dim_geo_level.strip().lower()
        if normalized == "us":
            return "NATIONAL"
        if normalized == "state":
            return "STATE"
        if normalized == "county":
            return "COUNTY"

    gid = _normalize_geo_id(geo_id)
    if gid == "us:1":
        return "NATIONAL"
    if gid and "|county:" in gid:
        return "COUNTY"
    if gid and gid.startswith("state:"):
        return "STATE"
    return "NATIONAL"


def _lookup_geo_attributes(
    hook: PostgresHook,
    geo_ids: list[str],
) -> dict[str, tuple[str | None, str | None, str | None, str | None, str | None]]:
    """Return geo_id -> (state_id, state_name, county_id, county_name, dim_geo_level)."""
    if not geo_ids:
        return {}

    requested_geo_ids = sorted({
        candidate
        for gid in geo_ids
        for candidate in {str(gid).strip(), _normalize_geo_id(gid)}
        if candidate
    })
    if not requested_geo_ids:
        return {}

    sql = """
        SELECT DISTINCT ON (geo_id)
            geo_id,
            CASE
                WHEN state_fips IS NOT NULL THEN LPAD(state_fips::TEXT, 2, '0')
                ELSE NULL
            END AS state_id,
            state_name,
            CASE
                WHEN county_fips IS NOT NULL AND state_fips IS NOT NULL
                    THEN CONCAT(LPAD(state_fips::TEXT, 2, '0'), LPAD(county_fips::TEXT, 3, '0'))
                ELSE NULL
            END AS county_id,
            county_name,
            geo_level
        FROM silver_ref.dim_geo
        WHERE geo_id = ANY(%s)
        ORDER BY geo_id, source_year DESC NULLS LAST
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (requested_geo_ids,))
        rows: list[tuple[Any, ...]] = cur.fetchall()

    lookup: dict[str, tuple[str | None, str | None, str | None, str | None, str | None]] = {}
    for r in rows:
        db_geo_id = str(r[0]).strip()
        attrs = (r[1], r[2], r[3], r[4], r[5])
        lookup[db_geo_id] = attrs
        normalized = _normalize_geo_id(db_geo_id)
        if normalized:
            lookup[normalized] = attrs

    return lookup


def _upsert_gold_rows(hook: PostgresHook, rows: list[tuple], month_start: date) -> int:
    """Upsert rows into gold.fact_metrics. Returns count of rows upserted."""
    if not rows:
        return 0

    geo_lookup = _lookup_geo_attributes(
        hook,
        sorted({str(r[_F_GEO_ID]).strip() for r in rows if r[_F_GEO_ID] is not None}),
    )
    year = month_start.year
    quarter = ((month_start.month - 1) // 3) + 1

    sql = """
        INSERT INTO gold.fact_metrics
            (geo_id, geo_level, state_id, state_name, county_id, county_name,
             month_start, year, quarter,
             period_type,
             source_system, element_id, element_name,
             value, observation_date, observation_end,
             duration_start, duration_end,
             acs_dataset, margin_of_error, margin_of_error_pct,
             survey_concept,
             unit_of_measure, value_semantics,
             seasonal_adjustment, is_seasonally_adjusted, is_saar,
             as_of_date)
        VALUES %s
        ON CONFLICT (geo_id, month_start, source_system, element_id)
        DO UPDATE SET
            geo_level           = EXCLUDED.geo_level,
            state_id            = EXCLUDED.state_id,
            state_name          = EXCLUDED.state_name,
            county_id           = EXCLUDED.county_id,
            county_name         = EXCLUDED.county_name,
            year                = EXCLUDED.year,
            quarter             = EXCLUDED.quarter,
            period_type         = EXCLUDED.period_type,
            element_name        = EXCLUDED.element_name,
            value               = EXCLUDED.value,
            observation_date    = EXCLUDED.observation_date,
            observation_end     = EXCLUDED.observation_end,
            duration_start      = EXCLUDED.duration_start,
            duration_end        = EXCLUDED.duration_end,
            acs_dataset         = EXCLUDED.acs_dataset,
            margin_of_error     = EXCLUDED.margin_of_error,
            margin_of_error_pct = EXCLUDED.margin_of_error_pct,
            survey_concept      = EXCLUDED.survey_concept,
            unit_of_measure     = EXCLUDED.unit_of_measure,
            value_semantics     = EXCLUDED.value_semantics,
            seasonal_adjustment = EXCLUDED.seasonal_adjustment,
            is_seasonally_adjusted = EXCLUDED.is_seasonally_adjusted,
            is_saar             = EXCLUDED.is_saar,
            as_of_date          = EXCLUDED.as_of_date,
            updated_at          = NOW()
    """
    insert_rows: list[tuple[Any, ...]] = []
    for r in rows:
        geo_id = str(r[_F_GEO_ID]).strip()
        attrs = (
            geo_lookup.get(_normalize_geo_id(geo_id))
            or geo_lookup.get(geo_id)
            or (None, None, None, None, None)
        )
        geo_level = _derive_geo_level(geo_id, attrs[4] if len(attrs) > 4 else None)

        observation_date = r[_F_OBSERVATION_DATE]
        observation_end = r[_F_OBSERVATION_END] or observation_date

        insert_rows.append(
            (
                geo_id,
                geo_level,
                attrs[0],
                attrs[1],
                attrs[2],
                attrs[3],
                month_start,
                year,
                quarter,
                r[_F_PERIOD_TYPE],
                r[_F_SOURCE_SYSTEM],
                r[_F_ELEMENT_ID],
                r[_F_ELEMENT_NAME],
                r[_F_VALUE],
                observation_date,
                observation_end,
                r[_F_DURATION_START],
                r[_F_DURATION_END],
                r[_F_ACS_DATASET],
                r[_F_MARGIN_OF_ERROR],
                r[_F_MARGIN_OF_ERROR_PCT],
                r[_F_SURVEY_CONCEPT],
                r[_F_UNIT_OF_MEASURE],
                r[_F_VALUE_SEMANTICS],
                r[_F_SEASONAL_ADJUSTMENT],
                r[_F_IS_SEASONALLY_ADJUSTED],
                r[_F_IS_SAAR],
                date.today(),
            )
        )

    with hook.get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, insert_rows)
        conn.commit()

    return len(insert_rows)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ensure_gold_schema(hook: PostgresHook | None = None) -> None:
    """Read and execute gold/DDL/gold.sql to ensure schema and tables exist."""
    if hook is None:
        hook = _get_hook()
    sql = _DDL_PATH.read_text(encoding="utf-8")
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
    logger.info("Gold schema ensured via %s", _DDL_PATH)


def build_shard_list(
    window_start: date,
    window_end: date,
    hook: PostgresHook | None = None,
) -> list[str]:
    """Return ISO month_start strings from silver_ref.dim_time within the window."""
    if hook is None:
        hook = _get_hook()
    sql = """
        SELECT DISTINCT date_trunc('month', date_key)::date AS month_start
        FROM silver_ref.dim_time
        WHERE date_key >= %s
          AND date_key <= %s
          AND is_month_start = TRUE
        ORDER BY month_start
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (window_start, window_end))
        rows = cur.fetchall()
    shards = [r[0].isoformat() for r in rows]
    logger.info(
        "build_shard_list: %d shards from %s to %s", len(shards), window_start, window_end
    )
    return shards


