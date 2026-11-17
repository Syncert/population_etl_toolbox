"""The QCEW periods one run captures (see ``utility.backfill``)."""

from __future__ import annotations

from datetime import date
from typing import Any

from data_ingestion_toolbox.bls_qcew.config import QcewConfig
from data_ingestion_toolbox.bls_qcew.registry import (
    INDUSTRIES,
    recent_periods,
    registered_periods,
)
from data_ingestion_toolbox.utility.backfill import (
    answered_periods,
    fetch_answered_slices,
    plan_capture,
)

ANSWERED_SLICES_SQL = """
    SELECT DISTINCT year, period, industry_code
    FROM control.bls_qcew_slice
    WHERE status = ANY(%s)
"""

_INDUSTRY_CODES = tuple(industry.code for industry in INDUSTRIES)


def answered_qcew_periods(connection: Any) -> set[tuple[int, str]]:
    """The (year, period) pairs whose every industry slice has been answered."""
    rows = fetch_answered_slices(connection, ANSWERED_SLICES_SQL, ())
    return answered_periods(
        (((int(year), str(period)), str(code)) for year, period, code in rows),
        lambda _period: _INDUSTRY_CODES,
    )


def periods_to_capture(
    today: date,
    conf: dict[str, Any] | None,
    config: QcewConfig,
    answered: set[tuple[int, str]] | frozenset[tuple[int, str]] = frozenset(),
) -> list[str]:
    """The `year-period` keys one run captures: the window plus every unanswered period."""
    quarter = (today.month - 1) // 3
    year = today.year if quarter else today.year - 1
    periods = plan_capture(
        registered_periods(year, quarter or 4),
        recent_periods(today, config.recent_quarters),
        answered,
        history=bool(conf and conf.get("history")),
    )
    return [f"{year}-{period}" for year, period in periods]
