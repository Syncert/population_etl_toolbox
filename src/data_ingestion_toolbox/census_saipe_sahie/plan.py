"""The estimate years one SAIPE/SAHIE run captures (see ``utility.backfill``)."""

from __future__ import annotations

from typing import Any

from data_ingestion_toolbox.census_saipe_sahie.registry import GEO_LEVELS, get_dataset
from data_ingestion_toolbox.utility.backfill import (
    answered_periods,
    fetch_answered_slices,
    plan_capture,
)

#: Estimate years an ordinary run re-captures per dataset, newest first: the
#: Bureau revises nothing older in place, it publishes the next year.
RECENT_YEARS = 2

ANSWERED_SLICES_SQL = """
    SELECT DISTINCT estimate_year, geo_level
    FROM control.census_sae_slice
    WHERE dataset_id = %s AND status = ANY(%s)
"""


def answered_years(connection: Any, dataset_id: str) -> set[int]:
    """The years whose every grain has been published or answered empty."""
    rows = fetch_answered_slices(connection, ANSWERED_SLICES_SQL, (dataset_id,))
    return answered_periods(
        ((int(year), str(level)) for year, level in rows), lambda _year: GEO_LEVELS
    )


def years_to_capture(
    dataset_id: str,
    conf: dict[str, Any] | None,
    answered: set[int] | frozenset[int] = frozenset(),
) -> list[int]:
    """The revision window plus every registered year never answered."""
    dataset = get_dataset(dataset_id)
    return plan_capture(
        dataset.years,
        dataset.years[-RECENT_YEARS:],
        answered,
        history=bool(conf and conf.get("history")),
    )
