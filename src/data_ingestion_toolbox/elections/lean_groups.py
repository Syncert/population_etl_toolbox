"""Election lean groups (ADR-0014, plan item E-3).

The rebuild itself is SQL (``gold_elections/DDL/lean_groups.sql``); this
module runs it and holds the names the API and tests share with it.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Any

from psycopg2.extensions import connection

#: Measures in display order; each is compared one at a time, never summed.
MEASURES: tuple[str, ...] = (
    "real_income_per_person",
    "real_gdp",
    "jobs",
    "unemployment_rate",
    "poverty_rate",
)

#: Lean bands in display order, from the window-start presidential margin.
LEAN_BANDS: tuple[str, ...] = ("R20_plus", "R0_20", "D0_20", "D20_plus")

#: Peer groups a comparison can be narrowed to (ADR-0014 rule 4).
PEER_TYPES: tuple[str, ...] = (
    "all",
    "rural_urban",
    "state",
    "starting_level",
    "industry",
    "tax_burden",
)


def lean_band(margin: Decimal | float) -> str | None:
    """The band of a two-party margin (Republican minus Democratic, points).

    The same rule as ``build_lean_groups``: 20 or more either way is the outer
    band, anything else non-zero the inner one, and an exact tie is no band.
    """
    value = Decimal(str(margin))
    if value == 0:
        return None
    if value >= 20:
        return "R20_plus"
    if value > 0:
        return "R0_20"
    if value > -20:
        return "D0_20"
    return "D20_plus"


def refresh_lean_groups(
    connection_factory: Callable[[], connection], *, force: bool = False
) -> dict[str, Any]:
    """Rebuild the lean groups if their inputs changed; commit and report."""
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '55min'")
            cursor.execute("SET LOCAL lock_timeout = '30s'")
            cursor.execute(
                "SELECT refresh_id, status, place_rows, group_rows "
                "FROM gold_elections.refresh_lean_groups(%s)",
                (force,),
            )
            refresh_id, status, place_rows, group_rows = cursor.fetchone()
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    return {
        "refresh_id": refresh_id,
        "status": status,
        "place_rows": place_rows,
        "group_rows": group_rows,
    }
