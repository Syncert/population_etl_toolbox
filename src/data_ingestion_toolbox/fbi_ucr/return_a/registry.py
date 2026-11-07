"""The products the Return A master files load, frozen like the CDE's own.

The file publishes offense *components* (murder, rape, robbery, assault ...);
it has no violent or property total. Each product's total is therefore the
CDE's own definition summed from the file's actual-offense card, and named
as derived from it (``value_source``). Overlapping agencies are checked
against the CDE's published totals (DQ-FBI-011). Measure ids are spelled as
the CDE products spell them, so a county view reads either the same way.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from .layout import property_total, violent_total


@dataclass(frozen=True)
class ReturnAProduct:
    product_id: str
    offense_code: str
    offense_label: str
    #: The CDE summarized product whose published total it mirrors.
    mirrors_product_id: str
    total: Callable[[tuple[int | None, ...]], int]


RETURN_A_VIOLENT_CRIME = ReturnAProduct(
    "return_a_violent_crime",
    "V",
    "Violent Crime",
    "summarized_violent_crime",
    violent_total,
)
RETURN_A_PROPERTY_CRIME = ReturnAProduct(
    "return_a_property_crime",
    "P",
    "Property Crime",
    "summarized_property_crime",
    property_total,
)
RETURN_A_PRODUCTS: tuple[ReturnAProduct, ...] = (
    RETURN_A_VIOLENT_CRIME,
    RETURN_A_PROPERTY_CRIME,
)


#: The first master file the warehouse registers (``reta-2015.zip``).
FIRST_YEAR = 2015


def registered_years(today_year: int | None = None) -> list[int]:
    """Every registered Return A data year, newest first (the registry periods).

    A master file holds one finished calendar year, so the newest registered
    year is last year. The FBI publishes it during the following year, so
    until then that one year may answer "not found" (see ``load``); every
    older year is expected to exist. The scheduled run asks for all of them:
    a year never loaded is loaded, an unchanged file is skipped unread
    (LOADER RULE, CHURN RULE).
    """
    newest = (today_year or datetime.now(UTC).year) - 1
    return list(range(newest, FIRST_YEAR - 1, -1))
