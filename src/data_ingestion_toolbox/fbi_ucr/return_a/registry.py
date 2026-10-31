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
