"""The registered EIA retail gasoline products and the areas they describe.

Read from API v2 on 2026-10-07 (route ``petroleum/pri/gnd``, facets
``product`` and ``duoarea``). Only gasoline is registered; diesel is out of
scope. Each answer row carries ``period`` (the Monday of the survey week,
``YYYY-MM-DD``), ``duoarea`` and its ``area-name``, ``product`` and its
``product-name``, ``series`` (EIA's own series id), ``value`` in ``$/GAL``
and ``units``.

An area code says what kind of area it is, and nothing here reads a name:

* ``NUS`` is the United States;
* ``S`` and a USPS code is a state (``SCA`` California), resolved through
  the USPS code the shared reference's Census Gazetteer carries;
* ``R`` codes are Petroleum Administration for Defense Districts and their
  sub-districts and ``Y`` codes are cities: EIA's own areas, loaded as
  provider areas from EIA's own facet list. A city price is never assigned
  to a CBSA.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

#: product code -> grade.
PRODUCTS: dict[str, str] = {
    "EPMR": "Regular gasoline",
    "EPMM": "Midgrade gasoline",
    "EPMP": "Premium gasoline",
    "EPM0": "All grades gasoline",
}
UNITS = "$/GAL"
PROCESS = "PTE"

_STATE = re.compile(r"^S([A-Z]{2})$")
_EIA_AREA = re.compile(r"^(R[0-9A-Z]{2,5}|Y[0-9A-Z]{2,5})$")


@dataclass(frozen=True)
class AreaCode:
    """What an EIA area code is: its type and, where it has one, its identity."""

    geo_type: str
    #: The canonical id, or None for a state still to be resolved by USPS code.
    geo_id: str | None
    #: The state's USPS code, for a state.
    usps: str | None = None


def classify_area(duoarea: str) -> AreaCode | None:
    """The kind of area an EIA code names, or None for a code not registered."""
    code = (duoarea or "").strip().upper()
    if code == "NUS":
        return AreaCode("nation", "us:1")
    state = _STATE.fullmatch(code)
    if state:
        return AreaCode("state", None, usps=state.group(1))
    if _EIA_AREA.fullmatch(code):
        return AreaCode("provider_area", f"area:eia:{code}")
    return None


def metric_key(product: str) -> str:
    """The publisher's key for one grade, across every area: EIA's product code.

    The catalog code is therefore `EIA:EPMR` (regular), `EIA:EPMM`,
    `EIA:EPMP` and `EIA:EPM0`.
    """
    return product
