"""What each registered source's pipeline can publish as a geography grain.

Held as a reviewed declaration, for the reason the dispatch registry is one: a
set discovered from the catalog at test time is the set the fixtures just
produced, so it agrees with any corpus and proves nothing about the one it was
given.

It lives here rather than beside its first reader because two tiers now ask the
same question of it. DB-044 (``tests/integration/api/test_catalog_serving_agreement``)
asks whether the fixture corpus reaches every grain a source can publish;
WEB-103 (``tests/support/viz_coverage``) asks whether any of those grains is one
the web's tile boundary can draw, because a source that publishes only
``NATIONAL`` has no spatial presentation however healthy it is. Two copies of
this table would be two answers to the first question a map asks.
"""

from __future__ import annotations

#: Where each entry's range comes from, since the repository states it per
#: source rather than in one place:
#:
#: * ``BLS`` -- ``bls/geography.py`` parses a LAUS area code to ``state`` or
#:   ``county`` and to nothing else ("LAUS has no national series"); the
#:   national CPS/CES series carry ``us:1``, which
#:   ``gold_bls.fact_bls_observation`` reads as ``NATIONAL``.
#: * ``CDC`` -- ``cdc/registry.py`` declares ``geography_levels`` per asset:
#:   ``("us", "state")`` for CDI, ``("us", "county")`` for PLACES.
#: * ``CENSUS_ACS`` -- ``census_acs/config.py`` declares
#:   ``geo_levels = ["us", "state", "county"]``.
#: * ``CENSUS_PEP`` -- ``silver_pep/transform.py`` maps summary levels 010,
#:   040, 050 and 162 to nation, state, county and place, and every other
#:   level to ``unsupported``, which reaches no served row.
#: * ``FBI_UCR`` -- ``fbi_ucr/registry.py`` closes ``subject_type`` to
#:   ``national``, ``state`` and ``agency``.
#: * ``FRED`` -- ``gold_fred.fact_fred_observation`` writes ``'us:1'`` and
#:   ``'NATIONAL'`` as literals. FRED is national by construction.
#: * ``USDA_NASS`` -- migration 012 closes ``geo_type`` to ``nation``,
#:   ``state``, ``county`` and ``unsupported``.
ADVERTISED_GEO_GRAINS: dict[str, frozenset[str]] = {
    "BLS": frozenset({"NATIONAL", "STATE", "COUNTY"}),
    "CDC": frozenset({"NATIONAL", "STATE", "COUNTY"}),
    "CENSUS_ACS": frozenset({"NATIONAL", "STATE", "COUNTY"}),
    "CENSUS_PEP": frozenset({"NATIONAL", "STATE", "COUNTY", "PLACE"}),
    "FBI_UCR": frozenset({"NATIONAL", "STATE", "AGENCY"}),
    "FRED": frozenset({"NATIONAL"}),
    "USDA_NASS": frozenset({"NATIONAL", "STATE", "COUNTY"}),
}
