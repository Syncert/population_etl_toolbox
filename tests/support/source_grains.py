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
#:   ``gold_bls.fact_bls_observation`` reads as ``NATIONAL``; the CPI and
#:   average-price series carry a Census region or division or a BLS metro
#:   (``silver_bls/geography_parser.py``, grocery-and-gasoline-prices).
#: * ``CDC`` -- ``cdc/registry.py`` declares ``geography_levels`` per asset:
#:   ``("us", "state")`` for CDI, ``("us", "county")`` for PLACES.
#: * ``CENSUS_ACS`` -- ``census_acs/config.py`` declares
#:   ``geo_levels = ["us", "state", "county", "place", "tract"]``; tracts
#:   from the 5-year estimates only (sub-county-geography).
#: * ``IRS_MIGRATION`` -- the SOI county files describe counties only; the
#:   file totals are served for the county each file describes.
#: * ``CENSUS_BPS`` -- ``census_bps/registry.py`` registers the state file
#:   (states and the US total), the county files and the place files.
#: * ``CENSUS_SAIPE_SAHIE`` -- ``census_saipe_sahie/registry.py`` requests
#:   ``us``, ``state`` and ``county`` only, and ``silver_census_sae`` closes
#:   ``geo_type`` to ``nation``, ``state`` and ``county``.
#: * ``BEA`` -- ``bea/registry.py`` loads the nation, states and counties
#:   and counts BEA's regions and combined areas out of scope; its price
#:   parities add CBSAs and BEA's own state metropolitan and nonmetropolitan
#:   portions (grocery-and-gasoline-prices).
#: * ``CENSUS_PEP`` -- ``silver_pep/transform.py`` maps summary levels 010,
#:   040, 050 and 162 to nation, state, county and place, and every other
#:   level to ``unsupported``, which reaches no served row.
#: * ``BLS_QCEW`` -- ``bls_qcew/registry.py`` registers aggregation levels
#:   10-14, 50-54 and 70-74 (national, state, county) and counts every other
#:   level -- MSAs and the "unknown county" areas -- out of scope.
#: * ``EIA`` -- ``eia/registry.py`` classifies ``NUS`` as the nation, ``S`` codes
#:   as states by USPS code, and PADDs and cities as EIA's own provider areas.
#: * ``CENSUS_CBP`` -- ``census_cbp/registry.py`` reads the nation, state
#:   and county files and counts each county file's statewide row out of
#:   scope.
#: * ``CENSUS_LODES`` -- the adapter sums each state's blocks to its
#:   counties and the state; no national figure is published.
#: * ``EPA_AQS`` -- a county figure derived from its highest complete
#:   monitor; counties without a complete monitor have no row.
#: * ``NOAA_NORMALS`` -- a county figure derived from the stations placed
#:   inside the county; counties without a standard or representative
#:   station have no row.
#: * ``FCC_BDC`` -- the FCC's own nation, state, county and place
#:   availability summaries; CBSA, district and tribal rows are not kept.
#: * ``FEMA_NRI`` -- the NRI county layer and county declaration counts;
#:   statewide and tribal-area declarations are not counted toward a county.
#: * ``FHFA_HPI`` -- the annual county workbook only; the ZIP and tract
#:   files wait on sub-county identities.
#: * ``HUD_FMR_IL`` -- HUD area values repeated per county; New England town
#:   rows are held, not published as their county.
#: * ``NCES_CCD`` -- county and state sums of the schools NCES's EDGE
#:   geocodes place there; no nation row.
#: * ``USDA_ERS`` -- county files only; ERS publishes no national or state
#:   row for these measures.
#: * ``FBI_UCR`` -- ``fbi_ucr/registry.py`` closes ``subject_type`` to
#:   ``national``, ``state`` and ``agency``.
#: * ``FRED`` -- ``gold_fred.fact_fred_observation`` writes ``'us:1'`` and
#:   ``'NATIONAL'`` as literals. FRED is national by construction.
#: * ``USDA_NASS`` -- migration 012 closes ``geo_type`` to ``nation``,
#:   ``state``, ``county`` and ``unsupported``.
ADVERTISED_GEO_GRAINS: dict[str, frozenset[str]] = {
    "BEA": frozenset({"NATIONAL", "STATE", "METRO", "COUNTY", "PROVIDER_AREA"}),
    "BLS": frozenset(
        {
            "NATIONAL",
            "CENSUS_REGION",
            "CENSUS_DIVISION",
            "STATE",
            "COUNTY",
            "PROVIDER_AREA",
        }
    ),
    "BLS_QCEW": frozenset({"NATIONAL", "STATE", "COUNTY"}),
    "CDC": frozenset({"NATIONAL", "STATE", "COUNTY"}),
    "CENSUS_ACS": frozenset({"NATIONAL", "STATE", "COUNTY", "PLACE", "TRACT"}),
    "CENSUS_BPS": frozenset({"NATIONAL", "STATE", "COUNTY", "PLACE"}),
    "CENSUS_PEP": frozenset({"NATIONAL", "STATE", "COUNTY", "PLACE"}),
    "CENSUS_SAIPE_SAHIE": frozenset({"NATIONAL", "STATE", "COUNTY"}),
    "EIA": frozenset({"NATIONAL", "STATE", "PROVIDER_AREA"}),
    "FBI_UCR": frozenset({"NATIONAL", "STATE", "AGENCY"}),
    "FRED": frozenset({"NATIONAL"}),
    "USDA_ERS": frozenset({"COUNTY"}),
    "NCES_CCD": frozenset({"STATE", "COUNTY"}),
    "HUD_FMR_IL": frozenset({"COUNTY"}),
    "FHFA_HPI": frozenset({"COUNTY"}),
    "FEMA_NRI": frozenset({"COUNTY"}),
    "FCC_BDC": frozenset({"NATIONAL", "STATE", "COUNTY", "PLACE"}),
    "NOAA_NORMALS": frozenset({"COUNTY"}),
    "EPA_AQS": frozenset({"COUNTY"}),
    "CENSUS_LODES": frozenset({"STATE", "COUNTY"}),
    "CENSUS_CBP": frozenset({"NATIONAL", "STATE", "COUNTY"}),
    "IRS_MIGRATION": frozenset({"COUNTY"}),
    "USDA_NASS": frozenset({"NATIONAL", "STATE", "COUNTY"}),
}
