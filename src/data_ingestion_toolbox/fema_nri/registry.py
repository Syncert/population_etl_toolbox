"""The registered FEMA National Risk Index fields and OpenFEMA declaration stream.

Read on 2026-10-07 from FEMA's own services:

* **National Risk Index, counties.** FEMA publishes the NRI county table as
  a keyless ArcGIS feature layer owned by ``FEMA_NationalRiskIndex``
  (``National_Risk_Index_Counties``, 3,232 counties and county equivalents,
  ``NRI_VER = 'December 2025'``, which is v1.20.0). The table zip on
  fema.gov refuses scripted requests (HTTP 403), so the layer is read: the
  registered fields only, no geometry, in pages of at most 2,000 ordered by
  ``OBJECTID``. A hazard's ``_EALR`` rating says when its loss is not a
  measurement: ``Not Applicable`` (the hazard cannot occur there),
  ``Insufficient Data``, ``Data Unavailable``; ``No Expected Annual Losses``
  and ``No Rating`` describe a published value.
* **Disaster declarations.** OpenFEMA's ``DisasterDeclarationsSummaries`` v2
  (one row per declaration per designated area, keyed by ``id``, revised by
  ``hash``), read with ``$select`` in pages of ``$top`` ordered by ``id``.
  ``fipsCountyCode = '000'`` is a statewide or non-county area (a tribal
  area, for example), never a county.

Neither needs a credential.
"""

from __future__ import annotations

from dataclasses import dataclass

NRI_LAYER_URL = "https://services.arcgis.com/XG15cJAlne2vxtgt/arcgis/rest/services/National_Risk_Index_Counties/FeatureServer/0/query"
DECLARATIONS_URL = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"

NRI = "nri"
DECLARATIONS = "declarations"
STREAMS = (NRI, DECLARATIONS)

NRI_PAGE_SIZE = 2000
DECLARATION_PAGE_SIZE = 10000

#: (field prefix, published key, FEMA's hazard name).
HAZARDS: tuple[tuple[str, str, str], ...] = (
    ("AVLN", "avalanche", "Avalanche"),
    ("CFLD", "coastal_flooding", "Coastal Flooding"),
    ("CWAV", "cold_wave", "Cold Wave"),
    ("DRGT", "drought", "Drought"),
    ("ERQK", "earthquake", "Earthquake"),
    ("HAIL", "hail", "Hail"),
    ("HWAV", "heat_wave", "Heat Wave"),
    ("HRCN", "hurricane", "Hurricane"),
    ("ISTM", "ice_storm", "Ice Storm"),
    ("LNDS", "landslide", "Landslide"),
    ("LTNG", "lightning", "Lightning"),
    ("IFLD", "inland_flooding", "Inland Flooding"),
    ("SWND", "strong_wind", "Strong Wind"),
    ("TRND", "tornado", "Tornado"),
    ("TSUN", "tsunami", "Tsunami"),
    ("VLCN", "volcanic_activity", "Volcanic Activity"),
    ("WFIR", "wildfire", "Wildfire"),
    ("WNTW", "winter_weather", "Winter Weather"),
)
#: The hazards whose annualized frequency is published.
FREQUENCY_HAZARDS: tuple[str, ...] = ("IFLD", "TRND", "WFIR", "HRCN", "HWAV")


@dataclass(frozen=True)
class NriField:
    field: str
    #: The rating field that says whether ``field`` is a measurement.
    rating_field: str | None
    measure: str
    unit: str
    label: str


def _nri_fields() -> tuple[NriField, ...]:
    fields = [
        NriField(
            "EAL_VALT",
            "EAL_RATNG",
            "expected_annual_loss",
            "dollars per year",
            "Expected Annual Loss - Total - Composite",
        ),
    ]
    for prefix, key, name in HAZARDS:
        fields.append(
            NriField(
                f"{prefix}_EALT",
                f"{prefix}_EALR",
                f"expected_annual_loss_{key}",
                "dollars per year",
                f"{name} - Expected Annual Loss - Total",
            )
        )
    names = {prefix: (key, name) for prefix, key, name in HAZARDS}
    for prefix in FREQUENCY_HAZARDS:
        key, name = names[prefix]
        fields.append(
            NriField(
                f"{prefix}_AFREQ",
                f"{prefix}_EALR",
                f"annualized_frequency_{key}",
                "events per year",
                f"{name} - Annualized Frequency",
            )
        )
    return tuple(fields)


NRI_FIELDS: tuple[NriField, ...] = _nri_fields()
NRI_IDENTITY_FIELDS: tuple[str, ...] = (
    "OBJECTID",
    "STCOFIPS",
    "STATEFIPS",
    "COUNTYFIPS",
    "COUNTY",
    "COUNTYTYPE",
    "NRI_VER",
)


def nri_out_fields() -> tuple[str, ...]:
    """Every field a page is requested with, in a fixed order."""
    names = list(NRI_IDENTITY_FIELDS)
    for item in NRI_FIELDS:
        for name in (item.field, item.rating_field):
            if name and name not in names:
                names.append(name)
    return tuple(names)


#: A rating that says the paired value is not a measurement, and the status
#: and reason it carries.
NON_MEASURE_RATINGS: dict[str, tuple[str, str]] = {
    "Not Applicable": ("not_applicable", "hazard_not_applicable"),
    "Insufficient Data": ("missing", "insufficient_data"),
    "Data Unavailable": ("missing", "data_unavailable"),
}

DECLARATION_FIELDS: tuple[str, ...] = (
    "id",
    "hash",
    "femaDeclarationString",
    "disasterNumber",
    "declarationType",
    "declarationDate",
    "incidentType",
    "fipsStateCode",
    "fipsCountyCode",
    "placeCode",
    "designatedArea",
    "lastRefresh",
)
#: (declarationType, published key, label).
DECLARATION_TYPES: tuple[tuple[str, str, str], ...] = (
    ("DR", "major_disaster_declarations", "Major disaster declarations"),
    ("EM", "emergency_declarations", "Emergency declarations"),
    ("FM", "fire_management_declarations", "Fire management assistance declarations"),
)
STATEWIDE_COUNTY_CODE = "000"
