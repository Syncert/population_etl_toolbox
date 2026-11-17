"""The registered vehicle registration documents and what is read from them.

Report 28's tables, as WisDOT prints them:

* Table 1, "By County within CVT": per county ("County Name: 13 - DANE"),
  one line per city, village or town (name, CVT code, T/V/C), an UNKNOWN
  line for vehicles with no CVT, and the county's ``Sum:`` line. A county
  whose vehicles include no type prints no column for it (Menominee has no
  AUCY column).
* Table 2, "By County": one line per county (WisDOT county code and name),
  an UNKNOWN line, and the statewide ``Sum:`` line.
* Table 3 and after, by registration (plate) type: not read.

A blank cell is printed where a line has no vehicle of that type; it is kept
``blank`` with no number, and each line's printed ``Sum`` equals the sum of
its numbers. WisDOT's county codes are its own (alphabetical, Menominee 73),
not FIPS codes; ``WISDOT_COUNTIES`` places each by code and printed name.

MV-1 is one sheet: a title, a header of five groups (automobiles, buses,
trucks, motorcycles, all motor vehicles), each three columns (private and
commercial, publicly owned, total), one row per state and the District of
Columbia, a ``Total`` row and footnotes. A state's name may carry footnote
marks such as ``(2)``; they are kept.
"""

from __future__ import annotations

from dataclasses import dataclass

from data_ingestion_toolbox.fbi_ucr.registry import STATE_CODE_CONTRACT

RPT28_CONTRACT_VERSION = "wisdot_rpt28_pdf_layout:v1"
MV1_CONTRACT_VERSION = "fhwa_mv1_xlsx:v1"
INDEX_CONTRACT_VERSION = "wisdot_veh_info_html:v1"

#: WisDOT vehicle types, as Report 28 heads its columns.
WISDOT_VEHICLE_TYPES: dict[str, str] = {
    "AUCY": "Autocycles",
    "AUTO": "Automobiles (passenger vehicles)",
    "CYCL": "Motorcycles and mopeds",
    "TRLR": "Trailers",
    "TRUK": "Trucks (pick-ups, vans, most SUVs and heavy trucks)",
}
#: The printed row total.
SUM_COLUMN = "SUM"

#: WisDOT county code -> (name as Report 28 prints it, shared geography id).
WISDOT_COUNTIES: dict[str, tuple[str, str]] = {
    "01": ("ADAMS", "55001"),
    "02": ("ASHLAND", "55003"),
    "03": ("BARRON", "55005"),
    "04": ("BAYFIELD", "55007"),
    "05": ("BROWN", "55009"),
    "06": ("BUFFALO", "55011"),
    "07": ("BURNETT", "55013"),
    "08": ("CALUMET", "55015"),
    "09": ("CHIPPEWA", "55017"),
    "10": ("CLARK", "55019"),
    "11": ("COLUMBIA", "55021"),
    "12": ("CRAWFORD", "55023"),
    "13": ("DANE", "55025"),
    "14": ("DODGE", "55027"),
    "15": ("DOOR", "55029"),
    "16": ("DOUGLAS", "55031"),
    "17": ("DUNN", "55033"),
    "18": ("EAU CLAIRE", "55035"),
    "19": ("FLORENCE", "55037"),
    "20": ("FOND DU LAC", "55039"),
    "21": ("FOREST", "55041"),
    "22": ("GRANT", "55043"),
    "23": ("GREEN", "55045"),
    "24": ("GREEN LAKE", "55047"),
    "25": ("IOWA", "55049"),
    "26": ("IRON", "55051"),
    "27": ("JACKSON", "55053"),
    "28": ("JEFFERSON", "55055"),
    "29": ("JUNEAU", "55057"),
    "30": ("KENOSHA", "55059"),
    "31": ("KEWAUNEE", "55061"),
    "32": ("LA CROSSE", "55063"),
    "33": ("LAFAYETTE", "55065"),
    "34": ("LANGLADE", "55067"),
    "35": ("LINCOLN", "55069"),
    "36": ("MANITOWOC", "55071"),
    "37": ("MARATHON", "55073"),
    "38": ("MARINETTE", "55075"),
    "39": ("MARQUETTE", "55077"),
    "40": ("MILWAUKEE", "55079"),
    "41": ("MONROE", "55081"),
    "42": ("OCONTO", "55083"),
    "43": ("ONEIDA", "55085"),
    "44": ("OUTAGAMIE", "55087"),
    "45": ("OZAUKEE", "55089"),
    "46": ("PEPIN", "55091"),
    "47": ("PIERCE", "55093"),
    "48": ("POLK", "55095"),
    "49": ("PORTAGE", "55097"),
    "50": ("PRICE", "55099"),
    "51": ("RACINE", "55101"),
    "52": ("RICHLAND", "55103"),
    "53": ("ROCK", "55105"),
    "54": ("RUSK", "55107"),
    "55": ("ST. CROIX", "55109"),
    "56": ("SAUK", "55111"),
    "57": ("SAWYER", "55113"),
    "58": ("SHAWANO", "55115"),
    "59": ("SHEBOYGAN", "55117"),
    "60": ("TAYLOR", "55119"),
    "61": ("TREMPEALEAU", "55121"),
    "62": ("VERNON", "55123"),
    "63": ("VILAS", "55125"),
    "64": ("WALWORTH", "55127"),
    "65": ("WASHBURN", "55129"),
    "66": ("WASHINGTON", "55131"),
    "67": ("WAUKESHA", "55133"),
    "68": ("WAUPACA", "55135"),
    "69": ("WAUSHARA", "55137"),
    "70": ("WINNEBAGO", "55139"),
    "71": ("WOOD", "55141"),
    "73": ("MENOMINEE", "55078"),
}


def county_geo_id(fips: str) -> str:
    """``55025`` -> ``state:55|county:025``."""
    return f"state:{fips[:2]}|county:{fips[2:]}"


@dataclass(frozen=True)
class Mv1Column:
    vehicle_class: str
    ownership: str
    label: str


#: MV-1's five groups, as its header names them, and the shared key of each.
MV1_GROUPS: tuple[tuple[str, str], ...] = (
    ("AUTOMOBILES", "automobiles"),
    ("BUSES", "buses"),
    ("TRUCKS", "trucks"),
    ("MOTORCYCLES", "motorcycles"),
    ("ALL MOTOR VEHICLES", "all_motor_vehicles"),
)
#: The three columns of each group, left to right.
MV1_OWNERSHIP: tuple[tuple[str, str], ...] = (
    ("private_commercial", "private and commercial"),
    ("publicly_owned", "publicly owned"),
    ("total", "total"),
)

MV1_VEHICLE_CLASS_LABELS: dict[str, str] = {
    "automobiles": "Automobiles (including taxicabs)",
    "buses": "Buses",
    "trucks": "Trucks",
    "motorcycles": "Motorcycles",
    "all_motor_vehicles": "All motor vehicles",
}

#: MV-1's state names -> two-digit FIPS. The District is ``Dist. of Col.``.
MV1_STATE_FIPS: dict[str, str] = {
    name: fips for name, fips in STATE_CODE_CONTRACT.values() if fips <= "56"
}
MV1_STATE_FIPS["Dist. of Col."] = "11"
MV1_TOTAL_LABEL = "Total"


@dataclass(frozen=True)
class Measure:
    """One served figure: a Report 28 column or an MV-1 column."""

    measure_id: str
    publisher: str  # wisdot_rpt28 | fhwa_mv1
    source_column: str
    label: str
    basis: str


def _wisdot_measures() -> tuple[Measure, ...]:
    measures = [
        Measure(
            f"wisdot_rpt28:{code.lower()}",
            "wisdot_rpt28",
            code,
            f"WisDOT {code}: {label}",
            f"vehicles WisDOT counts as valid registrations of type {code} for the "
            "calendar year, by the owner's county, as Report 28 prints them",
        )
        for code, label in WISDOT_VEHICLE_TYPES.items()
    ]
    measures.append(
        Measure(
            "wisdot_rpt28:all",
            "wisdot_rpt28",
            SUM_COLUMN,
            "WisDOT Sum: registered vehicles of every type",
            "the Sum Report 28 prints for the county or state, every vehicle type "
            "including trailers",
        )
    )
    return tuple(measures)


def _mv1_measures() -> tuple[Measure, ...]:
    return tuple(
        Measure(
            f"fhwa_mv1:{vehicle_class}:{ownership}",
            "fhwa_mv1",
            f"{vehicle_class}:{ownership}",
            f"{MV1_VEHICLE_CLASS_LABELS[vehicle_class]} registered, {ownership_label}",
            f"{MV1_VEHICLE_CLASS_LABELS[vehicle_class].lower()} registered in the "
            f"state, {ownership_label}, as FHWA's Highway Statistics table MV-1 "
            "publishes them",
        )
        for _label, vehicle_class in MV1_GROUPS
        for ownership, ownership_label in MV1_OWNERSHIP
    )


MEASURES: tuple[Measure, ...] = _wisdot_measures() + _mv1_measures()
MEASURE_IDS = frozenset(measure.measure_id for measure in MEASURES)
