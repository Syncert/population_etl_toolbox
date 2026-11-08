"""The registered election streams, offices and parties (elections-and-economic-performance).

Three publishers, each read whole on every run:

* **Seat holders in Congress.** The unitedstates/congress-legislators
  project (CC0 1.0) publishes every member's terms -- chamber, state,
  district or Senate class, party, start and end -- as
  ``legislators-current.json`` and ``legislators-historical.json`` on its
  ``gh-pages`` branch. A term's ``end`` is the day the member left (read on
  2026-11-04: no two Senate terms since 1969 overlap for one seat). A term
  whose party changed carries ``party_affiliations``; each affiliation is
  kept as its own span. An independent who caucuses with a party carries
  ``caucus``.
* **Governors.** Wikidata (CC0) is queried for every holder of a governor
  position whose jurisdiction is a U.S. state, with start and end dates and
  the holder's party statements. The query and its result are captured;
  the party in office is the party statement covering the term's start.
* **Election results.** MIT Election Data and Science Lab files on Harvard
  Dataverse: county presidential returns (2000 onward), and state-level
  presidential, U.S. Senate and U.S. House (district) returns (1976 onward).
  County returns before 2000 come from Algara & Amlani's county dataset on
  the same Dataverse (CC0 1.0, version 1.1 of 2022-09-04): county
  presidential returns for 1992 and 1996 and county U.S. Senate returns
  for 1990 to 2020, each an R ``.Rdata`` data frame of one row per county
  and contest (plan election-results-back-to-1990).
  A dataset's file list is read from Dataverse's API and the registered
  file is downloaded in its original format. The licences are recorded as
  the publisher states them on each dataset page and must be confirmed by
  hand before the first load (Dataverse refuses automated reads from the
  build container); see the operations guide.

* **Which House districts cover each county.** The Census Bureau's
  congressional district summary files on its API (public domain) give the
  population of every county piece of every district, for the Congresses
  whose plans it tabulated: the 110th (2000 census), the 111th (2010
  census), the 113th, 115th and 116th (2010 census) and the 118th and
  119th (2020 census). Read on OFFICE_LAB 2026-11-07: nothing is served for
  the 103rd to 109th Congresses, and the relationship files Census
  publishes give land area only. ``HOUSE_PLAN_YEARS`` says which plan each
  House election was held under.

Every stream is re-read whole, so a registered election or a new Congress
that was never loaded is loaded by the next scheduled run (the loader rule).
The district-to-county files are the exception: each is a frozen census
tabulation, so a plan is read until one read of it is published and never
again; a plan added to the registry is read by the next scheduled run.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

LEGISLATORS = "legislators"
GOVERNORS = "governors"
COUNTY_PRESIDENT = "county_president"
STATE_PRESIDENT = "state_president"
SENATE = "senate"
HOUSE = "house"
COUNTY_PRESIDENT_HISTORY = "county_president_history"
COUNTY_SENATE_HISTORY = "county_senate_history"
COUNTY_SENATE = "county_senate"
COUNTY_SENATE_PRECINCT = "county_senate_precinct"
HOUSE_COUNTY_SHARE = "house_county_share"

RESULT_STREAMS = (
    COUNTY_PRESIDENT,
    STATE_PRESIDENT,
    SENATE,
    HOUSE,
    COUNTY_PRESIDENT_HISTORY,
    COUNTY_SENATE_HISTORY,
    COUNTY_SENATE,
    COUNTY_SENATE_PRECINCT,
)
#: Streams read at county grain.
COUNTY_STREAMS = (
    COUNTY_PRESIDENT,
    COUNTY_PRESIDENT_HISTORY,
    COUNTY_SENATE_HISTORY,
    COUNTY_SENATE,
    COUNTY_SENATE_PRECINCT,
)
#: Streams MEDSL publishes by precinct only, summed into counties on replay.
PRECINCT_STREAMS = (COUNTY_SENATE_PRECINCT,)
#: The precinct files' reading rules, in each read's checksum like
#: ``MEDSL_READING_VERSION``.
#: 1 (2026-11-08): a candidate's TOTAL row in a precinct is their count
#: there, else the sum of their modes; total and ballot lines are not votes;
#: a suppressed count ("*") is quarantined, never zero; a blank party is the
#: party the same candidate carries elsewhere in the contest; the District
#: of Columbia's shadow senator is not a Senate seat.
#: 2 (2026-11-08): served as published, checked against the state; a county
#: total or a renamed copy printed as a precinct is set aside; counties a
#: spanning unit covers, and contests whose labels conflict, are withheld.
PRECINCT_READING_VERSION = "medsl-precinct-reading-2"
#: Streams whose ``totalvotes`` is not the area's own total. MEDSL's county
#: Senate 2022 file repeats the statewide total on every county row (read on
#: OFFICE_LAB 2026-11-07: 2,652,477 on each Wisconsin county), so a county's
#: total there is the sum of its candidates' votes.
STATEWIDE_TOTAL_STREAMS = (COUNTY_SENATE,)
#: The MEDSL files' reading rules, by version. A read's checksum includes it,
#: so a changed rule makes the next run of an unchanged file a new read that
#: replays once under the new rule; a run after that is unchanged again.
#: 2 (2026-11-07): total lines ("TOTAL VOTES CAST", "TOTAL") are the area's
#: total, over/under/blank/spoiled lines are not votes, and Alaska's state
#: House district rows are not counties.
#: 3 (2026-11-07): a key MEDSL repeats is a part of the count and is added
#: (Indiana 2022 precincts, North Carolina 2024 blank modes, folded write-in
#: and Other lines), except in Louisiana, where it is another round; the House
#: file's runoff column is a stage; quarantine records state and county.
#: 4 (2026-11-08): party text "GOP" is Republican (Washington 2024 Senate,
#: Raul Garcia, in the statewide file).
MEDSL_READING_VERSION = "medsl-reading-4"
MEDSL_STREAMS = (COUNTY_PRESIDENT, STATE_PRESIDENT, SENATE, HOUSE, COUNTY_SENATE)
#: The Algara & Amlani streams: one R data frame per file, one row per county
#: and contest with the two major parties' votes and the county's total.
HISTORY_STREAMS = (COUNTY_PRESIDENT_HISTORY, COUNTY_SENATE_HISTORY)
STREAMS = (LEGISLATORS, GOVERNORS, *RESULT_STREAMS, HOUSE_COUNTY_SHARE)

LEGISLATORS_BASE_URL = (
    "https://raw.githubusercontent.com/unitedstates/congress-legislators/gh-pages"
)
LEGISLATOR_FILES = ("legislators-current.json", "legislators-historical.json")
LEGISLATORS_LICENCE = "CC0 1.0 (unitedstates/congress-legislators)"

WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
WIKIDATA_LICENCE = "CC0 1.0 (Wikidata)"

#: Terms that end before this day are not kept: the warehouse's economic
#: history starts in 1969 (BEA CAINC).
FIRST_TERM_DAY = "1969-01-01"

#: The governor query. ``Q35657`` is "state of the United States",
#: ``Q132050`` is "governor"; ``P5087`` is the state's FIPS 5-2 numeric code.
#: Deprecated statements are left out. One row per term per party statement.
GOVERNOR_QUERY = """
SELECT ?position ?state ?stateFips ?person ?personLabel ?start ?end
       ?party ?partyLabel ?partyStart ?partyEnd WHERE {
  ?state wdt:P31 wd:Q35657 ; wdt:P5087 ?stateFips .
  ?position wdt:P1001 ?state ; (wdt:P31|wdt:P279)/wdt:P279* wd:Q132050 .
  ?person p:P39 ?held .
  ?held ps:P39 ?position ; pq:P580 ?start ; wikibase:rank ?rank .
  FILTER(?rank != wikibase:DeprecatedRank)
  OPTIONAL { ?held pq:P582 ?end . }
  FILTER(!BOUND(?end) || ?end >= "1969-01-01T00:00:00Z"^^xsd:dateTime)
  OPTIONAL {
    ?person p:P102 ?partyStatement .
    ?partyStatement ps:P102 ?party .
    OPTIONAL { ?partyStatement pq:P580 ?partyStart . }
    OPTIONAL { ?partyStatement pq:P582 ?partyEnd . }
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
ORDER BY ?stateFips ?start ?person
""".strip()

DATAVERSE_BASE_URL = "https://dataverse.harvard.edu"

#: Algara & Amlani, "Replication Data for: Partisanship & Nationalization in
#: American Elections: Evidence from Presidential, Senatorial, & Gubernatorial
#: Elections in the U.S. Counties, 1872-2020". Read on OFFICE_LAB 2026-11-07:
#: version 1.1 (2022-09-04), CC0 1.0, no guestbook, no restricted files.
ALGARA_AMLANI_DOI = "10.7910/DVN/DGUMFI"
ALGARA_AMLANI_LICENCE = "CC0 1.0 (Algara & Amlani, Harvard Dataverse)"

#: The first election year the county history streams load.
HISTORY_FIRST_YEAR = 1990


@dataclass(frozen=True)
class ResultDataset:
    """One MEDSL dataset on Harvard Dataverse and the file read from it."""

    stream: str
    doi: str
    label: str
    office: str
    geo_level: str
    #: The file whose original is downloaded, matched against Dataverse's
    #: file names so a new edition (``2000-2028``) is picked up.
    file_pattern: re.Pattern[str]
    first_year: int
    reference_url: str
    licence: str = (
        "As stated on the Dataverse dataset page (MEDSL); confirm before first load"
    )
    provider: str = "MEDSL"
    #: ``delimited`` (MEDSL's CSV or tab files), ``precinct`` (MEDSL's
    #: precinct CSV, summed by county) or ``rdata`` (an R data frame).
    payload_format: str = "delimited"
    #: The data frame an ``rdata`` file is read from (it may hold others).
    rdata_object: str | None = None

    @property
    def persistent_id(self) -> str:
        return f"doi:{self.doi}"


RESULT_DATASETS: tuple[ResultDataset, ...] = (
    ResultDataset(
        COUNTY_PRESIDENT,
        "10.7910/DVN/VOQCHQ",
        "County Presidential Election Returns 2000-2024",
        "president",
        "county",
        re.compile(r"^countypres_2000-20\d\d\.(csv|tab)$"),
        2000,
        "https://doi.org/10.7910/DVN/VOQCHQ",
    ),
    ResultDataset(
        STATE_PRESIDENT,
        "10.7910/DVN/42MVDX",
        "U.S. President 1976-2024",
        "president",
        "state",
        re.compile(r"^1976-20\d\d-president\.(csv|tab)$"),
        1976,
        "https://doi.org/10.7910/DVN/42MVDX",
    ),
    ResultDataset(
        SENATE,
        "10.7910/DVN/PEJ5QU",
        "U.S. Senate statewide 1976-2024",
        "us_senate",
        "state",
        # MEDSL renamed the file "1976-2024-senate-state" with the 2024 edition.
        re.compile(r"^1976-20\d\d-senate(-state)?\.(csv|tab)$"),
        1976,
        "https://doi.org/10.7910/DVN/PEJ5QU",
    ),
    ResultDataset(
        HOUSE,
        "10.7910/DVN/IG0UN2",
        "U.S. House 1976-2024",
        "us_house",
        "district",
        re.compile(r"^1976-20\d\d-house\.(csv|tab)$"),
        1976,
        "https://doi.org/10.7910/DVN/IG0UN2",
    ),
    ResultDataset(
        COUNTY_PRESIDENT_HISTORY,
        ALGARA_AMLANI_DOI,
        "County presidential returns 1868-2020 (Algara & Amlani)",
        "president",
        "county",
        re.compile(
            r"^dataverse_shareable_presidential_county_returns_1868_20\d\d\.Rdata$"
        ),
        HISTORY_FIRST_YEAR,
        f"https://doi.org/{ALGARA_AMLANI_DOI}",
        licence=ALGARA_AMLANI_LICENCE,
        provider="ALGARA_AMLANI",
        payload_format="rdata",
        rdata_object="pres_elections_release",
    ),
    ResultDataset(
        COUNTY_SENATE_HISTORY,
        ALGARA_AMLANI_DOI,
        "County U.S. Senate returns 1908-2020 (Algara & Amlani)",
        "us_senate",
        "county",
        re.compile(
            r"^dataverse_shareable_us_senate_county_returns_1908_20\d\d\.Rdata$"
        ),
        HISTORY_FIRST_YEAR,
        f"https://doi.org/{ALGARA_AMLANI_DOI}",
        licence=ALGARA_AMLANI_LICENCE,
        provider="ALGARA_AMLANI",
        payload_format="rdata",
        rdata_object="senate_elections_release",
    ),
    ResultDataset(
        COUNTY_SENATE,
        "10.7910/DVN/YB60EJ",
        "U.S. Senate county-level results 2022",
        "us_senate",
        "county",
        # One dataset per election; the 2022 file is senate_2022.csv. MEDSL
        # publishes 2024 only by precinct: COUNTY_SENATE_PRECINCT below.
        re.compile(r"^senate_20\d\d\.(csv|tab)$"),
        2022,
        "https://doi.org/10.7910/DVN/YB60EJ",
    ),
    ResultDataset(
        COUNTY_SENATE_PRECINCT,
        "10.7910/DVN/ZCM3BN",
        "U.S. Senate precinct-level returns 2024, summed by county",
        "us_senate",
        "county",
        # One file, read on OFFICE_LAB 2026-11-08: 994,635 rows, 34 states
        # and DC, no guestbook, no totalvotes column.
        re.compile(r"^SENATE_precinct_general\.(csv|tab)$"),
        2024,
        "https://doi.org/10.7910/DVN/ZCM3BN",
        payload_format="precinct",
    ),
)

#: Which provider serves a county contest in a given year, so one contest
#: never has two serving rows. MEDSL's county presidential file starts in
#: 2000; Algara & Amlani's presidential rows from 2000 to 2020 are kept in
#: silver only for the agreement check (DQ-ELEC-007), never served.
HISTORY_SERVED_BEFORE = {"president": 2000, "us_senate": 2100}

#: County codes retired since 1990, mapped to the county that carries the
#: area now (Census Bureau, "Substantial Changes to Counties and County
#: Equivalent Entities", public domain). A rename maps one to one; an
#: independent city that merged into its county is added into that county,
#: never apportioned. Yellowstone National Park (30113, about 50 voters) was
#: split between two counties in 1997 and is left unmapped.
COUNTY_SUCCESSOR: dict[str, str] = {
    "12025": "12086",  # Dade, FL -> Miami-Dade (1997), a rename
    "46113": "46102",  # Shannon, SD -> Oglala Lakota (2015), a rename
    "02270": "02158",  # Wade Hampton, AK -> Kusilvak (2015), a rename
    "51780": "51083",  # South Boston city, VA -> Halifax County (1995)
    "51560": "51005",  # Clifton Forge city, VA -> Alleghany County (2001)
    "51515": "51019",  # Bedford city, VA -> Bedford County (2013)
}


def result_dataset(stream: str) -> ResultDataset:
    for dataset in RESULT_DATASETS:
        if dataset.stream == stream:
            return dataset
    raise KeyError(f"unregistered result stream: {stream}")


#: The offices a seat term or a result can belong to.
OFFICES: tuple[tuple[str, str], ...] = (
    ("president", "President of the United States"),
    ("us_senate", "U.S. Senate"),
    ("us_house", "U.S. House of Representatives"),
    ("governor", "Governor"),
)
OFFICE_CODES = frozenset(code for code, _label in OFFICES)

#: The party vocabulary (plan decision L-2). A source's own party text is
#: always kept beside the code.
PARTIES: tuple[tuple[str, str], ...] = (
    ("REP", "Republican"),
    ("DEM", "Democratic"),
    ("IND", "Independent"),
    ("OTH", "Other"),
)
PARTY_CODES = frozenset(code for code, _label in PARTIES)

#: Wikidata items for the two major parties and independents.
WIKIDATA_PARTIES = {
    "Q29468": "REP",  # Republican Party
    "Q29552": "DEM",  # Democratic Party
    "Q327591": "IND",  # independent politician
}

_DEMOCRATIC = re.compile(r"\bDEMOCRAT(IC)?\b|DEMOCRATIC-FARMER-LABOR|DEMOCRATIC-NPL")
_REPUBLICAN = re.compile(r"\bREPUBLICAN\b|^GOP$")
_INDEPENDENT = re.compile(r"^(INDEPENDENT|IND\.?|NO PARTY AFFILIATION|NONPARTISAN)$")


def party_code(text: str | None) -> str:
    """The vocabulary code for a source's party text.

    "Democrat-Liberal" and "Democratic-Farmer-Labor" are Democratic;
    "Republican-Conservative" and "GOP" are Republican; "Ind. Democrat" and other
    labels are Other, never folded into a major party by guess.
    """
    if text is None:
        return "OTH"
    value = text.strip().upper()
    if not value:
        return "OTH"
    if value.startswith("IND. ") or value.startswith("INDEPENDENT "):
        return "OTH"
    if _REPUBLICAN.search(value) and not _DEMOCRATIC.search(value):
        return "REP"
    if _DEMOCRATIC.search(value) and not _REPUBLICAN.search(value):
        return "DEM"
    if _INDEPENDENT.match(value):
        return "IND"
    return "OTH"


#: USPS codes of the fifty states and the District of Columbia, with their
#: FIPS codes. Territories elect delegates the House files also carry; their
#: results are kept as unmapped rows, never served as states.
STATE_FIPS: dict[str, str] = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
    "CT": "09", "DE": "10", "DC": "11", "FL": "12", "GA": "13", "HI": "15",
    "ID": "16", "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21",
    "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
    "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46",
    "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
    "WV": "54", "WI": "55", "WY": "56",
}  # fmt: skip
FIPS_STATE: dict[str, str] = {fips: code for code, fips in STATE_FIPS.items()}


# ---------------------------------------------------------------------------
# House districts by county (Census Bureau API, public domain).
# ---------------------------------------------------------------------------

CENSUS_API_BASE_URL = "https://api.census.gov/data"
CENSUS_LICENCE = "Public domain (U.S. Census Bureau)"


@dataclass(frozen=True)
class DistrictPlan:
    """One Census tabulation of a Congress's districts by county piece."""

    key: str
    congress: int
    census_year: int
    path: str
    population_variable: str

    @property
    def url(self) -> str:
        return f"{CENSUS_API_BASE_URL}/{self.path}"


#: Each tabulation's API dataset and total-population variable (read on
#: OFFICE_LAB 2026-11-07 for Wisconsin; every one answers ``county (or
#: part)`` within ``state`` and ``congressional district``).
DISTRICT_PLANS: tuple[DistrictPlan, ...] = (
    DistrictPlan("cd110", 110, 2000, "2000/dec/cd110h", "P001001"),
    DistrictPlan("cd111", 111, 2010, "2010/dec/sf1", "P001001"),
    DistrictPlan("cd113", 113, 2010, "2010/dec/cd113", "P001001"),
    DistrictPlan("cd115", 115, 2010, "2010/dec/cd115", "P001001"),
    DistrictPlan("cd116", 116, 2010, "2010/dec/cd116", "P001001"),
    DistrictPlan("cd118", 118, 2020, "2020/dec/cd118", "P1_001N"),
    DistrictPlan("cd119", 119, 2020, "2020/dec/cd119", "P1_001N"),
)

#: The plan each House general election was held under, and the states
#: where it was not. A Congress with no tabulation of its own uses the one
#: before it, which is the same plan everywhere except where a state redrew
#: in between: North Carolina's court-ordered 2019 map was used in 2020
#: (116th tabulated, 117th not), so North Carolina 2020 has no county view.
#: Congresses before the 110th have no tabulation at all.
HOUSE_PLAN_YEARS: dict[int, tuple[str, tuple[str, ...]]] = {
    2006: ("cd110", ()),
    2008: ("cd111", ()),
    2010: ("cd111", ()),
    2012: ("cd113", ()),
    2014: ("cd113", ()),
    2016: ("cd115", ()),
    2018: ("cd116", ()),
    2020: ("cd116", ("NC",)),
    2022: ("cd118", ()),
    2024: ("cd119", ()),
}

#: Census's code for the District of Columbia's delegate district; MEDSL
#: numbers a single seat 0, as Census numbers an at-large seat ``00``.
CENSUS_DELEGATE_DISTRICT = "98"


def district_plan(key: str) -> DistrictPlan:
    for plan in DISTRICT_PLANS:
        if plan.key == key:
            return plan
    raise KeyError(f"unregistered district plan: {key}")


def congress_elected(election_year: int) -> int:
    """The Congress a November general election in this year elects."""
    return (election_year - 1786) // 2
