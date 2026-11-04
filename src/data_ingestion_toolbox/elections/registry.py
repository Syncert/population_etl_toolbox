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
  A dataset's file list is read from Dataverse's API and the registered
  file is downloaded in its original format. The licences are recorded as
  the publisher states them on each dataset page and must be confirmed by
  hand before the first load (Dataverse refuses automated reads from the
  build container); see the operations guide.

Every stream is re-read whole, so a registered election or a new Congress
that was never loaded is loaded by the next scheduled run (the loader rule).
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

RESULT_STREAMS = (COUNTY_PRESIDENT, STATE_PRESIDENT, SENATE, HOUSE)
STREAMS = (LEGISLATORS, GOVERNORS, *RESULT_STREAMS)

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
        "U.S. President 1976-2020",
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
)


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
_REPUBLICAN = re.compile(r"\bREPUBLICAN\b")
_INDEPENDENT = re.compile(r"^(INDEPENDENT|IND\.?|NO PARTY AFFILIATION|NONPARTISAN)$")


def party_code(text: str | None) -> str:
    """The vocabulary code for a source's party text.

    "Democrat-Liberal" and "Democratic-Farmer-Labor" are Democratic;
    "Republican-Conservative" is Republican; "Ind. Democrat" and other
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
