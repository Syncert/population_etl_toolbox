"""Write the Algara & Amlani ``.Rdata`` fixtures from the rows below.

Development only: it needs pyreadr and pandas, which the project does not
depend on (pyreadr is AGPL-3.0), and is run by hand when the rows change::

    python -m venv /tmp/rtools && /tmp/rtools/bin/pip install pyreadr pandas
    /tmp/rtools/bin/python tests/fixtures/elections/make_history_fixtures.py

pyreadr writes R's version-2 serialization (``RDX2``); the real files are
version 3 (``RDX3``, R 4.0.2), which ``tests/unit/shared/test_rdata.py``
covers with hand-built bytes. The vote counts are illustrative, in the
release frames' real column layout (read on OFFICE_LAB 2026-11-07).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyreadr

HERE = Path(__file__).resolve().parent
NA = float("nan")

PRESIDENT_COLUMNS = [
    "election_year", "fips", "county_name", "state", "sfips", "office",
    "election_type", "seat_status", "democratic_raw_votes", "dem_nominee",
    "republican_raw_votes", "rep_nominee", "pres_raw_county_vote_totals_two_party",
    "raw_county_vote_totals", "county_first_date", "county_end_date",
    "state_admission_date", "complete_county_cases", "original_county_name",
    "original_name_end_date",
]  # fmt: skip

SENATE_COLUMNS = [
    "election_id", "election_year", "fips", "seat_class", "county_name", "state",
    "sfips", "office", "election_type", "seat_status", "democratic_raw_votes",
    "dem_nominee", "republican_raw_votes", "rep_nominee",
    "senate_raw_county_vote_totals_two_party", "raw_county_vote_totals",
    "county_first_date", "county_end_date", "state_admission_date",
    "original_county_name", "original_name_end_date",
]  # fmt: skip


def _president(
    year,
    fips,
    name,
    state,
    dem,
    rep,
    total,
    dem_name="Bill Clinton",
    rep_name="George H. W. Bush",
):  # noqa: ANN001, ANN202
    two = NA if dem != dem or rep != rep else dem + rep
    return [float(year), fips, name, state, fips[:2], "PRES", "G", "Open Seat",
            dem, dem_name, rep, rep_name, two, total, "1836-12-07", None,
            "1848-05-29", 1.0, None, NA]  # fmt: skip


PRESIDENT_ROWS = [
    # Before 1990: not read.
    _president(1988, "55025", "DANE", "WI", 105000.0, 60000.0, 167000.0,
               "Michael Dukakis", "George H. W. Bush"),
    _president(1992, "55025", "DANE", "WI", 120000.0, 60000.0, 205000.0),
    _president(1992, "55133", "WAUKESHA", "WI", 50000.0, 110000.0, 190000.0),
    # Dade's old code: read as Miami-Dade (12086).
    _president(1992, "12025", "DADE", "FL", 300000.0, 230000.0, 560000.0),
    # South Boston city merged into Halifax County in 1995: added into it.
    _president(1992, "51780", "SOUTH BOSTON CITY", "VA", 1500.0, 1800.0, 3600.0),
    _president(1992, "51083", "HALIFAX", "VA", 5000.0, 6000.0, 12000.0),
    # Neither major party's votes given: quarantined, never zero.
    _president(1992, "36001", "ALBANY", "NY", NA, NA, NA),
    _president(1996, "12086", "MIAMI-DADE", "FL", 317000.0, 209000.0, 560000.0,
               "Bill Clinton", "Bob Dole"),
    _president(1996, "55025", "DANE", "WI", 130000.0, 60000.0, 210000.0,
               "Bill Clinton", "Bob Dole"),
    # An identical repeat: one kept, the repeat set aside.
    _president(1996, "55025", "DANE", "WI", 130000.0, 60000.0, 210000.0,
               "Bill Clinton", "Bob Dole"),
    # Two copies that disagree: both quarantined.
    _president(1996, "55133", "WAUKESHA", "WI", 55000.0, 115000.0, 185000.0,
               "Bill Clinton", "Bob Dole"),
    _president(1996, "55133", "WAUKESHA", "WI", 56000.0, 115000.0, 185000.0,
               "Bill Clinton", "Bob Dole"),
    # MEDSL's years: kept for DQ-ELEC-007, never served. Dane agrees with
    # MEDSL's fixture; Waukesha's margin is more than half a point off.
    _president(2016, "55025", "DANE", "WI", 217697.0, 71275.0, 310363.0,
               "Hillary Clinton", "Donald Trump"),
    _president(2016, "55133", "WAUKESHA", "WI", 60000.0, 220000.0, 300000.0,
               "Hillary Clinton", "Donald Trump"),
]  # fmt: skip


def _senate(
    year, state, seat_class, fips, name, kind, dem, dem_name, rep, rep_name, total
):  # noqa: ANN001, ANN202
    two = (0 if dem != dem else dem) + (0 if rep != rep else rep)
    return [f"{year} {state} {seat_class}", float(year), fips, seat_class, name,
            state, str(int(fips[:2])), "SEN", kind, "Open seat", dem, dem_name, rep,
            rep_name, float(two), total, "1836-12-07", None, "1848-05-29", None,
            NA]  # fmt: skip


SENATE_ROWS = [
    _senate(1986, "WI", "Class III", "55025", "DANE", "G", 90000.0, "Ed Garvey",
            60000.0, "Bob Kasten", 151000.0),
    _senate(1992, "WI", "Class III", "55025", "DANE", "G", 125000.0, "Russ Feingold",
            70000.0, "Bob Kasten", 200000.0),
    _senate(1992, "WI", "Class III", "55133", "WAUKESHA", "G", 60000.0,
            "Russ Feingold", 120000.0, "Bob Kasten", 185000.0),
    _senate(1994, "WI", "Class I", "55025", "DANE", "G", 110000.0, "Herb Kohl",
            40000.0, "Robert T. Welch", 152000.0),
    # Two races in one state and year, told apart by class (a special).
    _senate(2018, "MN", "Class I", "27053", "HENNEPIN", "G", 450000.0,
            "Amy Klobuchar", 200000.0, "Jim Newberger", 670000.0),
    _senate(2018, "MN", "Class II", "27053", "HENNEPIN", "S", 410000.0, "Tina Smith",
            230000.0, "Karin Housley", 665000.0),
    # No Democratic nominee: Republican and the other candidates only.
    _senate(2002, "VA", "Class II", "51083", "HALIFAX", "G", NA, None, 6000.0,
            "John Warner", 8000.0),
]  # fmt: skip


def main() -> None:
    pyreadr.write_rdata(
        str(HERE / "county_president_history.Rdata"),
        pd.DataFrame(PRESIDENT_ROWS, columns=PRESIDENT_COLUMNS),
        df_name="pres_elections_release",
        compress="gzip",
    )
    pyreadr.write_rdata(
        str(HERE / "county_senate_history.Rdata"),
        pd.DataFrame(SENATE_ROWS, columns=SENATE_COLUMNS),
        df_name="senate_elections_release",
        compress="gzip",
    )


if __name__ == "__main__":
    main()
