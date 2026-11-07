"""Real PostgreSQL elections capture-to-gold contract.

Covers: ETL-111
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.elections import capture as capture_module
from data_ingestion_toolbox.elections.client import ElectionsPayloadError
from data_ingestion_toolbox.elections.registry import (
    COUNTY_PRESIDENT,
    COUNTY_PRESIDENT_HISTORY,
    COUNTY_SENATE,
    COUNTY_SENATE_HISTORY,
    GOVERNORS,
    HOUSE,
    HOUSE_COUNTY_SHARE,
    LEGISLATORS,
    SENATE,
    STATE_PRESIDENT,
)
from data_ingestion_toolbox.elections.schema import (
    REQUIRED_RELATIONS,
    ensure_elections_schema,
)
from data_ingestion_toolbox.glossary.harvest import (
    Publisher,
    harvest_publisher,
    process_pending_events,
)
from data_ingestion_toolbox.quality.sources import (
    elections_county_history_agreement,
    elections_house_district_coverage,
    elections_run_reconciliation,
    elections_seat_overlap,
    elections_totals_and_geography,
)
from tests.support import elections as el

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def elections_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return el.reviewed_warehouse(postgres_connection_factory, request)


def _rows(
    factory: Callable[[], connection], sql: str, parameters: tuple | None = None
) -> list[tuple]:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


def _write(factory: Callable[[], connection], sql: str, parameters: tuple) -> None:
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(sql, parameters)
        writer.commit()
    finally:
        writer.close()


def _outcome(factory: Callable[[], connection], executor):  # noqa: ANN001, ANN202
    reader = factory()
    try:
        with reader.cursor() as cursor:
            return executor(cursor, {})[0]
    finally:
        reader.close()


def test_seat_terms_keep_party_changes_caucus_and_dates(elections_warehouse) -> None:
    """Covers: ETL-111 — one span per party a member held; an independent keeps their caucus."""
    factory = elections_warehouse
    _run, status, rows, published = el.run_to_gold(factory, LEGISLATORS)
    assert (status, published) == ("captured", 1) and rows == 85
    van_drew = _rows(
        factory,
        """
        SELECT seat_label, party_code, start_date, end_date FROM gold_elections.seat_term
         WHERE holder_name = 'Jefferson Van Drew' ORDER BY start_date LIMIT 2
        """,
    )
    assert van_drew == [
        ("NJ-02", "DEM", dt.date(2019, 1, 3), dt.date(2019, 12, 18)),
        ("NJ-02", "REP", dt.date(2019, 12, 19), dt.date(2021, 1, 3)),
    ]
    assert _rows(
        factory,
        """
        SELECT DISTINCT seat_label, party_code, caucus_code, source_label
          FROM gold_elections.seat_term WHERE holder_name = 'Bernard Sanders'
         ORDER BY seat_label
        """,
    ) == [
        (
            "VT Senate class 1",
            "IND",
            "DEM",
            "unitedstates/congress-legislators (CC0 1.0)",
        ),
        ("VT-AL", "IND", "DEM", "unitedstates/congress-legislators (CC0 1.0)"),
    ]
    # A term that ended before 1969 is not kept; one that crossed it is.
    assert _rows(
        factory,
        "SELECT MIN(end_date) > DATE '1969-01-01' FROM gold_elections.seat_term",
    ) == [(True,)]
    # Wisconsin's two Senate seats are held today by one member each.
    assert _rows(
        factory,
        """
        SELECT seat_label, holder_name FROM gold_elections.seat_term
         WHERE state_code = 'WI' AND office = 'us_senate' AND end_date > DATE '2026-11-04'
         ORDER BY seat_label
        """,
    ) == [("WI Senate class 1", "Tammy Baldwin"), ("WI Senate class 3", "Ron Johnson")]

    _run, status, rows, published = el.run_to_gold(factory, GOVERNORS)
    assert (status, published) == ("captured", 1) and rows == 6
    governors = _rows(
        factory,
        """
        SELECT holder_name, party_code, start_date, end_date, source_label
          FROM gold_elections.seat_term WHERE office = 'governor' AND state_code = 'WI'
         ORDER BY start_date
        """,
    )
    assert [(name, party) for name, party, *_ in governors] == [
        ("Tommy Thompson", "REP"),
        ("Scott McCallum", "REP"),
        ("Jim Doyle", "DEM"),
        ("Scott Walker", "REP"),
        ("Tony Evers", "DEM"),
    ]
    assert governors[-1][3] is None and governors[-1][4] == "Wikidata (CC0 1.0)"
    # Crist is recorded with the party he took office in.
    assert _rows(
        factory,
        "SELECT party_code FROM gold_elections.seat_term WHERE holder_name = 'Charlie Crist'",
    ) == [("REP",)]
    assert _rows(
        factory,
        "SELECT error_code FROM silver_elections.quarantine ORDER BY error_code",
    ) == [("unreadable_state",)]
    assert _outcome(factory, elections_seat_overlap).result == "pass"


def test_results_count_each_candidate_once_and_derive_the_margin(
    elections_warehouse,
) -> None:
    """Covers: ETL-111 — TOTAL lines or summed modes, fusion lines merged, quarantine for gaps."""
    factory = elections_warehouse
    _run, status, rows, published = el.run_to_gold(factory, COUNTY_PRESIDENT)
    assert (status, published) == ("captured", 1) and rows == 15
    assert _rows(
        factory,
        "SELECT record_index, error_code FROM silver_elections.quarantine ORDER BY 1",
    ) == [(16, "no_county_code"), (17, "unreadable_votes")]
    contests = {
        (geo_id, year): (republican, democratic, total, leading)
        for geo_id, year, republican, democratic, total, leading in _rows(
            factory,
            """
            SELECT geo_id, election_year, republican_votes, democratic_votes, total_votes,
                   leading_party_code
              FROM gold_elections.contest_result WHERE office = 'president'
            """,
        )
    }
    # Dane 2020 publishes only modes: the candidate is their sum; the county
    # total MEDSL repeats on each mode is counted once.
    assert contests[(el.DANE, 2020)] == (78800, 260000, 344000, "DEM")
    # Albany's Biden and Trump each appear on two lines.
    assert contests[(el.ALBANY, 2020)] == (50000, 96000, 150000, "DEM")
    assert contests[(el.WAUKESHA, 2016)][3] == "REP"
    observations = {
        (metric, geo_id, year): (value, period, release)
        for metric, geo_id, year, value, period, release in _rows(
            factory,
            """
            SELECT metric_key, geo_id, year, value, period_start, release_key
              FROM gold_elections.observation_latest
            """,
        )
    }
    assert observations[("president_two_party_margin", el.DANE, 2016)] == (
        Decimal("-50.67"),
        dt.date(2016, 11, 8),
        "MEDSL countypres_2000-2024.csv",
    )
    assert observations[("president_two_party_republican_share", el.ALBANY, 2020)][
        :2
    ] == (Decimal("34.25"), dt.date(2020, 11, 3))
    # Waukesha 2020's only Republican row had no count: there is no 2020
    # Waukesha observation rather than one that reads as a Democratic sweep.
    assert ("president_two_party_margin", el.WAUKESHA, 2020) not in observations

    for stream, expected in ((STATE_PRESIDENT, 5), (SENATE, 6), (HOUSE, 3)):
        _run, status, rows, published = el.run_to_gold(factory, stream)
        assert (status, rows, published) == ("captured", expected, 1), stream
    assert _rows(
        factory,
        """
        SELECT special, republican_votes, democratic_votes FROM gold_elections.contest_result
         WHERE office = 'us_senate' AND state_code = 'MN' ORDER BY special
        """,
    ) == [(False, 940437, 1566174), (True, 1095777, 1370540)]
    # House districts are contests, not yet observations of a shared geography.
    assert _rows(
        factory,
        """
        SELECT district, geography_status, democratic_votes FROM gold_elections.contest_result
         WHERE office = 'us_house' ORDER BY state_code
        """,
    ) == [(2, "district", 268740), (0, "district", None)]
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_elections.observation_latest WHERE geo_type = 'district'",
    ) == [(0,)]
    assert _rows(
        factory,
        """
        SELECT value FROM gold_elections.observation_latest
         WHERE metric_key = 'president_two_party_margin' AND geo_id = %s AND year = 2020
        """,
        (el.WISCONSIN,),
    ) == [(Decimal("-0.64"),)]


#: MEDSL's county presidential layout with the mode mixes of its 2020 and
#: 2024 files: Dane's county total repeated on four modes (Georgia's shape),
#: Waukesha's TOTAL line beside its ONE STOP and FAILSAFE modes, and Albany
#: publishing each mode's own total.
MODE_MIX = b"""year,state,state_po,county_name,county_fips,office,candidate,party,candidatevotes,totalvotes,version,mode
2020,WISCONSIN,WI,DANE,55025,US PRESIDENT,JOSEPH R BIDEN JR,DEMOCRAT,3000,15257,20250101,ABSENTEE
2020,WISCONSIN,WI,DANE,55025,US PRESIDENT,DONALD J TRUMP,REPUBLICAN,1000,15257,20250101,ABSENTEE
2020,WISCONSIN,WI,DANE,55025,US PRESIDENT,JOSEPH R BIDEN JR,DEMOCRAT,4000,15257,20250101,ELECTION DAY
2020,WISCONSIN,WI,DANE,55025,US PRESIDENT,DONALD J TRUMP,REPUBLICAN,3000,15257,20250101,ELECTION DAY
2020,WISCONSIN,WI,DANE,55025,US PRESIDENT,JOSEPH R BIDEN JR,DEMOCRAT,2000,15257,20250101,EARLY VOTING
2020,WISCONSIN,WI,DANE,55025,US PRESIDENT,DONALD J TRUMP,REPUBLICAN,2000,15257,20250101,EARLY VOTING
2020,WISCONSIN,WI,DANE,55025,US PRESIDENT,JOSEPH R BIDEN JR,DEMOCRAT,100,15257,20250101,PROVISIONAL
2020,WISCONSIN,WI,DANE,55025,US PRESIDENT,DONALD J TRUMP,REPUBLICAN,57,15257,20250101,PROVISIONAL
2020,WISCONSIN,WI,WAUKESHA,55133,US PRESIDENT,JOSEPH R BIDEN JR,DEMOCRAT,5000,11500,20250101,TOTAL
2020,WISCONSIN,WI,WAUKESHA,55133,US PRESIDENT,DONALD J TRUMP,REPUBLICAN,6000,11500,20250101,TOTAL
2020,WISCONSIN,WI,WAUKESHA,55133,US PRESIDENT,JOSEPH R BIDEN JR,DEMOCRAT,3000,11500,20250101,ONE STOP
2020,WISCONSIN,WI,WAUKESHA,55133,US PRESIDENT,DONALD J TRUMP,REPUBLICAN,4000,11500,20250101,ONE STOP
2020,WISCONSIN,WI,WAUKESHA,55133,US PRESIDENT,JOSEPH R BIDEN JR,DEMOCRAT,2000,11500,20250101,FAILSAFE
2020,WISCONSIN,WI,WAUKESHA,55133,US PRESIDENT,DONALD J TRUMP,REPUBLICAN,2000,11500,20250101,FAILSAFE
2020,NEW YORK,NY,ALBANY,36001,US PRESIDENT,JOSEPH R BIDEN JR,DEMOCRAT,600,950,20250101,ELECTION DAY
2020,NEW YORK,NY,ALBANY,36001,US PRESIDENT,DONALD J TRUMP,REPUBLICAN,300,950,20250101,ELECTION DAY
2020,NEW YORK,NY,ALBANY,36001,US PRESIDENT,JOSEPH R BIDEN JR,DEMOCRAT,400,520,20250101,ABSENTEE
2020,NEW YORK,NY,ALBANY,36001,US PRESIDENT,DONALD J TRUMP,REPUBLICAN,100,520,20250101,ABSENTEE
"""


def _president_2020(factory: Callable[[], connection]) -> dict[str, tuple]:
    return {
        geo_id: (republican, democratic, total)
        for geo_id, republican, democratic, total in _rows(
            factory,
            """
            SELECT geo_id, republican_votes, democratic_votes, total_votes
              FROM gold_elections.contest_result
             WHERE office = 'president' AND election_year = 2020
            """,
        )
    }


def test_a_county_total_repeated_on_each_mode_is_counted_once(
    elections_warehouse,
) -> None:
    """Covers: ETL-111 — a county total repeated on each mode counts once; a TOTAL line wins; part totals add up; only changed stored contests are rewritten."""
    factory = elections_warehouse
    run_id, _status, rows, published = el.run_to_gold(
        factory,
        COUNTY_PRESIDENT,
        client=el.FixtureClient({"county_president.csv": MODE_MIX}),
    )
    assert (rows, published) == (18, 1)
    assert _president_2020(factory) == {
        # Four modes each carry Dane's 15,257: the total is 15,257, not 61,028.
        el.DANE: (6057, 9100, 15257),
        # The TOTAL line is read alone; its modes are not added to it.
        el.WAUKESHA: (6000, 5000, 11500),
        # Albany's modes each publish their own part: 950 + 520.
        el.ALBANY: (400, 1000, 1470),
    }

    # A store written by the old totalling (Dane at four times its total) is
    # corrected by rewriting that one contest; the others are left untouched.
    _write(
        factory,
        """
        UPDATE gold_elections.contest_result_store SET total_votes = 61028
         WHERE run_id = %s AND geo_id = %s
        """,
        (str(run_id), el.DANE),
    )
    untouched = _rows(
        factory,
        """
        SELECT geo_id, xmin::TEXT FROM gold_elections.contest_result_store
         WHERE run_id = %s AND geo_id <> %s ORDER BY geo_id
        """,
        (str(run_id), el.DANE),
    )
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute("SELECT gold_elections.reconcile_contest_result()")
            assert cursor.fetchone() == (1,)
        writer.commit()
    finally:
        writer.close()
    assert _president_2020(factory)[el.DANE] == (6057, 9100, 15257)
    assert (
        _rows(
            factory,
            """
            SELECT geo_id, xmin::TEXT FROM gold_elections.contest_result_store
             WHERE run_id = %s AND geo_id <> %s ORDER BY geo_id
            """,
            (str(run_id), el.DANE),
        )
        == untouched
    )
    # Nothing left to correct: a second pass writes nothing.
    assert _rows(factory, "SELECT gold_elections.reconcile_contest_result()") == [(0,)]


def test_a_changed_medsl_reading_rule_replays_an_unchanged_file_once(
    elections_warehouse, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: ETL-111 — a new MEDSL reading version makes one new read of an unchanged file, then unchanged again."""
    factory = elections_warehouse
    first, _status, _rows_written, _published = el.run_to_gold(
        factory, COUNTY_PRESIDENT
    )
    monkeypatch.setattr(capture_module, "MEDSL_READING_VERSION", "medsl-reading-test")
    second, status, rows, published = el.run_to_gold(factory, COUNTY_PRESIDENT)
    assert (status, rows, published) == ("captured", 15, 1)
    assert _rows(
        factory,
        "SELECT DISTINCT run_id::TEXT FROM gold_elections.contest_result_store",
    ) == [(str(second),)]
    assert first != second
    _run, status, rows, published = el.run_to_gold(factory, COUNTY_PRESIDENT)
    assert (status, rows, published) == ("unchanged", 0, 0)


def test_reruns_and_revisions(elections_warehouse) -> None:
    """Covers: ETL-111 — an unchanged read replays nothing; a corrected file becomes the current read, stored once."""
    factory = elections_warehouse
    el.run_to_gold(factory, COUNTY_PRESIDENT)
    _run, status, rows, published = el.run_to_gold(factory, COUNTY_PRESIDENT)
    assert (status, rows, published) == ("unchanged", 0, 0)
    corrected = el.fixture_bytes("county_president.csv").replace(
        b"REPUBLICAN,NA,250000", b"REPUBLICAN,160000,250000"
    )
    run_id, status, rows, published = el.run_to_gold(
        factory,
        COUNTY_PRESIDENT,
        client=el.FixtureClient({"county_president.csv": corrected}),
    )
    assert (status, rows, published) == ("captured", 16, 1)
    assert _rows(
        factory,
        "SELECT run_id::TEXT FROM gold_elections.current_read WHERE stream = %s",
        (COUNTY_PRESIDENT,),
    ) == [(str(run_id),)]
    assert _rows(
        factory,
        """
        SELECT republican_votes FROM gold_elections.contest_result
         WHERE geo_id = %s AND election_year = 2020
        """,
        (el.WAUKESHA,),
    ) == [(160000,)]
    # The served contests are stored once per read: the superseded read's
    # rows are gone, and the store matches a fresh total of the current read.
    assert _rows(
        factory,
        """
        SELECT run_id::TEXT, COUNT(*) FROM gold_elections.contest_result_store
         GROUP BY run_id
        """,
    ) == [(str(run_id), 5)]
    assert _rows(
        factory,
        """
        SELECT COUNT(*) FROM (
            SELECT * FROM gold_elections.contest_tally WHERE run_id = %s
            EXCEPT SELECT * FROM gold_elections.contest_result_store
        ) AS differs
        """,
        (str(run_id),),
    ) == [(0,)]
    assert _rows(factory, "SELECT gold_elections.sync_contest_result()") == [(0,)]


def test_a_refused_file_and_the_rules(elections_warehouse) -> None:
    """Covers: ETL-111 — an error body fails capture; DQ-ELEC-002, -003 and -004 pass or warn, then catch a fault."""
    factory = elections_warehouse
    assert _outcome(factory, elections_run_reconciliation).result == "not_applicable"
    with pytest.raises(ElectionsPayloadError, match="service_error"):
        el.run_to_gold(
            factory,
            COUNTY_PRESIDENT,
            client=el.FixtureClient(
                {"listing-county_president.json": b'{"status": "ERROR"}'}
            ),
        )
    with pytest.raises(ElectionsPayloadError, match="not_a_results_file"):
        el.run_to_gold(
            factory,
            COUNTY_PRESIDENT,
            client=el.FixtureClient(
                {"county_president.csv": b"<html>maintenance</html>"}
            ),
        )
    assert _rows(factory, "SELECT COUNT(*) FROM control.elections_run") == [(0,)]

    el.run_all(factory)
    assert _outcome(factory, elections_run_reconciliation).result == "pass"
    assert _outcome(factory, elections_seat_overlap).result == "pass"
    # The fixture's county file holds two of Wisconsin's seventy-two counties,
    # so their sum cannot match the state's; Minnesota's two 2018 Senate races
    # are each one county (Hennepin) against the state.
    totals = _outcome(factory, elections_totals_and_geography)
    assert totals.result == "warn" and totals.observed_count == 4

    _write(
        factory,
        """
        UPDATE silver_elections.seat_term SET end_date = DATE '1994-01-01'
         WHERE holder_name = 'Robert Kasten' AND office = 'us_senate'
           AND start_date = DATE '1987-01-06'
        """,
        (),
    )
    overlap = _outcome(factory, elections_seat_overlap)
    assert (overlap.result, overlap.observed_count) == ("fail", 1)

    (run_id,) = _rows(
        factory,
        "SELECT run_id::TEXT FROM gold_elections.current_read WHERE stream = %s",
        (HOUSE,),
    )[0]
    _write(
        factory,
        "DELETE FROM silver_elections.candidate_result WHERE run_id = %s",
        (run_id,),
    )
    lost = _outcome(factory, elections_run_reconciliation)
    assert (lost.result, lost.observed_count) == ("fail", 1)

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_elections_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]


def test_the_harvest_names_every_published_measure(elections_warehouse) -> None:
    """Covers: ETL-111 — three presidential and three Senate measures at state and county."""
    factory = elections_warehouse
    el.run_all(factory)
    published = {
        key: (kind, grains)
        for key, kind, grains in _rows(
            factory,
            """
            SELECT source_object_key, measure_kind, valid_geo_grains
              FROM gold_elections.metric_publisher
            """,
        )
    }
    assert published == {
        "president_two_party_republican_share": (
            "derived_summary",
            ["COUNTY", "STATE"],
        ),
        "president_two_party_margin": ("derived_summary", ["COUNTY", "STATE"]),
        "president_total_votes": ("source_fact", ["COUNTY", "STATE"]),
        "senate_two_party_republican_share": ("derived_summary", ["COUNTY", "STATE"]),
        "senate_two_party_margin": ("derived_summary", ["COUNTY", "STATE"]),
        "senate_total_votes": ("source_fact", ["COUNTY", "STATE"]),
    }
    assert harvest_publisher(factory, Publisher("gold_elections")) > 0
    assert process_pending_events(factory) >= 1
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_glossary.dim_metric WHERE source_code = 'ELECTIONS'",
    ) == [(6,)]


def test_county_history_fills_1990s_counties_and_serves_one_provider(
    elections_warehouse,
) -> None:
    """Covers: ETL-111 — Algara & Amlani serve before 2000 only; Senate counties from 1990."""
    factory = elections_warehouse
    assert _outcome(factory, elections_county_history_agreement).result == (
        "not_applicable"
    )
    _run, status, rows, published = el.run_to_gold(factory, COUNTY_PRESIDENT_HISTORY)
    # 24 lines: 1992 and 1996 counties plus the 2016 rows kept for the check.
    assert (status, rows, published) == ("captured", 24, 1)
    el.run_to_gold(factory, COUNTY_PRESIDENT)
    contests = {
        (geo_id, year): (republican, democratic, total, stream)
        for geo_id, year, republican, democratic, total, stream in _rows(
            factory,
            """
            SELECT geo_id, election_year, republican_votes, democratic_votes, total_votes, stream
              FROM gold_elections.contest_result
             WHERE office = 'president' AND geo_level = 'county'
            """,
        )
    }
    # Dade's 1992 code reads as Miami-Dade; South Boston city adds into Halifax.
    assert contests[(el.MIAMI_DADE, 1992)] == (
        230000, 300000, 560000, COUNTY_PRESIDENT_HISTORY,
    )  # fmt: skip
    assert contests[(el.HALIFAX, 1992)] == (
        7800, 6500, 15600, COUNTY_PRESIDENT_HISTORY,
    )  # fmt: skip
    # Waukesha 1996's two copies disagree: no 1996 Waukesha contest at all.
    assert (el.WAUKESHA, 1996) not in contests
    # 2016 is MEDSL's year: Algara & Amlani's 2016 rows are not served.
    assert contests[(el.DANE, 2016)][3] == COUNTY_PRESIDENT
    releases = dict(
        _rows(
            factory,
            """
            SELECT year, release_key FROM gold_elections.observation_latest
             WHERE metric_key = 'president_two_party_margin' AND geo_id = %s
            """,
            (el.DANE,),
        )
    )
    assert releases[1992] == (
        "Algara & Amlani dataverse_shareable_presidential_county_returns_1868_2020.Rdata"
    )
    assert releases[2016] == "MEDSL countypres_2000-2024.csv"
    # Waukesha's 2016 margin differs from MEDSL's by more than half a point.
    agreement = _outcome(factory, elections_county_history_agreement)
    assert (agreement.result, agreement.observed_count) == ("warn", 1)

    for stream, expected in ((COUNTY_SENATE_HISTORY, 17), (COUNTY_SENATE, 7)):
        _run, status, rows, published = el.run_to_gold(factory, stream)
        assert (status, rows, published) == ("captured", expected, 1), stream
    senate = {
        (geo_id, year): value
        for geo_id, year, value in _rows(
            factory,
            """
            SELECT geo_id, year, value FROM gold_elections.observation_latest
             WHERE metric_key = 'senate_two_party_margin' AND geo_type = 'county'
            """,
        )
    }
    assert senate[(el.DANE, 1992)] == Decimal("-28.21")
    # Minnesota's 2018 special is a contest but not the served measure.
    assert senate[(el.HENNEPIN, 2018)] == Decimal("-38.46")
    # No Democratic nominee: 100 points Republican, never a missing zero.
    assert senate[(el.HALIFAX, 2002)] == Decimal("100.00")
    # MEDSL's 2022 county total is the county's own sum, not the state's.
    assert _rows(
        factory,
        """
        SELECT total_votes FROM gold_elections.contest_result
         WHERE office = 'us_senate' AND geo_id = %s AND election_year = 2022
        """,
        (el.DANE,),
    ) == [(231818 + 68228 + 4 + 678,)]

    _run, status, rows, published = el.run_to_gold(factory, COUNTY_SENATE_HISTORY)
    assert (status, rows, published) == ("unchanged", 0, 0)


def test_house_districts_cover_each_county_with_its_share_from_2006(
    elections_warehouse,
) -> None:
    """Covers: ETL-111 — a county shows each covering House district, its share and the district margin."""
    factory = elections_warehouse
    assert _outcome(factory, elections_house_district_coverage).result == (
        "not_applicable"
    )
    el.run_to_gold(factory, HOUSE)
    run_id, status, rows, published = el.run_to_gold(factory, HOUSE_COUNTY_SHARE)
    # Five county pieces across the two states, every one read.
    assert (status, rows, published) == ("captured", 5, 1)
    # Only the resolved counties are served (Kenosha and Laramie are not seeded).
    pieces = _rows(
        factory,
        """
        SELECT geo_id, election_year, district_label, piece_population, county_population,
               county_population_share, leading_party_code, district_two_party_margin,
               missing_reason
          FROM gold_elections.county_house_district
         ORDER BY geo_id, district
        """,
    )
    assert pieces == [
        (el.DANE, 2022, "WI-02", 561504, 561504, Decimal("100.00"), "DEM",
         Decimal("-45.02"), None),
        (el.WAUKESHA, 2022, "WI-01", 60000, 406000, Decimal("14.78"), None, None,
         "no_district_result"),
        (el.WAUKESHA, 2022, "WI-05", 346000, 406000, Decimal("85.22"), None, None,
         "no_district_result"),
    ]  # fmt: skip
    # The fixture's House file has Wisconsin's second and Wyoming's at-large
    # results only, so the first and fifth have county pieces and no result.
    outcome = _outcome(factory, elections_house_district_coverage)
    assert (outcome.result, outcome.observed_count) == ("warn", 2)

    # Each plan is a frozen census tabulation: a held plan is never read again.
    run_id, status, rows, published = el.run_to_gold(factory, HOUSE_COUNTY_SHARE)
    assert (status, rows, published) == ("unchanged", 0, 0)
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM control.elections_file WHERE run_id = %s",
        (str(run_id),),
    ) == [(0,)]
