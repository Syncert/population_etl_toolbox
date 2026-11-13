"""Governor tenures with taxes and public money (taxes-and-public-money T-6).

Each test replaces the three gold views the fiscal views read with tables of
hand-made rows, inside a transaction that is always rolled back, so the
arithmetic is checked against numbers worked out by hand.

Covers: ETL-119
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from psycopg2.extensions import connection, cursor

pytestmark = [pytest.mark.integration, pytest.mark.database]

WISCONSIN = "state:55"
NATION = "us:1"


def _stub(db: cursor, relation: str) -> str:
    """Point ``relation`` (a view) at an empty table of the same shape."""
    table = "t6_stub." + relation.replace(".", "__")
    db.execute("CREATE SCHEMA IF NOT EXISTS t6_stub")
    db.execute(f"CREATE TABLE {table} (LIKE {relation})")
    db.execute(f"CREATE OR REPLACE VIEW {relation} AS SELECT * FROM {table}")
    return table


def _insert(db: cursor, table: str, rows: list[dict]) -> None:
    for row in rows:
        columns = ", ".join(row)
        marks = ", ".join(["%s"] * len(row))
        db.execute(
            f"INSERT INTO {table} ({columns}) VALUES ({marks})", tuple(row.values())
        )


def _term(key: str, holder: str, party: str, start: date, end: date | None) -> dict:
    return {
        "office": "governor",
        "state_code": "WI",
        "state_fips": "55",
        "geo_id": WISCONSIN,
        "seat_label": "WI governor",
        "term_key": key,
        "holder_id": holder,
        "holder_name": holder.title(),
        "party_code": party,
        "party_text": {"REP": "Republican Party", "DEM": "Democratic Party"}[party],
        "start_date": start,
        "end_date": end,
        "source_label": "Wikidata (CC0 1.0)",
    }


def _seed(db: cursor) -> None:
    seat = _stub(db, "gold_elections.seat_term")
    burden = _stub(db, "gold_census_govfin.tax_burden")
    bea = _stub(db, "gold_bea.observation_revision")
    census = _stub(db, "gold_census_govfin.observation_revision")
    _insert(
        db,
        seat,
        [
            # Two terms of one holder, back to back: one tenure, 2011-2019.
            _term("walker-1", "walker", "REP", date(2011, 1, 3), date(2015, 1, 5)),
            _term("walker-2", "walker", "REP", date(2015, 1, 5), date(2019, 1, 7)),
            # Still serving: "so far", ends on the newest year both publish.
            _term("evers-1", "evers", "DEM", date(2019, 1, 7), None),
            # Under a year in office: no full year inside it.
            _term("short-1", "short", "DEM", date(2010, 3, 1), date(2010, 12, 1)),
        ],
    )
    burden_rows = {
        # Wisconsin 11.0 -> 10.0 over 2010-2018 (-1.0); nation 10.0 -> 9.5 (-0.5).
        (WISCONSIN, 2010): "11.000",
        (WISCONSIN, 2018): "10.000",
        (WISCONSIN, 2023): "9.800",
        (NATION, 2010): "10.000",
        (NATION, 2018): "9.500",
        (NATION, 2023): "9.700",
        # 2024 is Wisconsin's alone: a serving tenure does not end on it.
        (WISCONSIN, 2024): "9.000",
    }
    _insert(
        db,
        burden,
        [
            {
                "dataset_id": "asslgf",
                "government_level": "state_and_local",
                "geo_id": geo_id,
                "fiscal_year": year,
                "taxes_pct_of_personal_income": Decimal(value),
            }
            for (geo_id, year), value in burden_rows.items()
        ],
    )
    bea_rows = []
    # Payments: Wisconsin 18 -> 20 percent over 2010-2018 (+2); nation 17 -> 18 (+1).
    for geo_id, year, payments in (
        (WISCONSIN, 2010, "18"),
        (WISCONSIN, 2018, "20"),
        (NATION, 2010, "17"),
        (NATION, 2018, "18"),
    ):
        bea_rows.append(
            {
                "table_code": "SAINC35",
                "line_code": "1000",
                "geo_type": "nation" if geo_id == NATION else "state",
                "release_date": date(2025, 9, 1),
                "geo_id": geo_id,
                "year": year,
                "value": Decimal(payments),
                "value_status": "valid",
            }
        )
        bea_rows.append(
            {
                "table_code": "SAINC50",
                "line_code": "10",
                "geo_type": "nation" if geo_id == NATION else "state",
                "release_date": date(2025, 9, 1),
                "geo_id": geo_id,
                "year": year,
                "value": Decimal("100"),
                "value_status": "valid",
            }
        )
    _insert(db, bea, bea_rows)
    # Census taxes for Wisconsin's fiscal 2018, in thousands of dollars: the
    # total and its parts. The view takes each figure's newest capture, so an
    # older property figure is ignored.
    newest = datetime(2026, 10, 1, tzinfo=timezone.utc)
    older = datetime(2025, 10, 1, tzinfo=timezone.utc)
    census_rows = [
        (measure, value, newest)
        for measure, value in (
            ("taxes_total", "1000"),
            ("tax_property", "350"),
            ("tax_general_sales", "220"),
            ("tax_individual_income", "280"),
            ("tax_corporate_income", "50"),
            ("tax_selective_sales", "70"),
            ("tax_other", "30"),
        )
    ] + [("tax_property", "999", older)]
    _insert(
        db,
        census,
        [
            {
                "dataset_id": "asslgf",
                "government_level": "state_and_local",
                "geo_id": WISCONSIN,
                "fiscal_year": 2018,
                "quarter": 0,
                "measure_id": measure,
                "value": Decimal(value),
                "value_status": "valid",
                "retrieved_at": retrieved_at,
                "capture_id": str(uuid.uuid4()),
            }
            for measure, value, retrieved_at in census_rows
        ],
    )


def _fiscal(db: cursor, holder: str, measure: str) -> dict:
    db.execute(
        """
        SELECT window_start_year, window_end_year, so_far, term_count, place_change,
               nation_change, net_change, unavailable_reason
        FROM gold_elections.governor_term_fiscal
        WHERE geo_id = %s AND holder_id = %s AND measure = %s
        """,
        (WISCONSIN, holder, measure),
    )
    names = [column.name for column in db.description]
    rows = db.fetchall()
    assert len(rows) == 1, rows
    return dict(zip(names, rows[0]))


def test_a_tenure_is_its_change_net_of_the_nation_one_measure_at_a_time(
    postgres_connection: connection,
) -> None:
    """Covers: ETL-119 — tenure window, net-of-nation points, no figure moved to a nearby year."""
    with postgres_connection.cursor() as db:
        _seed(db)
        walker = _fiscal(db, "walker", "tax_burden")
        assert walker == {
            "window_start_year": 2010,
            "window_end_year": 2018,
            "so_far": False,
            "term_count": 2,
            "place_change": Decimal("-1.000"),
            "nation_change": Decimal("-0.500"),
            "net_change": Decimal("-0.500"),
            "unavailable_reason": None,
        }
        payments = _fiscal(db, "walker", "payments_share")
        assert (
            payments["place_change"],
            payments["nation_change"],
            payments["net_change"],
        ) == (
            Decimal("2.000"),
            Decimal("1.000"),
            Decimal("1.000"),
        )

        # Serving: from 2018 to 2023, the newest year both publish.
        evers = _fiscal(db, "evers", "tax_burden")
        assert (
            evers["window_start_year"],
            evers["window_end_year"],
            evers["so_far"],
        ) == (
            2018,
            2023,
            True,
        )
        # Wisconsin 10.0 -> 9.8 (-0.2) against the nation's 9.5 -> 9.7 (+0.2).
        assert evers["net_change"] == Decimal("-0.400")
        # Payments were not published after 2018: said, never zero.
        evers_pay = _fiscal(db, "evers", "payments_share")
        assert evers_pay["net_change"] is None
        assert evers_pay["unavailable_reason"] == "no_year_published_since_start"

        short = _fiscal(db, "short", "tax_burden")
        assert short["net_change"] is None
        assert short["unavailable_reason"] == "tenure_shorter_than_a_full_year"


def test_a_missing_end_year_is_said_and_the_tax_mix_is_the_window_end(
    postgres_connection: connection,
) -> None:
    """Covers: ETL-119 — unpublished end year named; tax mix grouped in four, last window year."""
    with postgres_connection.cursor() as db:
        _seed(db)
        db.execute(
            "DELETE FROM t6_stub.gold_census_govfin__tax_burden WHERE geo_id = %s AND fiscal_year = 2018",
            (WISCONSIN,),
        )
        walker = _fiscal(db, "walker", "tax_burden")
        assert (
            walker["window_end_year"],
            walker["net_change"],
            walker["unavailable_reason"],
        ) == (
            2018,
            None,
            "end_year_not_published",
        )
        db.execute(
            """
            SELECT tax_group, share_of_taxes_pct
            FROM gold_elections.governor_term_tax_mix
            WHERE geo_id = %s AND tenure_key = 'walker-1'
            ORDER BY tax_group
            """,
            (WISCONSIN,),
        )
        assert db.fetchall() == [
            ("general_sales", Decimal("22.00")),
            ("income", Decimal("33.00")),
            ("other", Decimal("10.00")),
            ("property", Decimal("35.00")),
        ]

        # Census's annual survey has no "other taxes" aggregate: without it,
        # and without selective sales, "other" is still the total less the
        # named groups, so the mix adds to 100 (Alaska read 72 when it was a
        # sum of the published parts).
        db.execute(
            "DELETE FROM t6_stub.gold_census_govfin__observation_revision "
            "WHERE measure_id IN ('tax_other', 'tax_selective_sales')"
        )
        db.execute(
            """
            SELECT tax_group, share_of_taxes_pct
            FROM gold_elections.governor_term_tax_mix
            WHERE geo_id = %s AND tenure_key = 'walker-1' AND tax_group = 'other'
            """,
            (WISCONSIN,),
        )
        assert db.fetchall() == [("other", Decimal("10.00"))]

        # A named group missing leaves no mix at all, never a partial one.
        db.execute(
            "DELETE FROM t6_stub.gold_census_govfin__observation_revision "
            "WHERE measure_id = 'tax_general_sales'"
        )
        db.execute(
            "SELECT COUNT(*) FROM gold_elections.governor_term_tax_mix WHERE tenure_key = 'walker-1'"
        )
        assert db.fetchone()[0] == 0


def test_dq_elec_006_passes_the_seed_and_fails_an_overlap_or_a_broken_mix(
    postgres_connection: connection,
) -> None:
    """Covers: ETL-119 — DQ-ELEC-006 passes the seed and fails a tenure overlap."""
    from data_ingestion_toolbox.quality.sources import (
        elections_governor_fiscal_arithmetic,
    )

    with postgres_connection.cursor() as db:
        _seed(db)
        (outcome,) = elections_governor_fiscal_arithmetic(db, {})
        assert (outcome.result, outcome.observed_count) == ("pass", 0)

        # A second holder starting inside Walker's tenure. The mix's own sum
        # cannot break through the view (other is the remainder), so the rule
        # keeps that arm as a guard and is driven here by the overlap.
        _insert(
            db,
            "t6_stub.gold_elections__seat_term",
            [_term("overlap-1", "overlap", "DEM", date(2016, 6, 1), date(2017, 6, 1))],
        )
        (outcome,) = elections_governor_fiscal_arithmetic(db, {})
        assert outcome.result == "fail"
        assert outcome.observed_count == 1
