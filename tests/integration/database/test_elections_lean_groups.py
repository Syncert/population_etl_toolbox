"""Real PostgreSQL election lean groups (ADR-0014, plan item E-3).

Covers: ETL-112
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection, cursor

from data_ingestion_toolbox.bls_qcew.registry import TOTAL
from data_ingestion_toolbox.census_saipe_sahie.registry import SAIPE
from data_ingestion_toolbox.elections.lean_groups import (
    LEAN_BANDS,
    MEASURES,
    PEER_TYPES,
    lean_band,
    refresh_lean_groups,
)
from data_ingestion_toolbox.elections.registry import COUNTY_PRESIDENT
from data_ingestion_toolbox.quality.sources import elections_lean_group_arithmetic
from data_ingestion_toolbox.elections.schema import (
    REQUIRED_PROCEDURES,
    REQUIRED_RELATIONS,
)
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.support import bea, bls_qcew, census_sae, usda_ers
from tests.support import elections as el
from tests.support.capture_seed import seed_capture, seed_geography

pytestmark = [pytest.mark.integration, pytest.mark.database]

ALPHA = "state:10|county:001"
BETA = "state:10|county:003"
GAMMA = "state:10|county:005"
TIED = "state:24|county:001"
NATION = "us:1"


def _seed_inputs(db: cursor) -> None:
    """Hand-made inputs for one window, 2016 to 2020, in this transaction."""
    db.execute(
        """
        CREATE TEMP TABLE lean_vote (geo_id TEXT, year INTEGER, margin NUMERIC) ON COMMIT DROP;
        CREATE TEMP TABLE lean_pce (year INTEGER, price_level NUMERIC) ON COMMIT DROP;
        CREATE TEMP TABLE lean_input (
            measure TEXT, geo_id TEXT, year INTEGER, value NUMERIC, deflate BOOLEAN
        ) ON COMMIT DROP;
        CREATE TEMP TABLE lean_peer (
            geo_id TEXT, rural_urban_group TEXT, industry_group TEXT, industry_label TEXT
        ) ON COMMIT DROP;
        CREATE TEMP TABLE lean_state_name (state_fips TEXT, state_name TEXT) ON COMMIT DROP;
        """
    )
    db.executemany(
        "INSERT INTO lean_vote VALUES (%s, %s, %s)",
        [
            (ALPHA, 2016, Decimal("25.00")),
            (BETA, 2016, Decimal("10.00")),
            (GAMMA, 2016, Decimal("-30.00")),
            (TIED, 2016, Decimal("0.00")),
            # Later votes only open the window; they never set its lean.
            (ALPHA, 2020, Decimal("-40.00")),
            (BETA, 2020, Decimal("-40.00")),
            (GAMMA, 2020, Decimal("40.00")),
            (TIED, 2020, Decimal("5.00")),
        ],
    )
    db.executemany(
        "INSERT INTO lean_pce VALUES (%s, %s)",
        [(2016, Decimal("100")), (2020, Decimal("110"))],
    )
    rows = [
        # Income: deflated to 2020 prices. Nation 50,000 -> 55,000 real; 60,000 is +9.0909%.
        ("real_income_per_person", NATION, 2016, "50000", True),
        ("real_income_per_person", NATION, 2020, "60000", True),
        (
            "real_income_per_person",
            ALPHA,
            2016,
            "40000",
            True,
        ),  # 44,000 -> 48,400: +10%
        ("real_income_per_person", ALPHA, 2020, "48400", True),
        ("real_income_per_person", BETA, 2016, "30000", True),  # 33,000 -> 33,000: 0%
        ("real_income_per_person", BETA, 2020, "33000", True),
        (
            "real_income_per_person",
            GAMMA,
            2016,
            "60000",
            True,
        ),  # 66,000 -> 79,200: +20%
        ("real_income_per_person", GAMMA, 2020, "79200", True),
        ("real_income_per_person", TIED, 2016, "50000", True),
        ("real_income_per_person", TIED, 2020, "70000", True),
        # Poverty: points. Nation -3; the counties -5, -1, -3.
        ("poverty_rate", NATION, 2016, "14.0", False),
        ("poverty_rate", NATION, 2020, "11.0", False),
        ("poverty_rate", ALPHA, 2016, "20.0", False),
        ("poverty_rate", ALPHA, 2020, "15.0", False),
        ("poverty_rate", BETA, 2016, "12.0", False),
        ("poverty_rate", BETA, 2020, "11.0", False),
        ("poverty_rate", GAMMA, 2016, "9.0", False),
        ("poverty_rate", GAMMA, 2020, "6.0", False),
        # Jobs: a county missing its last year is left out, not zero.
        ("jobs", NATION, 2016, "1000", False),
        ("jobs", NATION, 2020, "1100", False),
        ("jobs", ALPHA, 2016, "100", False),
        ("jobs", ALPHA, 2020, "120", False),
        ("jobs", BETA, 2016, "100", False),
    ]
    db.executemany(
        "INSERT INTO lean_input VALUES (%s, %s, %s, %s::NUMERIC, %s)",
        [(m, g, y, v, d) for m, g, y, v, d in rows],
    )
    db.execute(
        "INSERT INTO lean_peer VALUES (%s, 'large_metro', '3', 'Manufacturing'), "
        "(%s, 'nonmetro_remote', NULL, NULL)",
        (ALPHA, GAMMA),
    )
    db.execute("INSERT INTO lean_state_name VALUES ('10', 'Delaware')")


def _place(db: cursor, measure: str, geo_id: str) -> tuple | None:
    db.execute(
        """
        SELECT lean_band, lean_margin, start_value, end_value, place_change, nation_change,
               net_change, rural_urban_group, starting_level_group, industry_group
        FROM gold_elections.lean_place_change
        WHERE window_start_year = 2016 AND window_end_year = 2020
          AND measure = %s AND geo_id = %s
        """,
        (measure, geo_id),
    )
    return db.fetchone()


def test_lean_groups_are_change_net_of_the_nation_by_window_start_lean(
    postgres_connection: connection,
) -> None:
    """Covers: ETL-112 — net-of-nation arithmetic, start-of-window lean, deflation, ties and gaps."""
    with postgres_connection.cursor() as db:
        _seed_inputs(db)
        db.execute("SELECT gold_elections.build_lean_groups(0)")
        assert (
            db.fetchone()[0] == 7
        )  # 3 income + 3 poverty + 1 jobs; the tie is in no band

        # The lean is the 2016 vote, not 2020's; income is in 2020 prices.
        alpha = _place(db, "real_income_per_person", ALPHA)
        assert alpha[:2] == ("R20_plus", Decimal("25.00"))
        assert alpha[2:7] == (
            Decimal("44000.0000"),
            Decimal("48400.0000"),
            Decimal("10.0000"),
            Decimal("9.0909"),
            Decimal("0.9091"),
        )
        assert alpha[7:] == ("large_metro", "2", "3")
        assert _place(db, "real_income_per_person", BETA)[0] == "R0_20"
        gamma = _place(db, "real_income_per_person", GAMMA)
        assert (gamma[0], gamma[6], gamma[8]) == ("D20_plus", Decimal("10.9091"), "3")

        # Rates change in points, net of the nation's points.
        poverty = {g: _place(db, "poverty_rate", g)[4:7] for g in (ALPHA, BETA, GAMMA)}
        assert poverty == {
            ALPHA: (Decimal("-5.0000"), Decimal("-3.0000"), Decimal("-2.0000")),
            BETA: (Decimal("-1.0000"), Decimal("-3.0000"), Decimal("2.0000")),
            GAMMA: (Decimal("-3.0000"), Decimal("-3.0000"), Decimal("0.0000")),
        }
        assert _place(db, "jobs", BETA) is None
        assert _place(db, "jobs", ALPHA)[6] == Decimal("10.0000")  # +20% against +10%
        assert _place(db, "real_income_per_person", TIED) is None

        db.execute(
            """
            SELECT peer_type, peer_group, peer_group_label, lean_band, place_count,
                   net_change_p25, net_change_median, net_change_p75, nation_change
            FROM gold_elections.lean_group_change
            WHERE measure = 'real_income_per_person'
            ORDER BY peer_type, peer_group, lean_band
            """
        )
        groups = db.fetchall()
        assert (
            "all",
            "all",
            "All counties",
            "R20_plus",
            1,
            Decimal("0.9091"),
            Decimal("0.9091"),
            Decimal("0.9091"),
            Decimal("9.0909"),
        ) in groups
        assert (
            "state",
            "10",
            "Delaware",
            "D20_plus",
            1,
            Decimal("10.9091"),
            Decimal("10.9091"),
            Decimal("10.9091"),
            Decimal("9.0909"),
        ) in groups
        assert {(g[0], g[1], g[2]) for g in groups if g[0] == "rural_urban"} == {
            ("rural_urban", "large_metro", "Large metro"),
            ("rural_urban", "nonmetro_remote", "Remote nonmetro"),
        }
        assert {(g[1], g[2]) for g in groups if g[0] == "industry"} == {
            ("3", "Manufacturing")
        }
        assert {g[0] for g in groups} == set(PEER_TYPES)

        # Without a full year of PCE at either end, income is not compared at all.
        db.execute("DELETE FROM lean_pce WHERE year = 2020")
        db.execute("SELECT gold_elections.build_lean_groups(0)")
        assert db.fetchone()[0] == 4
        assert _place(db, "real_income_per_person", ALPHA) is None


def test_a_group_shows_its_middle_and_middle_half(
    postgres_connection: connection,
) -> None:
    """Covers: ETL-112 — a lean group is its median and quartiles with a count (ADR-0014 rule 5)."""
    with postgres_connection.cursor() as db:
        _seed_inputs(db)
        db.execute("DELETE FROM lean_input WHERE measure <> 'poverty_rate'")
        # Four more Republican-leaning Delaware-like counties, poverty change -3 .. +3.
        for index, change in enumerate(("-3", "-1", "1", "3")):
            geo_id = f"state:11|county:{index + 101:03d}"
            db.execute("INSERT INTO lean_vote VALUES (%s, 2016, 30)", (geo_id,))
            db.execute(
                "INSERT INTO lean_input VALUES ('poverty_rate', %s, 2016, 10, FALSE), "
                "('poverty_rate', %s, 2020, 10 + %s::NUMERIC, FALSE)",
                (geo_id, geo_id, change),
            )
        db.execute("SELECT gold_elections.build_lean_groups(0)")
        db.execute(
            """
            SELECT place_count, net_change_p25, net_change_median, net_change_p75
            FROM gold_elections.lean_group_change
            WHERE measure = 'poverty_rate' AND peer_type = 'all' AND lean_band = 'R20_plus'
            """
        )
        # ALPHA (-2 net) and the four (0, 2, 4, 6 net of the nation's -3).
        assert db.fetchone() == (
            5,
            Decimal("0.0000"),
            Decimal("2.0000"),
            Decimal("4.0000"),
        )
        db.execute(
            """
            SELECT lean_band, starting_level_group
            FROM gold_elections.lean_place_change
            WHERE measure = 'poverty_rate' AND geo_id IN ('state:11|county:101', %s)
            ORDER BY geo_id
            """,
            (ALPHA,),
        )
        # Seven counties by starting poverty: the lowest two, then two, two, one.
        assert db.fetchall() == [("R20_plus", "4"), ("R20_plus", "1")]


def test_refresh_rebuilds_only_when_an_input_published(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ETL-112 — an unchanged fingerprint records `unchanged` and rewrites nothing."""
    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as db:
            db.execute("SELECT * FROM gold_elections.refresh_lean_groups(TRUE)")
            assert db.fetchone()[1] == "rebuilt"
            db.execute("SELECT * FROM gold_elections.refresh_lean_groups()")
            refresh_id, status, place_rows, group_rows = db.fetchone()
            assert (status, place_rows, group_rows) == ("unchanged", None, None)
            # A new publication of any input changes the fingerprint.
            db.execute(
                "UPDATE control.elections_run SET published_at = published_at + INTERVAL '1 second' "
                "WHERE status = 'published'"
            )
            db.execute(
                "SELECT COUNT(*) FROM control.elections_run WHERE status = 'published'"
            )
            if db.fetchone()[0]:
                db.execute("SELECT status FROM gold_elections.refresh_lean_groups()")
                assert db.fetchone()[0] == "rebuilt"
            db.execute("SELECT * FROM gold_elections.refresh_lean_groups()")
            assert db.fetchone()[1] == "unchanged"
    finally:
        database_connection.rollback()
        database_connection.close()


def test_the_schema_component_declares_the_lean_relations_and_procedures(
    postgres_connection: connection,
) -> None:
    """Covers: ETL-112 — the ensure task checks every lean relation and function."""
    with postgres_connection.cursor() as db:
        for relation in REQUIRED_RELATIONS:
            db.execute("SELECT to_regclass(%s)", (relation,))
            assert db.fetchone()[0] is not None, relation
        for procedure in REQUIRED_PROCEDURES:
            db.execute("SELECT to_regprocedure(%s)", (procedure,))
            assert db.fetchone()[0] is not None, procedure
        db.execute(
            "SELECT measure FROM gold_elections.lean_measure_definition ORDER BY display_order"
        )
        assert tuple(row[0] for row in db.fetchall()) == MEASURES
        db.execute(
            "SELECT lean_band FROM gold_elections.lean_band_definition ORDER BY display_order"
        )
        assert tuple(row[0] for row in db.fetchall()) == LEAN_BANDS
        db.execute(
            "SELECT peer_type FROM gold_elections.lean_peer_definition ORDER BY display_order"
        )
        assert tuple(row[0] for row in db.fetchall()) == PEER_TYPES
        # Only the warehouse owner rebuilds; the serving role's read grant is
        # schema-wide (`001_api_readonly.sql`, DB-050).
        for procedure in REQUIRED_PROCEDURES[:2] + REQUIRED_PROCEDURES[3:]:
            db.execute(
                "SELECT has_function_privilege('public', %s, 'EXECUTE')", (procedure,)
            )
            assert db.fetchone()[0] is False, procedure


def test_python_band_rule_matches_the_rebuild_at_every_boundary(
    postgres_connection: connection,
) -> None:
    """Covers: ETL-112 — `lean_band` and `build_lean_groups` agree at every boundary."""
    margins = ("-35", "-20", "-19.99", "-0.01", "0.01", "19.99", "20", "35")
    with postgres_connection.cursor() as db:
        _seed_inputs(db)
        db.execute("DELETE FROM lean_vote WHERE year = 2016")
        db.execute("DELETE FROM lean_input")
        db.execute(
            "INSERT INTO lean_input VALUES ('poverty_rate', 'us:1', 2016, 10, FALSE), "
            "('poverty_rate', 'us:1', 2020, 10, FALSE)"
        )
        for index, margin in enumerate(margins):
            geo_id = f"state:12|county:{index + 1:03d}"
            db.execute(
                "INSERT INTO lean_vote VALUES (%s, 2016, %s::NUMERIC)", (geo_id, margin)
            )
            db.execute(
                "INSERT INTO lean_input VALUES ('poverty_rate', %s, 2016, 10, FALSE), "
                "('poverty_rate', %s, 2020, 11, FALSE)",
                (geo_id, geo_id),
            )
        db.execute("SELECT gold_elections.build_lean_groups(0)")
        db.execute(
            "SELECT lean_margin, lean_band FROM gold_elections.lean_place_change "
            "WHERE measure = 'poverty_rate'"
        )
        built = {str(margin.normalize()): band for margin, band in db.fetchall()}
    assert built == {
        str(Decimal(margin).normalize()): lean_band(Decimal(margin))
        for margin in margins
    }
    assert lean_band(0) is None


@pytest.fixture
def source_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    factory = postgres_connection_factory
    for support in (bea, bls_qcew, census_sae, usda_ers, el):
        support.reviewed_warehouse(factory, request)
    bea.run_to_gold(factory, "CAINC1")
    bea.run_to_gold(factory, "CAGDP1")
    bls_qcew.run_to_gold(factory, 2023, "a", (TOTAL,))
    census_sae.run_to_gold(factory, SAIPE)
    usda_ers.run_all(factory)
    el.run_to_gold(factory, COUNTY_PRESIDENT)
    return factory


def test_inputs_are_read_from_each_source_by_their_published_keys(
    source_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-112 — each measure, the lean and the peers come from the named gold relation."""
    database_connection = source_warehouse()
    try:
        with database_connection.cursor() as db:
            # PCEPI and LAUS have no fixture pipeline here; seeded in this
            # transaction only.
            db.execute(
                "INSERT INTO gold_fred.dim_fred_series (series_id, frequency) VALUES ('PCEPI', 'Monthly') "
                "ON CONFLICT (series_id) DO NOTHING"
            )
            capture_id = seed_capture(db, "FRED")
            for month in range(1, 13):
                day = f"2097-{month:02d}-01"
                _seed_time(db, int(day.replace("-", "")), day)
                db.execute(
                    """
                    INSERT INTO silver_fred.fact_economic_indicators (
                        time_sk, duration_start, duration_end, observation_date, series_id,
                        value, capture_id, load_batch_id
                    ) VALUES (%s, %s, %s, %s, 'PCEPI', %s, %s, gen_random_uuid())
                    """,
                    (int(day.replace("-", "")), day, day, day, 100 + month, capture_id),
                )
            # Eleven months of the next year: no annual level for it.
            for month in range(1, 12):
                day = f"2098-{month:02d}-01"
                _seed_time(db, int(day.replace("-", "")), day)
                db.execute(
                    """
                    INSERT INTO silver_fred.fact_economic_indicators (
                        time_sk, duration_start, duration_end, observation_date, series_id,
                        value, capture_id, load_batch_id
                    ) VALUES (%s, %s, %s, %s, 'PCEPI', 120, %s, gen_random_uuid())
                    """,
                    (int(day.replace("-", "")), day, day, day, capture_id),
                )
            _seed_time(db, 20970101, "2097-01-01")
            county_sk = seed_geography(
                db,
                geo_type="county",
                state_fips="98",
                county_fips="001",
                vintage=2097,
                name="Lean Test County",
            )
            db.execute(
                """
                INSERT INTO silver_bls.fact_labor_statistics (
                    time_sk, geo_sk, duration_start, duration_end, period_date, series_id,
                    program, geo_level, geo_id, value, year, period, load_batch_id
                ) VALUES (20970101, %s, '2097-01-01', '2097-12-31', '2097-12-31',
                          'LAUCN980010000000003', 'la', 'county', 'state:98|county:001',
                          4.5, 2097, 'M13', gen_random_uuid())
                """,
                (county_sk,),
            )

            db.execute("SELECT gold_elections.load_lean_inputs()")
            assert db.fetchone()[0] > 0
            db.execute(
                "SELECT measure, COUNT(*) FILTER (WHERE geo_id = 'us:1'), "
                "COUNT(*) FILTER (WHERE geo_id LIKE 'state:%%|county:%%'), BOOL_AND(deflate) "
                "FROM lean_input GROUP BY measure ORDER BY measure"
            )
            by_measure = {row[0]: row[1:] for row in db.fetchall()}
            assert set(by_measure) == set(MEASURES)
            for measure in (
                "real_income_per_person",
                "real_gdp",
                "jobs",
                "poverty_rate",
            ):
                nation, counties, _deflate = by_measure[measure]
                assert nation > 0 and counties > 0, measure
            assert by_measure["real_income_per_person"][2] is True
            assert by_measure["real_gdp"][2] is False
            # LAUS: the county's published annual rate; no nation without all 51 states.
            assert by_measure["unemployment_rate"] == (0, 1, False)
            db.execute(
                "SELECT year, ROUND(price_level, 2) FROM lean_pce WHERE year >= 2097"
            )
            assert db.fetchall() == [(2097, Decimal("106.50"))]
            db.execute(
                "SELECT margin FROM lean_vote WHERE geo_id = 'state:55|county:025' AND year = 2016"
            )
            assert db.fetchone() == (Decimal("-50.67"),)
            db.execute(
                "SELECT COUNT(*) FROM lean_peer WHERE rural_urban_group IS NOT NULL"
            )
            assert db.fetchone()[0] > 0
    finally:
        database_connection.rollback()
        database_connection.close()


def test_refresh_through_python_commits_and_reports(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ETL-112 — the DAG's entry point rebuilds, commits and reports the refresh row."""
    outcome = refresh_lean_groups(postgres_connection_factory, force=True)
    assert outcome["status"] == "rebuilt"
    assert refresh_lean_groups(postgres_connection_factory)["status"] == "unchanged"
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as db:
            db.execute(
                "SELECT status FROM gold_elections.lean_refresh WHERE refresh_id >= %s "
                "ORDER BY refresh_id",
                (outcome["refresh_id"],),
            )
            assert [row[0] for row in db.fetchall()] == ["rebuilt", "unchanged"]
            db.execute("DELETE FROM gold_elections.lean_group_change")
            db.execute("DELETE FROM gold_elections.lean_place_change")
            db.execute(
                "DELETE FROM gold_elections.lean_refresh WHERE refresh_id >= %s",
                (outcome["refresh_id"],),
            )
        reader.commit()
    finally:
        reader.close()


def test_dq_elec_005_passes_built_groups_and_fails_a_tampered_row(
    postgres_connection: connection,
) -> None:
    """Covers: ETL-112 — DQ-ELEC-005 checks net-of-nation arithmetic, bands and group counts."""
    with postgres_connection.cursor() as db:
        _seed_inputs(db)
        db.execute("SELECT gold_elections.build_lean_groups(0)")
        assert [o.result for o in elections_lean_group_arithmetic(db, {})] == ["pass"]
        db.execute(
            "UPDATE gold_elections.lean_place_change SET lean_band = 'R0_20' "
            "WHERE geo_id = %s AND measure = 'poverty_rate'",
            (ALPHA,),
        )
        outcome = elections_lean_group_arithmetic(db, {})[0]
        # The banding is wrong, and the all-county groups no longer add up.
        assert (outcome.result, outcome.observed_count) == ("fail", 3)
