"""Real PostgreSQL Census government finance capture-to-gold contract.

Covers: ETL-114
"""

from __future__ import annotations

import json
from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_govfin import plan
from data_ingestion_toolbox.census_govfin.registry import MEASURES
from data_ingestion_toolbox.census_govfin.schema import (
    REQUIRED_RELATIONS,
    ensure_census_govfin_schema,
)
from data_ingestion_toolbox.quality.sources import (
    govfin_slice_reconciliation,
    govfin_unit_totals,
)
from tests.support import bea, census_govfin

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def govfin_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return census_govfin.reviewed_warehouse(postgres_connection_factory, request)


def _rows(
    factory: Callable[[], connection], sql: str, parameters: tuple = ()
) -> list[tuple]:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


def _load_units(factory: Callable[[], connection]) -> tuple[int, int]:
    _run, facts, published = census_govfin.run_to_gold(
        factory,
        "cog_units",
        census_govfin.UNIT_YEAR,
        census_govfin.FixtureClient(census_govfin.unit_archives()),
    )
    return facts, published


def test_unit_files_roll_up_to_county_areas_and_states(
    govfin_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-114 -- a county area is the sum of its home governments' unit records."""
    facts, published = _load_units(govfin_warehouse)
    # Dane, Milwaukee; Wisconsin's local governments; Wisconsin's and
    # Alabama's state governments (the real Alabama lines carry only debt
    # issued and retired, which no measure counts, so they read zero).
    assert facts == 5 * len(MEASURES)
    assert published == 1

    dane = dict(
        _rows(
            govfin_warehouse,
            """
            SELECT measure_id, value FROM gold_census_govfin.observation_latest
            WHERE geo_id = 'state:55|county:025' AND fiscal_year = 2022
            """,
        )
    )
    assert dane["taxes_total"] == Decimal(census_govfin.DANE_TAXES_TOTAL)
    assert dane["tax_property"] == Decimal(700_000)
    assert dane["tax_general_sales"] == Decimal(45_000)
    assert dane["tax_selective_sales"] == Decimal(1_500)
    # No individual income tax record: Census writes none for a zero.
    assert dane["tax_individual_income"] == Decimal(0)
    # A61 is a charge; A91 is water utility revenue and is not.
    assert dane["current_charges"] == Decimal(20_000)
    assert dane["own_source_general_revenue"] == Decimal(774_900)
    assert dane["intergovernmental_state"] == Decimal(192_000)
    assert dane["intergovernmental_total"] == Decimal(224_500)
    assert dane["debt_outstanding"] == Decimal(85_000)

    # The acceptance check: the county area equals the sum of its
    # governments' unit-file amounts, computed independently here.
    independent = _rows(
        govfin_warehouse,
        """
        SELECT SUM(amount.amount)
        FROM silver_census_govfin.unit_amount AS amount
        JOIN gold_census_govfin.county_area_government AS government
          ON government.government_code = amount.government_code
         AND government.census_year = amount.census_year
        WHERE government.geo_id = 'state:55|county:025' AND amount.item_code LIKE 'T%%'
        """,
    )
    assert independent == [(Decimal(census_govfin.DANE_TAXES_TOTAL),)]

    shape = _rows(
        govfin_warehouse,
        """
        SELECT geo_id, government_level, derivation, government_count, geo_level,
               period_label, value
        FROM gold_census_govfin.observation_latest
        WHERE measure_id = 'taxes_total'
        ORDER BY geo_id, government_level
        """,
    )
    assert shape == [
        ("state:01", "state", "state_government_units", 1, "STATE",
         "fiscal year ending in 2022", Decimal(0)),
        ("state:55", "local", "state_local_rollup", 5, "STATE",
         "fiscal year ending in 2022", Decimal(842_200)),
        ("state:55", "state", "state_government_units", 1, "STATE",
         "fiscal year ending in 2022", Decimal(9_423_456)),
        ("state:55|county:025", "local", "county_area_rollup", 3, "COUNTY",
         "fiscal year ending in 2022", Decimal(746_500)),
        ("state:55|county:079", "local", "county_area_rollup", 1, "COUNTY",
         "fiscal year ending in 2022", Decimal(95_000)),
    ]  # fmt: skip

    members = _rows(
        govfin_warehouse,
        """
        SELECT government_name, government_type FROM gold_census_govfin.county_area_government
        WHERE geo_id = 'state:55|county:025' ORDER BY government_code
        """,
    )
    assert members == [
        ("DANE COUNTY", "county"),
        ("MADISON CITY", "municipal"),
        ("MADISON METROPOLITAN SCHOOL DISTRICT", "school_district"),
    ]

    quarantined = _rows(
        govfin_warehouse,
        "SELECT error_code FROM silver_census_govfin.quarantine ORDER BY error_code",
    )
    assert quarantined == [("short_record",), ("unreadable_government",)]

    mix = dict(
        _rows(
            govfin_warehouse,
            """
            SELECT measure_id, share_of_taxes_pct FROM gold_census_govfin.tax_mix
            WHERE geo_id = 'state:55|county:025' AND dataset_id = 'cog_units'
            """,
        )
    )
    assert mix["tax_property"] == Decimal("93.77")
    assert mix["tax_individual_income"] == Decimal("0.00")


def test_tax_burden_divides_by_bea_personal_income_of_the_same_year(
    govfin_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-114 -- the county burden is its local taxes over BEA income, labelled derived."""
    _load_units(govfin_warehouse)
    burden = _rows(
        govfin_warehouse,
        """
        SELECT geo_id, taxes_thousands, unavailable_reason, derived
        FROM gold_census_govfin.tax_burden ORDER BY geo_id
        """,
    )
    # No BEA rows in this warehouse: the burden says why it is absent.
    assert burden == [
        ("state:55|county:025", Decimal(746_500), "denominator_missing", True),
        ("state:55|county:079", Decimal(95_000), "denominator_missing", True),
    ]


def test_a_loaded_edition_is_answered_and_a_rerun_plans_nothing(
    govfin_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-114 -- the loader rule: an edition never loaded is planned until it is published."""
    reader = govfin_warehouse()
    try:
        assert plan.answered(reader, "cog_units") == set()
    finally:
        reader.close()
    assert plan.periods_to_capture("cog_units", {}, set()) == [2002, 2012, 2017, 2022]

    _load_units(govfin_warehouse)
    reader = govfin_warehouse()
    try:
        done = plan.answered(reader, "cog_units")
    finally:
        reader.close()
    assert done == {2022}
    # The other editions are still unanswered, so the next scheduled run
    # asks for them and not for 2022 again.
    assert plan.periods_to_capture("cog_units", {}, done) == [2002, 2012, 2017]
    assert plan.periods_to_capture("cog_units", {"history": True}, done) == [
        2002, 2012, 2017, 2022,
    ]  # fmt: skip


def test_publisher_harvests_and_the_schema_task_is_idempotent(
    govfin_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-114 -- one publisher row per served measure and level; the DDL reapplies."""

    class _Hook:
        def get_conn(self) -> connection:
            return govfin_warehouse()

    ensure_census_govfin_schema(_Hook())
    ensure_census_govfin_schema(_Hook())
    for relation in REQUIRED_RELATIONS:
        schema, name = relation.split(".")
        assert _rows(
            govfin_warehouse,
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
            (schema, name),
        ), relation

    _load_units(govfin_warehouse)
    published = _rows(
        govfin_warehouse,
        """
        SELECT source_object_key, valid_geo_grains, valid_time_grains, measure_kind
        FROM gold_census_govfin.metric_publisher
        WHERE source_object_key LIKE 'cog_units:taxes_total:%%'
        ORDER BY source_object_key
        """,
    )
    assert published == [
        ("cog_units:taxes_total:local", ["COUNTY", "STATE"], ["ANNUAL"], "source_fact"),
        ("cog_units:taxes_total:state", ["STATE"], ["ANNUAL"], "source_fact"),
    ]


def _fixture_value(name: str, **match: str) -> Decimal:
    rows = json.loads((census_govfin.FIXTURE_DIR / name).read_bytes())
    header = rows[0]
    hits = [
        row for row in rows[1:]
        if all(row[header.index(key)] == value for key, value in match.items())
    ]  # fmt: skip
    assert len(hits) == 1, match
    return Decimal(
        hits[0][header.index("AMOUNT" if "AMOUNT" in header else "CELL_VALUE")]
    )


def test_annual_survey_and_qtax_replay_census_published_totals(
    govfin_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-114 -- Census's own state and national figures reach gold unchanged."""
    client = census_govfin.FixtureClient(census_govfin.api_responses())
    _run, facts, published = census_govfin.run_to_gold(
        govfin_warehouse, "asslgf", 2022, client
    )
    assert published == 2
    # 14 registered aggregates (tax_other has none) at three levels for the
    # nation and Wisconsin, less LF0005 and LF0006 where a level has none.
    served = dict(
        _rows(
            govfin_warehouse,
            """
            SELECT geo_id || ' ' || government_level, value
            FROM gold_census_govfin.observation_latest
            WHERE dataset_id = 'asslgf' AND measure_id = 'taxes_total'
            """,
        )
    )
    assert served["state:55 state_and_local"] == _fixture_value(
        "asslgf_2022_state.json", AGG_DESC="LF0008", GOVTYPE="001"
    )
    assert served["state:55 state"] == Decimal(23_363_814)
    assert served["us:1 local"] == _fixture_value(
        "asslgf_2022_us.json", AGG_DESC="LF0008", GOVTYPE="003"
    )
    assert facts == len(
        _rows(
            govfin_warehouse,
            "SELECT 1 FROM silver_census_govfin.api_revision WHERE dataset_id = 'asslgf'",
        )
    )

    _run, _facts, published = census_govfin.run_to_gold(
        govfin_warehouse, "qtax", 2024, client
    )
    assert published == 2
    quarters = _rows(
        govfin_warehouse,
        """
        SELECT period_label, period_start::TEXT, value, unit
        FROM gold_census_govfin.observation_latest
        WHERE dataset_id = 'qtax' AND measure_id = 'taxes_total' AND geo_id = 'state:55'
        ORDER BY quarter
        """,
    )
    assert [row[0] for row in quarters] == ["2024 Q1", "2024 Q2", "2024 Q3", "2024 Q4"]
    assert quarters[0][1:] == ("2024-01-01", Decimal(5221), "millions of dollars")


def test_state_and_local_tax_burden_matches_census_over_bea(
    govfin_warehouse: Callable[[], connection],
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> None:
    """Covers: ETL-114 -- the nation's fiscal 2022 burden is Census's taxes over BEA's income."""
    bea_warehouse = bea.reviewed_warehouse(postgres_connection_factory, request)
    bea.run_to_gold(bea_warehouse, "CAINC1")
    client = census_govfin.FixtureClient(census_govfin.api_responses())
    census_govfin.run_to_gold(govfin_warehouse, "asslgf", 2022, client)

    taxes = _fixture_value("asslgf_2022_us.json", AGG_DESC="LF0008", GOVTYPE="001")
    ((income, population),) = _rows(
        govfin_warehouse,
        """
        SELECT MAX(value) FILTER (WHERE metric_key = 'CAINC1:1'),
               MAX(value) FILTER (WHERE metric_key = 'CAINC1:2')
        FROM gold_bea.observation_latest WHERE geo_id = 'us:1' AND year = 2022
        """,
    )
    burden = _rows(
        govfin_warehouse,
        """
        SELECT geo_id, taxes_pct_of_personal_income, taxes_per_person_dollars,
               unavailable_reason, derived
        FROM gold_census_govfin.tax_burden
        WHERE dataset_id = 'asslgf' AND fiscal_year = 2022 ORDER BY geo_id
        """,
    )
    assert burden == [
        # The fixture BEA file holds no Wisconsin row: the page says why.
        ("state:55", None, None, "denominator_missing", True),
        (
            "us:1",
            round(taxes / income * 100, 3),
            round(taxes * 1000 / population, 2),
            None,
            True,
        ),
    ]
    # A sanity bound, not a fixture echo: US state and local taxes ran near
    # a tenth of personal income in fiscal 2022.
    assert Decimal(8) < burden[1][1] < Decimal(13)


def _outcome(factory: Callable[[], connection], executor):  # noqa: ANN001, ANN202
    reader = factory()
    try:
        with reader.cursor() as cursor:
            return executor(cursor, {})[0]
    finally:
        reader.close()


def _write(factory: Callable[[], connection], sql: str, parameters: tuple = ()) -> None:
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(sql, parameters)
        writer.commit()
    finally:
        writer.close()


def test_quality_rules_pass_on_the_fixture_then_catch_a_fault(
    govfin_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-114 -- DQ-GOVFIN-003 and -004 pass on a clean load, then fail and warn."""
    factory = govfin_warehouse
    assert _outcome(factory, govfin_slice_reconciliation).result == "not_applicable"
    assert _outcome(factory, govfin_unit_totals).result == "not_applicable"

    _load_units(factory)
    assert _outcome(factory, govfin_slice_reconciliation).result == "pass"
    # The fixture's Wisconsin lines sum, item by item, to its state totals.
    assert _outcome(factory, govfin_unit_totals).result == "pass"

    # Madison's property tax read 10 percent high no longer matches Census's
    # local total for Wisconsin.
    _write(
        factory,
        """
        UPDATE silver_census_govfin.unit_amount SET amount = amount * 1.1
         WHERE government_code = '552025000002' AND item_code = 'T01'
        """,
    )
    totals = _outcome(factory, govfin_unit_totals)
    assert (totals.result, totals.observed_count) == ("warn", 1)

    # A slice captured and never replayed is a lost load.
    _write(
        factory,
        "UPDATE control.census_govfin_slice SET status = 'captured'",
    )
    lost = _outcome(factory, govfin_slice_reconciliation)
    assert (lost.result, lost.observed_count) == ("fail", 1)
