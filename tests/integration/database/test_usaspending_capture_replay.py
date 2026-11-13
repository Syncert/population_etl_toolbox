"""Real PostgreSQL USAspending place-of-performance capture-to-gold contract.

Covers: ETL-132
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from data_ingestion_toolbox.quality.sources import (
    usaspending_group_total,
    usaspending_request_reconciliation,
)
from data_ingestion_toolbox.usaspending.capture import capture_awards
from data_ingestion_toolbox.usaspending.client import UsaspendingFetchError
from data_ingestion_toolbox.usaspending.config import UsaspendingConfig
from data_ingestion_toolbox.usaspending.registry import (
    GEOGRAPHY_PATH,
    fiscal_year_period,
)
from data_ingestion_toolbox.usaspending.schema import (
    REQUIRED_RELATIONS,
    ensure_usaspending_schema,
)
from data_ingestion_toolbox.usaspending.silver_usaspending.load import (
    settle_unreplayed_runs,
)
from tests.support import usaspending as usa

pytestmark = [pytest.mark.integration, pytest.mark.database]

#: Fiscal 2025 (nothing in the fixtures): the agency page and 2 x 6 reads.
#: Fiscal 2024 with three agencies: the agency page and 2 x 9 reads.
ORDINARY_RUN_REQUESTS = 13 + 19
#: Three agencies, 57 states and 72 Wisconsin counties.
ORDINARY_RUN_ROWS = 3 + 57 + 72


@pytest.fixture
def usaspending_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return usa.reviewed_warehouse(postgres_connection_factory, request)


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


def _outcomes(factory: Callable[[], connection], executor):  # noqa: ANN001, ANN202
    reader = factory()
    try:
        with reader.cursor() as cursor:
            return executor(cursor, {})
    finally:
        reader.close()


def _restating(edit: Callable[[list[dict[str, Any]]], None]) -> usa.FixtureClient:
    """A client whose fiscal 2024 all-award state answer is ``edit``-ed."""
    reviewed = json.loads(usa.STATE_ALL.read_bytes())
    edit(reviewed["results"])
    restated = json.dumps(reviewed).encode()
    fallback = usa.FixtureClient(agency_limit=3)

    def answer(path: str, body: dict[str, Any]) -> tuple[int, bytes] | None:
        if path != GEOGRAPHY_PATH:
            return None
        status, original = fallback._answer(path, body)
        return (status, restated) if original == usa.STATE_ALL.read_bytes() else None

    return usa.FixtureClient(agency_limit=3, answers=answer)


def test_every_read_is_captured_and_amounts_load_as_published(
    usaspending_warehouse,
) -> None:
    """Covers: ETL-132 — each request is one capture; amounts keep USAspending's text."""
    factory = usaspending_warehouse
    run_id, requests, written, published = usa.run_to_gold(factory)
    assert (requests, written, published) == (
        ORDINARY_RUN_REQUESTS,
        ORDINARY_RUN_ROWS,
        ORDINARY_RUN_REQUESTS,
    )
    assert _rows(
        factory,
        """
        SELECT COUNT(*), COUNT(DISTINCT capture_id)
        FROM control.usaspending_request WHERE run_id = %s AND status = 'published'
        """,
        (str(run_id),),
    ) == [(ORDINARY_RUN_REQUESTS, ORDINARY_RUN_REQUESTS)]
    served = dict(
        _rows(
            factory,
            """
            SELECT metric_key || ' ' || geo_id, value_source
            FROM gold_usaspending.observation_latest
            WHERE geo_id IN (%s, %s) AND year = 2024
            """,
            (usa.WISCONSIN, usa.DANE),
        )
    )
    assert served == {
        f"federal_award_obligations {usa.WISCONSIN}": usa.WI_FY2024_ALL_AWARDS,
        f"federal_award_obligations_grants {usa.DANE}": usa.DANE_FY2024_GRANTS,
    }
    assert _rows(
        factory,
        """
        SELECT agency_code, value_source FROM silver_usaspending.agency_year
        WHERE fiscal_year = 2024 ORDER BY source_row_index
        """,
    ) == [
        ("HHS", "1854703443882.42"),
        ("SSA", "1507122505629.61"),
        ("DOD", "491886346082.79"),
    ]


def test_an_area_with_no_recorded_place_is_kept_but_never_served(
    usaspending_warehouse,
) -> None:
    """Covers: ETL-132 — the empty shape code stays `unsupported`, out of every gold view."""
    factory = usaspending_warehouse
    usa.run_to_gold(factory)
    assert _rows(
        factory,
        """
        SELECT shape_code, geography_status, value_source
        FROM silver_usaspending.geography_amount
        WHERE geo_layer = 'state' AND geo_id IS NULL
        """,
    ) == [("", "unsupported", "853504.0")]
    assert _rows(
        factory,
        """
        SELECT COUNT(*) FROM gold_usaspending.observation_latest
        WHERE metric_key = 'federal_award_obligations' AND year = 2024
        """,
    ) == [(56,)]


def test_a_share_without_its_place_total_says_so(usaspending_warehouse) -> None:
    """Covers: ETL-132 — no all-award county read means no share, with the reason."""
    factory = usaspending_warehouse
    usa.run_to_gold(factory)
    assert _rows(
        factory,
        """
        SELECT award_group, obligated_amount, share_of_place_total_pct, unavailable_reason
        FROM gold_usaspending.awards_by_award_type
        WHERE geo_id = %s AND fiscal_year = 2024
        """,
        (usa.DANE,),
    ) == [
        (
            "grants",
            Decimal(usa.DANE_FY2024_GRANTS),
            None,
            "all-award total not published for this place and year",
        )
    ]


def test_a_reread_writes_only_what_changed(usaspending_warehouse) -> None:
    """Covers: ETL-132 — the churn rule: unchanged rows stay; changed and withdrawn rows are revisions."""
    factory = usaspending_warehouse
    first_run, _requests, _written, _published = usa.run_to_gold(factory)
    _second, _requests, unchanged, _published = usa.run_to_gold(factory)
    assert unchanged == 0
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM silver_usaspending.geography_amount WHERE run_id = %s",
        (str(first_run),),
    ) == [(57 + 72,)]

    def restate(rows: list[dict[str, Any]]) -> None:
        rows[:] = [row for row in rows if row["shape_code"] != "MN"]
        for row in rows:
            if row["shape_code"] == "WI":
                row["aggregated_amount"] = 112441461700.01

    third, _requests, changed, _published = usa.run_to_gold(
        factory, client=_restating(restate)
    )
    assert changed == 2
    assert _rows(
        factory,
        """
        SELECT shape_code, change_kind, value_source, superseded_by_run_id::TEXT
        FROM silver_usaspending.geography_amount_revision ORDER BY shape_code
        """,
    ) == [
        ("MN", "withdrawn", "211874354275.61", str(third)),
        ("WI", "replaced", usa.WI_FY2024_ALL_AWARDS, str(third)),
    ]
    assert _rows(
        factory,
        """
        SELECT run_id::TEXT, value_source FROM gold_usaspending.observation_revision
        WHERE metric_key = 'federal_award_obligations' AND geo_id = %s
        ORDER BY published_at
        """,
        (usa.WISCONSIN,),
    ) == [
        (str(first_run), usa.WI_FY2024_ALL_AWARDS),
        (str(third), "112441461700.01"),
    ]
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM silver_usaspending.geography_amount WHERE shape_code = 'MN'",
    ) == [(0,)]
    assert [
        outcome.result
        for outcome in _outcomes(factory, usaspending_request_reconciliation)
    ] == ["pass", "pass"]


def test_a_repeated_place_is_quarantined_alone(usaspending_warehouse) -> None:
    """Covers: ETL-132 — a shape code answered twice is set aside once; the rest load."""
    factory = usaspending_warehouse

    def repeat(rows: list[dict[str, Any]]) -> None:
        rows.append(dict(next(row for row in rows if row["shape_code"] == "WI")))

    _run, _requests, written, _published = usa.run_to_gold(
        factory, client=_restating(repeat)
    )
    assert written == ORDINARY_RUN_ROWS
    assert _rows(
        factory,
        "SELECT source_row_index, error_code FROM silver_usaspending.quarantine",
    ) == [(58, "repeated_shape")]


def test_a_history_load_resumes_from_the_requests_never_captured(
    usaspending_warehouse,
) -> None:
    """Covers: ETL-132 — a budgeted history run stops; the next one continues, oldest year first."""
    factory = usaspending_warehouse
    budget = {"history": True, "max_requests": ORDINARY_RUN_REQUESTS + 8}
    first, requests, _written, _published = usa.run_to_gold(factory, conf=budget)
    assert requests == ORDINARY_RUN_REQUESTS + 8
    second, requests, _written, _published = usa.run_to_gold(factory, conf=budget)
    assert requests == ORDINARY_RUN_REQUESTS + 8
    by_run = dict(
        _rows(
            factory,
            """
            SELECT run_id::TEXT, COUNT(*) FROM control.usaspending_request
            WHERE fiscal_year = 2008 AND cut_kind <> 'agency_list' GROUP BY run_id
            """,
        )
    )
    # Fiscal 2008 plans 12 reads (no agencies in the fixtures): 7, then 5.
    assert by_run == {str(first): 7, str(second): 5}
    assert _rows(
        factory,
        """
        SELECT COUNT(DISTINCT (geo_layer, cut_kind, cut_key)) FROM control.usaspending_request
        WHERE fiscal_year = 2008 AND cut_kind <> 'agency_list'
        """,
    ) == [(12,)]
    assert _rows(
        factory,
        """
        SELECT COUNT(*) FROM control.usaspending_request
        WHERE run_id = %s AND fiscal_year = 2009
        """,
        (str(second),),
    ) == [(2,)]


def test_a_retry_of_the_same_run_skips_what_its_failed_attempt_captured(
    usaspending_warehouse,
) -> None:
    """Covers: ETL-132 — a retry continues a refresh year where the failed attempt stopped.

    The first attempt fails on fiscal 2024's county all-award read after 23
    captured requests; the retry, given the run's start, reads only each
    year's agency list again and fiscal 2024's nine county reads.
    """
    factory = usaspending_warehouse
    started = datetime.now(UTC)

    def refuse_county(path: str, body: dict[str, Any]) -> tuple[int, bytes] | None:
        filters = body.get("filters", {})
        if (
            path == GEOGRAPHY_PATH
            and body.get("geo_layer") == "county"
            and filters.get("time_period") == [fiscal_year_period(2024)]
            and "award_type_codes" not in filters
            and "agencies" not in filters
        ):
            return 400, b"{}"
        return None

    with pytest.raises(UsaspendingFetchError, match="HTTP 400"):
        capture_awards(
            factory,
            config=UsaspendingConfig(min_spacing_seconds=0, max_attempts=1),
            client=usa.FixtureClient(agency_limit=3, answers=refuse_county),
            today=usa.TODAY,
            sleep=lambda _seconds: None,
        )
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM control.usaspending_request WHERE created_at >= %s",
        (started,),
    ) == [(23,)]
    settle_unreplayed_runs(factory)
    _run, requests, _written, _published = usa.run_to_gold(
        factory, refresh_since=started
    )
    assert requests == 1 + 1 + 9
    assert _rows(
        factory,
        """
        SELECT COUNT(DISTINCT (fiscal_year, geo_layer, cut_kind, cut_key))
          FROM control.usaspending_request
        """,
    ) == [(ORDINARY_RUN_REQUESTS,)]
    # A new scheduled run (no earlier attempt) reads both years in full.
    _run, requests, _written, _published = usa.run_to_gold(factory)
    assert requests == ORDINARY_RUN_REQUESTS


def test_an_unreplayed_run_is_settled_by_the_next_attempt(
    usaspending_warehouse,
) -> None:
    """Covers: ETL-132 — captures left by an interrupted run are replayed and published."""
    factory = usaspending_warehouse
    run_id, _requests = capture_awards(
        factory,
        config=UsaspendingConfig(min_spacing_seconds=0, max_attempts=1),
        client=usa.FixtureClient(agency_limit=3),
        today=usa.TODAY,
        sleep=lambda _seconds: None,
    )
    assert [
        outcome.result
        for outcome in _outcomes(factory, usaspending_request_reconciliation)
    ][0] == "fail"
    assert settle_unreplayed_runs(factory) == [run_id]
    assert _rows(
        factory,
        "SELECT DISTINCT status FROM control.usaspending_request WHERE run_id = %s",
        (str(run_id),),
    ) == [("published",)]


def test_group_totals_check_passes_when_no_state_groups_are_published(
    usaspending_warehouse,
) -> None:
    """Covers: ETL-132 — DQ-USASPENDING-004 compares only years with both reads."""
    factory = usaspending_warehouse
    usa.run_to_gold(factory)
    assert [
        outcome.result for outcome in _outcomes(factory, usaspending_group_total)
    ] == ["pass"]


def test_the_glossary_publishes_the_grains_served(usaspending_warehouse) -> None:
    """Covers: ETL-132 — the publisher derives each measure's grains from served rows."""
    factory = usaspending_warehouse
    usa.run_to_gold(factory)
    assert harvest_publisher(factory, Publisher("gold_usaspending")) == 2
    assert _rows(
        factory,
        """
        SELECT source_object_key, valid_geo_grains, valid_time_grains, source_watermark
        FROM gold_usaspending.metric_publisher ORDER BY source_object_key
        """,
    ) == [
        ("federal_award_obligations", ["STATE"], ["ANNUAL"], "USAspending FY2024"),
        (
            "federal_award_obligations_grants",
            ["COUNTY"],
            ["ANNUAL"],
            "USAspending FY2024",
        ),
    ]


def test_schema_is_reapplied_idempotently(usaspending_warehouse) -> None:
    """Covers: ETL-132 — the source-owned DDL applies twice and names every relation."""
    factory = usaspending_warehouse

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_usaspending_schema(_Hook())
    ensure_usaspending_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]
