"""Shared seeding and cleanup for USAspending database tests.

The fixtures under ``tests/fixtures/usaspending/`` are answers read through
OFFICE_LAB on 2026-11-12, each kept byte for byte as USAspending sent it:

* ``state_all_fy2024.json``: ``spending_by_geography``, state layer, every
  award, fiscal 2024 (57 rows: 50 states, DC, five territories and the empty
  code of awards with no recorded place).
* ``county_grants_fy2024_wi.json``: the county layer for grants (award types
  02-05), fiscal 2024, the 72 Wisconsin counties of that answer. The probe
  kept Wisconsin only; the answer is otherwise as sent.
* ``awarding_agencies_fy2024.json``: ``spending_by_category/awarding_agency``,
  fiscal 2024, page 1 of 1 (78 agencies).

The scripted client answers those three reads from the files and every other
read of a registered period with USAspending's own empty answer (an empty
``results`` list), so a run plans and captures every request it would make
against the live API without inventing a single amount.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.silver_ref.geography_pipeline import (
    GeographyRecord,
    GeographyRepository,
)
from data_ingestion_toolbox.usaspending.capture import capture_awards
from data_ingestion_toolbox.usaspending.config import SOURCE_CODE, UsaspendingConfig
from data_ingestion_toolbox.usaspending.registry import (
    AGENCY_PATH,
    AWARD_GROUPS,
    STATE_FIPS,
    fiscal_year_period,
)
from data_ingestion_toolbox.usaspending.silver_usaspending.load import (
    publish_run,
    replay_run,
)
from tests.support.capture_seed import (
    delete_shared_geographies,
    preexisting_geographies,
    seed_capture,
)
from tests.support.warehouse_scope import (
    delete_capture_graph,
    delete_harvested_glossary_rows,
    glossary_registration_exists,
    source_run_ids,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = REPOSITORY_ROOT / "tests/fixtures/usaspending"
STATE_ALL = FIXTURES / "state_all_fy2024.json"
COUNTY_GRANTS = FIXTURES / "county_grants_fy2024_wi.json"
AGENCIES = FIXTURES / "awarding_agencies_fy2024.json"

FIXTURE_FISCAL_YEAR = 2024
#: A day of fiscal 2025, so an ordinary run reads fiscal 2025 and 2024.
TODAY = date(2025, 3, 1)

WISCONSIN = "state:55"
DANE = "state:55|county:025"

#: Fiscal 2024 as USAspending published it.
WI_FY2024_ALL_AWARDS = "112441461673.81"
DANE_FY2024_GRANTS = "2978864182.74"
NATION_FY2024_STATE_ROWS = 57
WI_COUNTIES = 72

_GRANTS = next(group for group in AWARD_GROUPS if group.key == "grants")


def _body(path: Path) -> bytes:
    return path.read_bytes()


def agencies_body(limit: int | None = None) -> bytes:
    """The agency list answer; ``limit`` keeps its first agencies only (re-serialised)."""
    raw = _body(AGENCIES)
    if limit is None:
        return raw
    body = json.loads(raw)
    body["results"] = body["results"][:limit]
    return json.dumps(body).encode()


def empty_geography(geo_layer: str) -> bytes:
    """USAspending's answer for a read with no obligations."""
    return json.dumps(
        {
            "scope": "place_of_performance",
            "geo_layer": geo_layer,
            "spending_level": "transactions",
            "results": [],
            "messages": [],
        }
    ).encode()


def empty_agencies() -> bytes:
    return json.dumps(
        {
            "category": "awarding_agency",
            "spending_level": "transactions",
            "limit": 100,
            "page_metadata": {
                "page": 1,
                "next": None,
                "previous": None,
                "hasNext": False,
                "hasPrevious": False,
            },
            "results": [],
            "messages": [],
        }
    ).encode()


class FixtureClient:
    """Answers search requests from the reviewed fiscal 2024 answers.

    ``agency_limit`` names only the first agencies of the year's list, which
    keeps a run's agency reads few. ``any_year`` answers every fiscal year
    with the fiscal 2024 files, for a harness whose run date is today's.
    ``answers`` overrides a read: called with (path, body), it returns
    (status, bytes) or ``None`` to fall through.
    """

    def __init__(
        self,
        *,
        agency_limit: int | None = None,
        any_year: bool = False,
        answers: Callable[[str, dict[str, Any]], tuple[int, bytes] | None]
        | None = None,
    ) -> None:
        self.agency_limit = agency_limit
        self.any_year = any_year
        self.answers = answers
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def _answer(self, path: str, body: dict[str, Any]) -> tuple[int, bytes]:
        if self.answers is not None:
            override = self.answers(path, body)
            if override is not None:
                return override
        filters = body.get("filters", {})
        in_year = self.any_year or filters.get("time_period") == [
            fiscal_year_period(FIXTURE_FISCAL_YEAR)
        ]
        if path == AGENCY_PATH:
            if not in_year or int(body.get("page", 1)) != 1:
                return 200, empty_agencies()
            return 200, agencies_body(self.agency_limit)
        layer = str(body["geo_layer"])
        if in_year and "agencies" not in filters:
            codes = filters.get("award_type_codes")
            if layer == "state" and codes is None:
                return 200, _body(STATE_ALL)
            if layer == "county" and codes == list(_GRANTS.codes):
                return 200, _body(COUNTY_GRANTS)
        return 200, empty_geography(layer)

    def post(
        self, url: str, *, json: dict[str, Any], headers: dict[str, str]
    ) -> httpx.Response:
        path = url.split("/api/v2", 1)[-1]
        self.calls.append((path, json))
        status, content = self._answer(path, json)
        return httpx.Response(
            status,
            content=content,
            headers={"content-type": "application/json"},
            request=httpx.Request("POST", url),
        )

    def close(self) -> None:
        return None


def run_to_gold(
    connection_factory: Callable[[], connection],
    *,
    client: FixtureClient | None = None,
    conf: dict[str, Any] | None = None,
    today: date = TODAY,
) -> tuple[UUID, int, int, int]:
    """Capture, replay and publish a run; return (run, requests, written, published)."""
    run_id, requests = capture_awards(
        connection_factory,
        conf=conf,
        config=UsaspendingConfig(min_spacing_seconds=0, max_attempts=1),
        client=client or FixtureClient(agency_limit=3),
        today=today,
        sleep=lambda _seconds: None,
    )
    written = replay_run(connection_factory, run_id=run_id)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, requests, written, published


def run_all(connection_factory: Callable[[], connection]) -> None:
    run_to_gold(connection_factory)


def _geographies() -> list[GeographyRecord]:
    """The states USAspending names (with their USPS code) and Wisconsin's counties."""
    names = {
        str(row["shape_code"]): str(row["display_name"])
        for row in json.loads(_body(STATE_ALL))["results"]
        if row["shape_code"]
    }
    places = [
        GeographyRecord(
            "state",
            f"state:{fips}",
            fips,
            fips,
            None,
            None,
            names[postal],
            2024,
            usps=postal,
        )
        for postal, fips in sorted(STATE_FIPS.items(), key=lambda item: item[1])
    ]
    for row in json.loads(_body(COUNTY_GRANTS))["results"]:
        code = str(row["shape_code"])
        places.append(
            GeographyRecord(
                "county",
                f"state:{code[:2]}|county:{code[2:]}",
                code,
                code[:2],
                code[2:],
                None,
                str(row["display_name"]),
                2024,
            )
        )
    return places


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the states and Wisconsin's counties; remove all USAspending state afterwards."""
    places = _geographies()
    geo_ids = [record.geo_id for record in places]
    reader = connection_factory()
    try:
        with reader.cursor() as cursor:
            preexisting = preexisting_geographies(cursor, geo_ids)
    finally:
        reader.close()
    preexisting_glossary = glossary_registration_exists(connection_factory, SOURCE_CODE)
    baseline = source_run_ids(connection_factory, SOURCE_CODE)

    def cleanup() -> None:
        owned_runs = sorted(source_run_ids(connection_factory, SOURCE_CODE) - baseline)
        database_connection = connection_factory()
        try:
            with database_connection.cursor() as cursor:
                delete_harvested_glossary_rows(
                    cursor, SOURCE_CODE, preexisting=preexisting_glossary
                )
                cursor.execute(
                    "DELETE FROM control.publisher_ready_event WHERE source_code = %s",
                    (SOURCE_CODE,),
                )
                for table in (
                    "silver_usaspending.geography_amount_revision",
                    "silver_usaspending.geography_amount",
                    "silver_usaspending.agency_year",
                    "silver_usaspending.quarantine",
                    "control.usaspending_request",
                ):
                    cursor.execute(f"DELETE FROM {table}")
                delete_capture_graph(cursor, owned_runs)
                delete_shared_geographies(cursor, geo_ids, preexisting)
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()

    request.addfinalizer(cleanup)
    writer = connection_factory()
    try:
        with writer.cursor() as cursor:
            capture_id = seed_capture(cursor, "CENSUS_GEO")
        writer.commit()
    finally:
        writer.close()
    GeographyRepository(connection_factory).load_attributes(
        [record for record in places if record.geo_id not in preexisting],
        capture_id=capture_id,
    )
    return connection_factory
