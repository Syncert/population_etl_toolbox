"""Shared seeding and cleanup for Monthly Treasury Statement database tests.

``tests/fixtures/treasury_mts/mts_table_9_2026-08_2026-09.json`` holds the
first page of a Table 9 read made through OFFICE_LAB on 2026-11-12 (newest
first, 60 rows): all 33 lines of the September 2026 statement and the last 27
of the August 2026 one, each row copied verbatim, ordered here as the
adapter asks for them (record date, then source line). The page envelope the
scripted client answers (``meta`` with ``total-pages``, ``links``) is built
here from those rows, in the API's own shape.

The nation (``us:1``) is seeded unless another fixture already holds it.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.treasury_mts.capture import capture_statements
from data_ingestion_toolbox.treasury_mts.config import SOURCE_CODE, TreasuryMtsConfig
from data_ingestion_toolbox.treasury_mts.silver_treasury_mts.load import (
    publish_run,
    replay_run,
)
from tests.support.capture_seed import (
    delete_shared_geographies,
    preexisting_geographies,
    seed_geography,
)
from tests.support.warehouse_scope import (
    delete_capture_graph,
    delete_harvested_glossary_rows,
    glossary_registration_exists,
    source_run_ids,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = (
    REPOSITORY_ROOT / "tests/fixtures/treasury_mts/mts_table_9_2026-08_2026-09.json"
)
NATION = "us:1"

#: Fiscal 2026 as Treasury published it in the September 2026 statement.
FY2026_TOTAL_OUTLAYS = "7395799983313.23"
FY2026_NET_INTEREST = "1051753969311.11"
FY2026_NATIONAL_DEFENSE = "965160356821.48"


def fixture_rows() -> list[dict[str, str]]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))["data"]


def page_body(rows: list[dict[str, str]], page_number: int, page_size: int) -> bytes:
    """One page of ``rows`` in the Fiscal Data envelope."""
    total_pages = max(1, -(-len(rows) // page_size))
    chunk = rows[(page_number - 1) * page_size : page_number * page_size]
    link = "&page%5Bnumber%5D={}&page%5Bsize%5D=" + str(page_size)
    return json.dumps(
        {
            "data": chunk,
            "meta": {
                "count": len(chunk),
                "total-count": len(rows),
                "total-pages": total_pages,
            },
            "links": {
                "self": link.format(page_number),
                "first": link.format(1),
                "prev": link.format(page_number - 1) if page_number > 1 else None,
                "next": link.format(page_number + 1)
                if page_number < total_pages
                else None,
                "last": link.format(total_pages),
            },
        }
    ).encode()


class FixtureClient:
    """Answers Table 9 pages from the fixture rows on or after ``since``."""

    def __init__(self, rows: list[dict[str, str]] | None = None) -> None:
        self.rows = rows if rows is not None else fixture_rows()
        self.calls: list[dict[str, str]] = []

    def get(
        self, url: str, *, params: dict[str, str], headers: dict[str, str]
    ) -> httpx.Response:
        self.calls.append(dict(params))
        request = httpx.Request("GET", url)
        since = params["filter"].rsplit(":", 1)[-1]
        rows = [row for row in self.rows if row["record_date"] >= since]
        body = page_body(rows, int(params["page[number]"]), int(params["page[size]"]))
        return httpx.Response(
            200,
            content=body,
            headers={"content-type": "application/json"},
            request=request,
        )

    def close(self) -> None:
        return None


def run_to_gold(
    connection_factory: Callable[[], connection],
    *,
    client: FixtureClient | None = None,
    page_size: int = 10_000,
    conf: dict[str, Any] | None = None,
) -> tuple[UUID, int, int, int]:
    """Capture, replay and publish a read; return (run, pages, written, published)."""
    run_id, pages = capture_statements(
        connection_factory,
        conf=conf,
        config=TreasuryMtsConfig(
            min_spacing_seconds=0, max_attempts=1, page_size=page_size
        ),
        client=client or FixtureClient(),
        sleep=lambda _seconds: None,
    )
    written = replay_run(connection_factory, run_id=run_id)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, pages, written, published


def run_all(connection_factory: Callable[[], connection]) -> None:
    run_to_gold(connection_factory)


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the nation and remove all Treasury MTS state afterwards."""
    reader = connection_factory()
    try:
        with reader.cursor() as cursor:
            preexisting = preexisting_geographies(cursor, [NATION])
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
                    "silver_treasury_mts.statement_line_revision",
                    "silver_treasury_mts.statement_line",
                    "silver_treasury_mts.quarantine",
                    "control.treasury_mts_page",
                ):
                    cursor.execute(f"DELETE FROM {table}")
                delete_capture_graph(cursor, owned_runs)
                delete_shared_geographies(cursor, [NATION], preexisting)
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()

    request.addfinalizer(cleanup)
    if NATION not in preexisting:
        writer = connection_factory()
        try:
            with writer.cursor() as cursor:
                seed_geography(
                    cursor, geo_type="nation", vintage=2024, name="United States"
                )
            writer.commit()
        except BaseException:
            writer.rollback()
            raise
        finally:
            writer.close()
    return connection_factory
