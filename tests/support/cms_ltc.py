"""Shared seeding and cleanup for CMS long-term care database tests.

The checked-in fixtures under ``tests/fixtures/cms_ltc`` are CMS's own bytes:
the two series' entries of ``data.cms.gov/data.json`` (as the catalog holds
them) and the Wisconsin rows of three CSV releases, cut line for line from
the files CMS serves. A scripted HTTP client plays them through the
adapter's own capture path, so every database test exercises the capture,
replay and publication code production runs.
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

from data_ingestion_toolbox.cms_ltc.capture import capture_catalog, capture_release
from data_ingestion_toolbox.cms_ltc.catalog import Release
from data_ingestion_toolbox.cms_ltc.config import (
    CATALOG_URL,
    SOURCE_CODE,
    CmsLtcConfig,
)
from data_ingestion_toolbox.cms_ltc.silver_cms_ltc.load import (
    publish_run,
    replay_run,
)
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.warehouse_scope import (
    delete_capture_graph,
    delete_harvested_glossary_rows,
    glossary_registration_exists,
    source_run_ids,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/cms_ltc"
COUNTY_NAMES = REPOSITORY_ROOT / "tests/fixtures/census_decennial/pl_county_all.json"

DANE = "state:55|county:025"

#: The CSV each fixture release's catalog entry names, and the fixture
#: holding its Wisconsin rows.
FIXTURE_FILES: dict[tuple[str, date], str] = {
    ("cost_report", date(2024, 1, 1)): "CostReportsnf_Final_24_wi.csv",
    ("characteristics", date(2026, 4, 1)): "Form_671_Q2_2026_wi.csv",
    ("characteristics", date(2026, 7, 1)): "Form_671_Q3_2026_wi.csv",
}

SILVER_TABLES: tuple[str, ...] = (
    "silver_cms_ltc.area_total",
    "silver_cms_ltc.facility_count",
    "silver_cms_ltc.facility_report",
    "silver_cms_ltc.quarantine",
    "silver_cms_ltc.dim_measure",
)


def catalog_bytes() -> bytes:
    return (FIXTURE_DIR / "data_json_excerpt.json").read_bytes()


def fixture_catalog_bytes() -> bytes:
    """The catalog excerpt with only the releases a fixture holds."""
    document = json.loads(catalog_bytes())
    document["dataset"] = [
        entry
        for entry in document["dataset"]
        if (_series_of(entry["title"]), _start_of(entry)) in FIXTURE_FILES
    ]
    return json.dumps(document).encode()


def _series_of(title: str) -> str:
    return (
        "characteristics"
        if title.startswith("Long-Term Care Facility Characteristics")
        else "cost_report"
    )


def _start_of(entry: dict[str, Any]) -> date:
    return date.fromisoformat(entry["temporal"][0]["startDate"])


def file_bytes(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


def fixture_url_map() -> dict[str, str]:
    """The download URL the catalog names for each fixture release -> fixture file."""
    document = json.loads(catalog_bytes())
    urls = {}
    for entry in document["dataset"]:
        key = (_series_of(entry["title"]), _start_of(entry))
        if key not in FIXTURE_FILES:
            continue
        for item in entry["distribution"]:
            if item.get("mediaType") == "text/csv":
                urls[item["downloadURL"]] = FIXTURE_FILES[key]
    return urls


class FixtureClient:
    """Answers the catalog and each fixture release; anything else is a 404."""

    def __init__(
        self,
        overrides: dict[str, httpx.Response] | None = None,
        *,
        catalog: bytes | None = None,
    ) -> None:
        self.overrides = dict(overrides or {})
        self.catalog = catalog if catalog is not None else fixture_catalog_bytes()
        self.urls = fixture_url_map()
        self.calls: list[str] = []

    def get(self, url: str, *, headers: dict[str, str] | None = None) -> httpx.Response:
        self.calls.append(url)
        request = httpx.Request("GET", url)
        if url in self.overrides:
            response = self.overrides[url]
            return httpx.Response(
                response.status_code, content=response.content, request=request
            )
        if url == CATALOG_URL:
            return httpx.Response(
                200,
                content=self.catalog,
                headers={"content-type": "application/json"},
                request=request,
            )
        if url in self.urls:
            return httpx.Response(
                200,
                content=file_bytes(self.urls[url]),
                headers={"content-type": "text/csv"},
                request=request,
            )
        return httpx.Response(404, request=request)

    def close(self) -> None:
        return None


def config() -> CmsLtcConfig:
    return CmsLtcConfig(min_spacing_seconds=0, max_attempts=1)


def load_releases(
    connection_factory: Callable[[], connection],
    *,
    client: FixtureClient | None = None,
) -> tuple[list[Release], list[tuple[UUID, str, int, int]]]:
    """Read the catalog and every fixture release, cost reports first, to gold.

    Returns the releases and, per release, (run, status, rows stored, published).
    """
    active = client or FixtureClient()
    catalog_capture, found, _skipped = capture_catalog(
        connection_factory, config=config(), client=active
    )
    order = {"cost_report": 0, "characteristics": 1}
    found = sorted(found, key=lambda item: (order[item.series_id], item.period_start))
    results = []
    for release in found:
        run_id, status = capture_release(
            connection_factory,
            release,
            catalog_capture_id=catalog_capture,
            config=config(),
            client=active,
        )
        stored = replay_run(connection_factory, run_id=run_id)
        published = publish_run(connection_factory, run_id=run_id)
        results.append((run_id, status, stored, published))
    return found, results


def _seed_arguments() -> tuple[dict[str, Any], ...]:
    counties = json.loads(COUNTY_NAMES.read_bytes())
    return (
        {"geo_type": "nation", "vintage": 2024, "name": "United States"},
        {"geo_type": "state", "state_fips": "55", "vintage": 2024, "name": "Wisconsin"},
        *(
            {
                "geo_type": "county",
                "state_fips": "55",
                "county_fips": row[-1],
                "vintage": 2024,
                "name": row[0].split(",")[0],
            }
            for row in counties[1:]
        ),
    )


def canonical(arguments: dict[str, Any]) -> str:
    if arguments["geo_type"] == "nation":
        return "us:1"
    if arguments["geo_type"] == "state":
        return f"state:{arguments['state_fips']}"
    return f"state:{arguments['state_fips']}|county:{arguments['county_fips']}"


def tracked_geo_ids() -> tuple[str, ...]:
    return tuple(canonical(arguments) for arguments in _seed_arguments())


def _preexisting(connection_factory: Callable[[], connection]) -> set[str]:
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                (list(tracked_geo_ids()),),
            )
            return {row[0] for row in cursor.fetchall()}
    finally:
        database_connection.close()


def _cleanup(
    connection_factory: Callable[[], connection],
    preexisting: set[str],
    *,
    preexisting_glossary: bool,
    baseline_run_ids: frozenset,
) -> Callable[[], None]:
    def run() -> None:
        owned_runs = sorted(
            source_run_ids(connection_factory, SOURCE_CODE) - baseline_run_ids
        )
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
                for table in SILVER_TABLES:
                    cursor.execute(f"DELETE FROM {table}")
                cursor.execute("DELETE FROM control.cms_ltc_file")
                delete_capture_graph(cursor, owned_runs)
                for geo_id in sorted(set(tracked_geo_ids()) - preexisting):
                    delete_geography(cursor, geo_id)
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()

    return run


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the nation, Wisconsin and its counties; remove all CMS state afterwards."""
    preexisting = _preexisting(connection_factory)
    request.addfinalizer(
        _cleanup(
            connection_factory,
            preexisting,
            preexisting_glossary=glossary_registration_exists(
                connection_factory, SOURCE_CODE
            ),
            baseline_run_ids=source_run_ids(connection_factory, SOURCE_CODE),
        )
    )
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            for arguments in _seed_arguments():
                if canonical(arguments) not in preexisting:
                    seed_geography(cursor, **arguments)
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    return connection_factory


__all__ = [
    "DANE",
    "FIXTURE_FILES",
    "FixtureClient",
    "catalog_bytes",
    "file_bytes",
    "load_releases",
    "reviewed_warehouse",
]
