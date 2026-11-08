"""Seeding and small NDCP-shaped workbooks for the childcare tests.

``tests/fixtures/dol_childcare/NDCP2022.xlsx`` holds the Women's Bureau's own
rows for Dane and Rock counties (2021, 2022) and Los Angeles County (2022),
copied from ``NDCP2022.xlsx`` with every one of its 370 columns, with Rock's
2022 home-based school-age price blanked, a wholly blank row as the release
has, and one row whose county code is unreadable. The workbook writer below
needs no spreadsheet library; it emits the parts ``utility.workbook.read_sheet`` reads: the
workbook, its relationships and one sheet with inline strings and numbers.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID
from xml.sax.saxutils import escape

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.dol_childcare.capture import capture_release
from data_ingestion_toolbox.dol_childcare.config import SOURCE_CODE, ChildcareConfig
from data_ingestion_toolbox.dol_childcare.registry import MEASURES, RELEASES
from data_ingestion_toolbox.dol_childcare.silver_dol_childcare.load import (
    publish_run,
    replay_run,
)
from data_ingestion_toolbox.dol_childcare.silver_dol_childcare.parse import (
    required_columns,
)
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.warehouse_scope import (
    delete_capture_graph,
    delete_harvested_glossary_rows,
    glossary_registration_exists,
    source_run_ids,
)

RELEASE = RELEASES[0]

_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_PACKAGE_REL = "http://schemas.openxmlformats.org/package/2006/relationships"


def _letters(number: int) -> str:
    text = ""
    while number:
        number, rest = divmod(number - 1, 26)
        text = chr(65 + rest) + text
    return text


def _cell(reference: str, value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f'<c r="{reference}"><v>{value}</v></c>'
    return f'<c r="{reference}" t="inlineStr"><is><t>{escape(str(value))}</t></is></c>'


def build_workbook(rows: list[list[object]], *, sheet: str = RELEASE.sheet) -> bytes:
    body = "".join(
        f'<row r="{index}">'
        + "".join(
            _cell(f"{_letters(column)}{index}", value)
            for column, value in enumerate(row, start=1)
        )
        + "</row>"
        for index, row in enumerate(rows, start=1)
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "xl/workbook.xml",
            f'<workbook xmlns="{_MAIN}" xmlns:r="{_REL}"><sheets>'
            f'<sheet name="{escape(sheet)}" sheetId="1" r:id="rId1"/></sheets></workbook>',
        )
        archive.writestr(
            "xl/_rels/workbook.xml.rels",
            f'<Relationships xmlns="{_PACKAGE_REL}"><Relationship Id="rId1" '
            'Target="worksheets/sheet1.xml"/></Relationships>',
        )
        archive.writestr(
            "xl/worksheets/sheet1.xml",
            f'<worksheet xmlns="{_MAIN}"><sheetData>{body}</sheetData></worksheet>',
        )
    return buffer.getvalue()


def header() -> list[str]:
    return ["COUNTY_NAME", *sorted(required_columns(RELEASE))]


def row(columns: list[str], **values: object) -> list[object]:
    """One county-year: every price 987.5 (no county in the fixture charges it) and every code 00000 unless given."""
    defaults: dict[str, object] = {
        "COUNTY_NAME": "Dane County",
        "COUNTY_FIPS_CODE": 55025,
        "STUDYYEAR": 2022,
    }
    for measure in MEASURES:
        defaults[measure.column] = 987.5
        for column in measure.imputation_columns:
            defaults[column] = "00000"
    defaults.update(values)
    return [defaults.get(column) for column in columns]


# --- database seeding -------------------------------------------------------

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = REPOSITORY_ROOT / "tests/fixtures/dol_childcare/NDCP2022.xlsx"

DANE = "state:55|county:025"
ROCK = "state:55|county:105"
LOS_ANGELES = "state:06|county:037"

_SEED_ARGUMENTS: tuple[dict[str, Any], ...] = (
    {"geo_type": "state", "state_fips": "55", "vintage": 2022, "name": "Wisconsin"},
    {"geo_type": "state", "state_fips": "06", "vintage": 2022, "name": "California"},
    {
        "geo_type": "county",
        "state_fips": "55",
        "county_fips": "025",
        "vintage": 2022,
        "name": "Dane County",
    },
    {
        "geo_type": "county",
        "state_fips": "55",
        "county_fips": "105",
        "vintage": 2022,
        "name": "Rock County",
    },
    {
        "geo_type": "county",
        "state_fips": "06",
        "county_fips": "037",
        "vintage": 2022,
        "name": "Los Angeles County",
    },
)

TRACKED_GEO_IDS: tuple[str, ...] = ("state:55", "state:06", DANE, ROCK, LOS_ANGELES)

SILVER_TABLES: tuple[str, ...] = (
    "silver_dol_childcare.fact_price",
    "silver_dol_childcare.price_revision",
    "silver_dol_childcare.observation_quarantine",
)


class FixtureClient:
    """Answers the workbook with the fixture, or with the bytes given."""

    def __init__(self, payload: bytes | None = None, *, status: int = 200) -> None:
        self.payload = FIXTURE.read_bytes() if payload is None else payload
        self.status = status
        self.urls: list[str] = []

    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        self.urls.append(url)
        return httpx.Response(
            self.status,
            content=self.payload,
            headers={
                "content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            },
            request=httpx.Request("GET", url),
        )

    def close(self) -> None:
        return None


def run_to_gold(
    connection_factory: Callable[[], connection],
    *,
    client: FixtureClient | None = None,
) -> tuple[UUID, str, int, int]:
    """Capture, replay and publish the release; return (run, status, facts, published)."""
    run_id, status = capture_release(
        connection_factory,
        RELEASE,
        config=ChildcareConfig(min_spacing_seconds=0, max_attempts=1),
        client=client or FixtureClient(),
    )
    facts = replay_run(connection_factory, run_id=run_id)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, status, facts, published


def _canonical(arguments: dict[str, Any]) -> str:
    if arguments["geo_type"] == "state":
        return f"state:{arguments['state_fips']}"
    return f"state:{arguments['state_fips']}|county:{arguments['county_fips']}"


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the fixture counties and remove all NDCP state afterwards."""
    reader = connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                (list(TRACKED_GEO_IDS),),
            )
            preexisting = {row[0] for row in cursor.fetchall()}
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
                cursor.execute(
                    "DELETE FROM silver_ref.geography_resolution WHERE provider_source = %s",
                    (SOURCE_CODE,),
                )
                for table in SILVER_TABLES:
                    cursor.execute(f"DELETE FROM {table}")
                cursor.execute("DELETE FROM control.dol_childcare_file")
                delete_capture_graph(cursor, owned_runs)
                for geo_id in sorted(set(TRACKED_GEO_IDS) - preexisting):
                    delete_geography(cursor, geo_id)
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
            for arguments in _SEED_ARGUMENTS:
                if _canonical(arguments) not in preexisting:
                    seed_geography(cursor, **arguments)
        writer.commit()
    except BaseException:
        writer.rollback()
        raise
    finally:
        writer.close()
    return connection_factory


def rows(factory: Callable[[], connection], sql: str, parameters: tuple = ()) -> list:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()
