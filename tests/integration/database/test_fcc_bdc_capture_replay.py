"""Real PostgreSQL FCC broadband availability capture-to-gold contract.

Covers: ETL-071
"""

from __future__ import annotations

import csv
import io
import zipfile
from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.fcc_bdc.client import BdcPayloadError
from data_ingestion_toolbox.fcc_bdc.schema import (
    REQUIRED_RELATIONS,
    ensure_fcc_bdc_schema,
)
from data_ingestion_toolbox.glossary.harvest import (
    Publisher,
    harvest_publisher,
    process_pending_events,
)
from data_ingestion_toolbox.quality.sources import (
    bdc_read_reconciliation,
    bdc_tier_order_and_geography,
)
from tests.support import fcc_bdc as bdc

pytestmark = [pytest.mark.integration, pytest.mark.database]

NATIONAL_2025 = "downloads/downloadFile/availability/1820956"


@pytest.fixture
def bdc_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return bdc.reviewed_warehouse(postgres_connection_factory, request)


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


def _outcome(factory: Callable[[], connection], executor):  # noqa: ANN001, ANN202
    reader = factory()
    try:
        with reader.cursor() as cursor:
            return executor(cursor, {})[0]
    finally:
        reader.close()


def _national_csv() -> tuple[str, str]:
    archive = zipfile.ZipFile(
        io.BytesIO((bdc.FIXTURE_DIR / "1820956.zip").read_bytes())
    )
    member = archive.infolist()[0]
    return member.filename, archive.read(member).decode("utf-8")


_COLUMNS = (
    "area_data_type",
    "geography_type",
    "geography_id",
    "geography_desc",
    "geography_desc_full",
    "total_units",
    "biz_res",
    "technology",
    "speed_02_02",
    "speed_10_1",
    "speed_25_3",
    "speed_100_20",
    "speed_250_25",
    "speed_1000_100",
)


def _set_field(line: str, column: str, value: str) -> str:
    """One CSV line with one field replaced, quoting kept by the csv module."""
    fields = next(csv.reader([line]))
    fields[_COLUMNS.index(column)] = value
    out = io.StringIO()
    csv.writer(out, lineterminator="\n").writerow(fields)
    return out.getvalue()


def _zip(name: str, text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, text)
    return buffer.getvalue()


def _latest(
    factory: Callable[[], connection], metric: str, geo_id: str, year: int
) -> tuple:
    rows = _rows(
        factory,
        """
        SELECT value, value_status, geo_level, revision FROM gold_fcc_bdc.observation_latest
        WHERE metric_key = %s AND geo_id = %s AND year = %s
        """,
        (metric, geo_id, year),
    )
    return rows[0] if rows else ()


def test_each_vintage_serves_the_fccs_own_summaries(bdc_warehouse) -> None:
    """Covers: ETL-071 — nation, state, county and place rows by code, with the revision."""
    factory = bdc_warehouse
    client = bdc.FixtureClient()
    published = [
        bdc.run_to_gold(factory, as_of, client=client)
        for as_of in (bdc.DEC_2024, bdc.DEC_2025)
    ]
    assert [(status, done) for _run, status, _rows_written, done in published] == [
        ("captured", 1),
        ("captured", 1),
    ]
    assert _latest(factory, "share_any_1000_100", bdc.KENT, 2025) == (
        Decimal("0.636547056"),
        "valid",
        "COUNTY",
        "29sep2026",
    )
    assert _latest(factory, "share_any_1000_100", bdc.KENT, 2024)[0] == Decimal(
        "0.486427325"
    )
    assert _latest(factory, "residential_units", bdc.NATION, 2025)[:3] == (
        Decimal("162419094"),
        "valid",
        "NATIONAL",
    )
    assert _latest(factory, "share_any_1000_100", bdc.DOVER, 2025)[:3] == (
        Decimal("0.930515721"),
        "valid",
        "PLACE",
    )
    assert _latest(factory, "share_any_100_20", bdc.WASHINGTON_DC, 2025)[0] == Decimal(
        "1.000000000"
    )
    # The CBSA row is read and dropped; unseeded places are kept, unmapped.
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM silver_fcc_bdc.availability_row WHERE geo_id LIKE 'cbsa%%'",
    ) == [(0,)]
    assert (
        _rows(
            factory,
            """
        SELECT COUNT(*) FROM silver_fcc_bdc.availability_row
        WHERE geography_type = 'place' AND geography_status = 'unmapped'
        """,
        )[0][0]
        > 0
    )
    # Credentials are headers only: in no capture's endpoint, parameters,
    # headers or payload.
    assert {headers["hash_value"] for headers in client.headers} == {bdc.FIXTURE_TOKEN}
    assert _rows(
        factory,
        """
        SELECT COUNT(*) FROM raw_capture.response_capture AS capture
        JOIN raw_capture.payload_blob AS blob USING (payload_checksum)
        WHERE capture.source_code = 'FCC_BDC'
          AND (capture.endpoint LIKE %s OR capture.request_parameters::TEXT LIKE %s
               OR capture.response_headers::TEXT LIKE %s
               OR POSITION(convert_to(%s, 'UTF8') IN blob.payload) > 0)
        """,
        (
            f"%{bdc.FIXTURE_TOKEN}%",
            f"%{bdc.FIXTURE_TOKEN}%",
            f"%{bdc.FIXTURE_TOKEN}%",
            bdc.FIXTURE_TOKEN,
        ),
    ) == [(0,)]


def test_no_units_is_missing_and_a_new_revision_is_a_second_release(
    bdc_warehouse,
) -> None:
    """Covers: ETL-071 — zero units give no share; an unchanged read adds nothing; a revision adds a release."""
    factory = bdc_warehouse
    bdc.run_to_gold(factory, bdc.DEC_2025)
    _run, status, written, published = bdc.run_to_gold(factory, bdc.DEC_2025)
    assert (status, written, published) == ("unchanged", 0, 0)
    name, text = _national_csv()
    lines = text.splitlines(keepends=True)
    kent = next(
        index
        for index, line in enumerate(lines)
        if line.startswith("Total,County,10001,") and ",R,Any Technology," in line
    )
    lines[kent] = _set_field(lines[kent], "total_units", "0")
    revised = _zip(name, "".join(lines))
    listing = (bdc.FIXTURE_DIR / "listAvailabilityData_2025-12-31.json").read_text(
        encoding="utf-8"
    )
    listing = listing.replace("_29sep2026", "_15oct2026")
    client = bdc.FixtureClient(
        {
            NATIONAL_2025: revised,
            "downloads/listAvailabilityData/2025-12-31": listing.encode(),
        }
    )
    run_id, status, _written, published = bdc.run_to_gold(
        factory, bdc.DEC_2025, client=client
    )
    assert (status, published) == ("captured", 1)
    assert _rows(
        factory,
        """
        SELECT total_units, speed_100_20, value_status, missing_reason FROM silver_fcc_bdc.availability_row
        WHERE run_id = %s AND geo_id = %s AND technology = 'Any Technology'
        """,
        (str(run_id), bdc.KENT),
    ) == [(0, None, "missing", "no_units")]
    assert _latest(factory, "share_any_100_20", bdc.KENT, 2025)[:2] == (None, "missing")
    assert _latest(factory, "residential_units", bdc.KENT, 2025)[:2] == (
        Decimal("0"),
        "valid",
    )
    assert _rows(
        factory,
        """
        SELECT COUNT(DISTINCT revision) FROM gold_fcc_bdc.observation_revision
        WHERE metric_key = 'share_any_100_20' AND geo_id = %s AND year = 2025
        """,
        (bdc.KENT,),
    ) == [(2,)]


def test_bad_rows_are_quarantined_and_a_foreign_file_is_refused(bdc_warehouse) -> None:
    """Covers: ETL-071 — an out-of-range share or a mismatched place is refused alone; a non-zip fails capture."""
    factory = bdc_warehouse
    name, text = _national_csv()
    lines = text.splitlines(keepends=True)
    state = next(
        index
        for index, line in enumerate(lines)
        if line.startswith("Total,State,10,") and ",R,All Wired," in line
    )
    lines[state] = _set_field(lines[state], "speed_1000_100", "1.5")
    run_id, status, _written, _published = bdc.run_to_gold(
        factory,
        bdc.DEC_2025,
        client=bdc.FixtureClient({NATIONAL_2025: _zip(name, "".join(lines))}),
    )
    assert status == "captured"
    assert _rows(
        factory,
        "SELECT error_code FROM silver_fcc_bdc.quarantine WHERE run_id = %s",
        (str(run_id),),
    ) == [("share_out_of_range",)]
    with pytest.raises(BdcPayloadError, match="not_a_zip"):
        bdc.run_to_gold(
            factory,
            bdc.DEC_2024,
            client=bdc.FixtureClient(
                {"downloads/downloadFile/availability/1725104": b"<html>denied</html>"}
            ),
        )


def test_the_rules_and_the_schema(bdc_warehouse) -> None:
    """Covers: ETL-071 — DQ-FCC-002 and -004 pass and then catch a fault; the schema reapplies."""
    factory = bdc_warehouse
    assert _outcome(factory, bdc_read_reconciliation).result == "not_applicable"
    bdc.run_all(factory)
    assert _outcome(factory, bdc_read_reconciliation).result == "pass"
    assert _outcome(factory, bdc_tier_order_and_geography).result == "pass"
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                UPDATE silver_fcc_bdc.availability_row SET speed_1000_100 = speed_02_02, speed_02_02 = 0.1
                WHERE geo_id = %s AND technology = 'Any Technology'
                  AND run_id = (SELECT run_id FROM control.fcc_bdc_read WHERE as_of_date = '2025-12-31')
                """,
                (bdc.KENT,),
            )
            cursor.execute(
                """
                DELETE FROM silver_fcc_bdc.availability_row
                WHERE run_id = (SELECT run_id FROM control.fcc_bdc_read WHERE as_of_date = '2024-12-31')
                """
            )
        writer.commit()
    finally:
        writer.close()
    order = _outcome(factory, bdc_tier_order_and_geography)
    assert (order.result, order.observed_count) == ("warn", 1)
    lost = _outcome(factory, bdc_read_reconciliation)
    assert (lost.result, lost.observed_count) == ("fail", 1)

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_fcc_bdc_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]


def test_the_harvest_names_six_measures(bdc_warehouse) -> None:
    """Covers: ETL-071 — six provider-published measures, distinct from ACS subscription."""
    factory = bdc_warehouse
    bdc.run_all(factory)
    published = dict(
        _rows(
            factory,
            "SELECT source_object_key, measure_kind FROM gold_fcc_bdc.metric_publisher",
        )
    )
    assert len(published) == 6 and set(published.values()) == {"source_fact"}
    basis = _rows(
        factory, "SELECT observation_basis FROM gold_fcc_bdc.measure_definition LIMIT 1"
    )[0][0]
    assert "not what households subscribe to" in basis
    assert harvest_publisher(factory, Publisher("gold_fcc_bdc")) > 0
    assert process_pending_events(factory) >= 1
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_glossary.dim_metric WHERE source_code = 'FCC_BDC'",
    ) == [(6,)]
