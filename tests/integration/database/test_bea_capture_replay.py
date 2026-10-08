"""Real PostgreSQL BEA regional accounts capture-to-gold contract.

Covers: ETL-058
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Callable

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.bea.schema import REQUIRED_RELATIONS, ensure_bea_schema
from data_ingestion_toolbox.glossary.harvest import (
    Publisher,
    harvest_publisher,
    process_pending_events,
)
from data_ingestion_toolbox.quality.sources import bea_table_reconciliation
from tests.support import bea

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def bea_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return bea.reviewed_warehouse(postgres_connection_factory, request)


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


def _rezip(code: str, transform) -> bytes:
    source = zipfile.ZipFile(io.BytesIO(bea.fixture_bytes(code)))
    name = source.namelist()[0]
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as target:
        target.writestr(name, transform(source.read(name)))
    return out.getvalue()


def test_tables_reach_gold_with_release_dates_and_dollar_bases(bea_warehouse) -> None:
    """Covers: ETL-058 — income and GDP lines at three grains, by release."""
    factory = bea_warehouse
    for code in ("CAINC1", "CAGDP1", "CAGDP2"):
        _run, facts, published = bea.run_to_gold(factory, code)
        assert facts > 0 and published == 1
    kent = _rows(
        factory,
        """
        SELECT metric_key, value::TEXT, unit, dollar_basis, release_key, geo_level
        FROM gold_bea.observation_latest
        WHERE geo_id = 'state:10|county:001' AND year = 2024 AND metric_key IN ('CAINC1:3', 'CAGDP1:1', 'CAGDP1:3')
        ORDER BY metric_key
        """,
    )
    assert kent == [
        (
            "CAGDP1:1",
            "9069233",
            "Thousands of chained 2017 dollars",
            "chained_dollars",
            "2026-02-05",
            "COUNTY",
        ),
        (
            "CAGDP1:3",
            "11652246",
            "Thousands of dollars",
            "current_dollars",
            "2026-02-05",
            "COUNTY",
        ),
        (
            "CAINC1:3",
            "55474",
            "Dollars",
            "per_capita_current_dollars",
            "2026-02-05",
            "COUNTY",
        ),
    ]
    withheld = _rows(
        factory,
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE value IS NULL) FROM gold_bea.observation_latest WHERE value_status = 'withheld'",
    )
    assert withheld[0][0] > 0 and withheld[0][0] == withheld[0][1]
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_bea.observation_latest WHERE geo_id LIKE 'state:51|county:9%' OR geo_id LIKE 'state:98%'",
    ) == [(0,)]


def test_the_publisher_names_each_line_with_its_unit_and_harvests(
    bea_warehouse,
) -> None:
    """Covers: ETL-058 — one metric per table and line; chained and current differ."""
    factory = bea_warehouse
    bea.run_to_gold(factory, "CAINC1")
    bea.run_to_gold(factory, "CAGDP1")
    published = dict(
        _rows(
            factory,
            "SELECT source_object_key, metric_display_name FROM gold_bea.metric_publisher",
        )
    )
    assert set(published) == {
        "CAINC1:1",
        "CAINC1:2",
        "CAINC1:3",
        "CAGDP1:1",
        "CAGDP1:3",
    }
    assert "chained 2017 dollars" in published["CAGDP1:1"]
    assert "chained" not in published["CAGDP1:3"]
    grains = _rows(
        factory, "SELECT DISTINCT valid_geo_grains FROM gold_bea.metric_publisher"
    )
    assert grains == [(["COUNTY", "NATIONAL", "STATE"],)]
    assert harvest_publisher(factory, Publisher("gold_bea")) > 0
    assert process_pending_events(factory) >= 1
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_glossary.dim_metric WHERE source_code = 'BEA'",
    ) == [(5,)]


def test_a_rerun_adds_nothing_and_a_new_release_is_kept_beside_the_old(
    bea_warehouse,
) -> None:
    """Covers: ETL-058 — the same bytes change nothing; a revised release is a second row."""
    factory = bea_warehouse
    first, _f, _p = bea.run_to_gold(factory, "CAINC1")
    second, _f, _p = bea.run_to_gold(factory, "CAINC1")
    count = _rows(factory, "SELECT COUNT(*) FROM gold_bea.observation_latest")

    def revise(content: bytes) -> bytes:
        content = content.replace(
            b"Last updated: February 5, 2026", b"Last updated: November 19, 2026"
        )
        return content.replace(b",55474", b",55500")

    third, _f, _p = bea.run_to_gold(
        factory,
        "CAINC1",
        client=bea.FixtureClient(
            {"CAINC1": httpx.Response(200, content=_rezip("CAINC1", revise))}
        ),
    )
    history = _rows(
        factory,
        """
        SELECT revision.value::TEXT, revision.release_key, capture.payload_checksum, revision.run_id::TEXT
        FROM gold_bea.observation_revision AS revision
        JOIN raw_capture.response_capture AS capture ON capture.capture_id = revision.capture_id
        WHERE revision.metric_key = 'CAINC1:3' AND revision.geo_id = 'state:10|county:001' AND revision.year = 2024
        ORDER BY revision.retrieved_at
        """,
    )
    assert [row[3] for row in history] == [str(first), str(second), str(third)]
    assert history[0][2] == history[1][2] != history[2][2]
    assert [row[1] for row in history] == ["2026-02-05", "2026-02-05", "2026-11-19"]
    assert _rows(
        factory,
        """
        SELECT value::TEXT, release_key FROM gold_bea.observation_latest
        WHERE metric_key = 'CAINC1:3' AND geo_id = 'state:10|county:001' AND year = 2024
        """,
    ) == [("55500", "2026-11-19")]
    assert _rows(factory, "SELECT COUNT(*) FROM gold_bea.observation_latest") == count


def test_a_malformed_file_is_quarantined_and_the_ledger_rule_runs(
    bea_warehouse,
) -> None:
    """Covers: ETL-058 — a refused file is held back; DQ-BEA-002 passes, then catches a loss."""
    factory = bea_warehouse

    def outcome():
        reader = factory()
        try:
            with reader.cursor() as cursor:
                return bea_table_reconciliation(cursor, {})[0]
        finally:
            reader.close()

    assert outcome().result == "not_applicable"
    no_release = _rezip(
        "CAINC1", lambda content: content.replace(b"Last updated:", b"Updated:")
    )
    run_id, facts, published = bea.run_to_gold(
        factory,
        "CAINC1",
        client=bea.FixtureClient({"CAINC1": httpx.Response(200, content=no_release)}),
    )
    assert (facts, published) == (0, 0)
    assert _rows(
        factory,
        "SELECT status FROM control.bea_table_capture WHERE run_id = %s",
        (str(run_id),),
    ) == [("quarantined",)]
    assert _rows(
        factory,
        "SELECT error_code FROM silver_bea.observation_quarantine WHERE run_id = %s",
        (str(run_id),),
    ) == [("release_date_missing",)]
    bea.run_to_gold(factory, "CAGDP1")
    passed = outcome()
    assert (passed.result, passed.observed_count) == ("pass", 0)
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_bea.fact_observation WHERE geo_id = 'state:10|county:001'"
            )
            cursor.execute(
                "DELETE FROM silver_bea.observation_revision WHERE geo_id = 'state:10|county:001'"
            )
        writer.commit()
    finally:
        writer.close()
    failed = outcome()
    assert (failed.result, failed.observed_count) == ("fail", 1)

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_bea_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]
