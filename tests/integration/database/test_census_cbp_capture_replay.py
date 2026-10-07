"""Real PostgreSQL County Business Patterns capture-to-gold contract.

Covers: ETL-062
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Callable

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_cbp.client import CbpPayloadError
from data_ingestion_toolbox.census_cbp.schema import (
    REQUIRED_RELATIONS,
    ensure_census_cbp_schema,
)
from data_ingestion_toolbox.glossary.harvest import (
    Publisher,
    harvest_publisher,
    process_pending_events,
)
from data_ingestion_toolbox.quality.sources import (
    cbp_file_reconciliation,
    cbp_sector_sum,
)
from tests.support import census_cbp as cbp

pytestmark = [pytest.mark.integration, pytest.mark.database]

KENT = "state:10|county:001"


@pytest.fixture
def cbp_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return cbp.reviewed_warehouse(postgres_connection_factory, request)


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


def _rezip(name: str, transform) -> bytes:
    source = zipfile.ZipFile(io.BytesIO(cbp.fixture_bytes(name)))
    member = source.namelist()[0]
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as target:
        target.writestr(member, transform(source.read(member)))
    return out.getvalue()


def test_files_reach_gold_with_flags_units_and_three_grains(cbp_warehouse) -> None:
    """Covers: ETL-062 — county, state and nation sectors at gold, each flag beside its value."""
    factory = cbp_warehouse
    for kind in ("county", "state", "nation"):
        _run, facts, published = cbp.run_to_gold(factory, kind, 2023)
        assert facts > 0 and published == 1
    kent = _rows(
        factory,
        """
        SELECT metric_key, value::TEXT, unit, noise_flag, geo_level, naics_label
        FROM gold_census_cbp.observation_latest
        WHERE geo_id = %s AND year = 2023 AND metric_key IN ('emp:total', 'est:total', 'ap:72')
        ORDER BY metric_key
        """,
        (KENT,),
    )
    assert kent == [
        (
            "ap:72",
            "217407",
            "thousands of dollars",
            "G",
            "COUNTY",
            "Accommodation and food services",
        ),
        ("emp:total", "61078", "employees", "G", "COUNTY", "Total for all sectors"),
        (
            "est:total",
            "4971",
            "establishments",
            None,
            "COUNTY",
            "Total for all sectors",
        ),
    ]
    grains = _rows(
        factory,
        "SELECT DISTINCT geo_level FROM gold_census_cbp.observation_latest ORDER BY 1",
    )
    assert grains == [("COUNTY",), ("NATIONAL",), ("STATE",)]
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_census_cbp.observation_latest WHERE geo_id LIKE 'state:10|county:999'",
    ) == [(0,)]


def test_a_withheld_cell_has_no_value_and_the_publisher_harvests(cbp_warehouse) -> None:
    """Covers: ETL-062 — 2016's D cells are withheld, not zero; one metric per measure and sector."""
    factory = cbp_warehouse
    cbp.run_to_gold(factory, "county", 2016)
    withheld = _rows(
        factory,
        """
        SELECT COUNT(*), COUNT(*) FILTER (WHERE value IS NULL), MIN(value_source), MAX(value_source)
        FROM gold_census_cbp.observation_latest WHERE value_status = 'withheld'
        """,
    )
    assert withheld[0][0] > 0 and withheld[0][0] == withheld[0][1]
    assert withheld[0][2:] == ("D:0", "D:0")
    published = dict(
        _rows(
            factory,
            "SELECT source_object_key, metric_display_name FROM gold_census_cbp.metric_publisher",
        )
    )
    assert published["emp:72"] == (
        "Employees in the pay period including March 12, Accommodation and food services (County Business Patterns)"
    )
    assert harvest_publisher(factory, Publisher("gold_census_cbp")) > 0
    assert process_pending_events(factory) >= 1
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_glossary.dim_metric WHERE source_code = 'CENSUS_CBP'",
    ) == [(len(published),)]


def test_a_rerun_adds_nothing_and_a_corrected_file_is_kept_beside_the_old(
    cbp_warehouse,
) -> None:
    """Covers: ETL-062 — the same bytes change nothing; a changed file keeps both checksums."""
    factory = cbp_warehouse
    first, _f, _p = cbp.run_to_gold(factory, "county", 2023)
    second, _f, _p = cbp.run_to_gold(factory, "county", 2023)
    count = _rows(factory, "SELECT COUNT(*) FROM gold_census_cbp.observation_latest")
    corrected = _rezip(
        "cbp23co",
        lambda content: content.replace(
            b'"10","001","------","G",61078', b'"10","001","------","G",61080'
        ),
    )
    third, _f, _p = cbp.run_to_gold(
        factory,
        "county",
        2023,
        client=cbp.FixtureClient({"cbp23co": httpx.Response(200, content=corrected)}),
    )
    history = _rows(
        factory,
        """
        SELECT revision.value::TEXT, capture.payload_checksum, revision.run_id::TEXT
        FROM gold_census_cbp.observation_revision AS revision
        JOIN raw_capture.response_capture AS capture ON capture.capture_id = revision.capture_id
        WHERE revision.metric_key = 'emp:total' AND revision.geo_id = %s
        ORDER BY revision.retrieved_at
        """,
        (KENT,),
    )
    assert [row[2] for row in history] == [str(first), str(second), str(third)]
    assert [row[0] for row in history] == ["61078", "61078", "61080"]
    assert history[0][1] == history[1][1] != history[2][1]
    assert (
        _rows(factory, "SELECT COUNT(*) FROM gold_census_cbp.observation_latest")
        == count
    )


def test_quality_rules_pass_the_fixtures_and_catch_a_loss(cbp_warehouse) -> None:
    """Covers: ETL-062 — DQ-CBP-002 and DQ-CBP-004 pass, then fail; a wrong container is held back."""
    factory = cbp_warehouse

    def outcome(executor):
        reader = factory()
        try:
            with reader.cursor() as cursor:
                return executor(cursor, {})[0]
        finally:
            reader.close()

    assert outcome(cbp_file_reconciliation).result == "not_applicable"
    for kind in ("county", "state", "nation"):
        cbp.run_to_gold(factory, kind, 2023)
    assert (
        outcome(cbp_file_reconciliation).result,
        outcome(cbp_sector_sum).result,
    ) == ("pass", "pass")
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "UPDATE silver_census_cbp.fact_observation SET value = value + 100000 "
                "WHERE measure = 'est' AND naics_key = '72' AND geo_id = %s",
                (KENT,),
            )
            cursor.execute(
                "DELETE FROM silver_census_cbp.observation_revision WHERE geo_id = %s AND naics_key = '72'",
                (KENT,),
            )
        writer.commit()
    finally:
        writer.close()
    failed = outcome(cbp_file_reconciliation)
    assert (failed.result, failed.observed_count) == ("fail", 1)
    over = outcome(cbp_sector_sum)
    assert (over.result, over.observed_count) == ("warn", 1)

    with pytest.raises(CbpPayloadError, match="not_a_zip"):
        cbp.run_to_gold(
            factory,
            "county",
            2022,
            client=cbp.FixtureClient(
                {"cbp22co": httpx.Response(200, content=b"<html>")}
            ),
        )

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_census_cbp_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]
