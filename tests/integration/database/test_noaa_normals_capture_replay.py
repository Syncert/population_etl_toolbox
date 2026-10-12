"""Real PostgreSQL NOAA climate normals capture-to-gold contract.

Covers: ETL-069
"""

from __future__ import annotations

import csv
import io
import tarfile
from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import (
    Publisher,
    harvest_publisher,
    process_pending_events,
)
from data_ingestion_toolbox.noaa_normals.client import NormalsPayloadError
from data_ingestion_toolbox.noaa_normals.schema import (
    REQUIRED_RELATIONS,
    ensure_noaa_normals_schema,
)
from data_ingestion_toolbox.quality.sources import (
    normals_file_reconciliation,
    normals_value_and_geography,
)
from tests.support import noaa_normals as normals

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def normals_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return normals.reviewed_warehouse(postgres_connection_factory, request)


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


def _station_csv(name: str) -> str:
    archive = tarfile.open(fileobj=io.BytesIO(normals.fixture_bytes()), mode="r:gz")
    member = next(member for member in archive if member.name.endswith(name))
    return archive.extractfile(member).read().decode()


def _rebuilt_archive(replace: dict[str, bytes]) -> bytes:
    """The fixture archive with some station files' bytes replaced."""
    source = tarfile.open(fileobj=io.BytesIO(normals.fixture_bytes()), mode="r:gz")
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as target:
        for member in source:
            content = source.extractfile(member).read() if member.isfile() else b""
            name = member.name.rsplit("/", 1)[-1]
            if name in replace:
                content = replace[name]
                member.size = len(content)
            target.addfile(member, io.BytesIO(content))
    return buffer.getvalue()


def test_stations_are_placed_in_counties_and_averaged(normals_warehouse) -> None:
    """Covers: ETL-069 — point-in-polygon placement with its vintage; S/R station means per county."""
    factory = normals_warehouse
    run_id, status, written, published = normals.run_to_gold(factory)
    assert (status, written, published) == ("captured", 61, 1)
    placed = dict(
        _rows(
            factory,
            """
            SELECT station_id, COALESCE(geo_id, geography_reason) FROM silver_noaa_normals.station
            WHERE run_id = %s
            """,
            (str(run_id),),
        )
    )
    assert placed == {
        "USC00072730": normals.KENT,
        "US1DEKN0001": normals.KENT,
        "USC00076410": normals.NEW_CASTLE,
        "USC00079605": normals.NEW_CASTLE,
        "USW00013781": normals.NEW_CASTLE,
        "USC00073595": normals.SUSSEX,
        "USC00075320": normals.SUSSEX,
        "USW00013764": normals.SUSSEX,
        "USC00049063": "outside_counties",
        "CAW00064757": "outside_counties",
        "RQC00662801": "outside_counties",
    }
    assert _rows(
        factory,
        "SELECT DISTINCT boundary_vintage FROM silver_noaa_normals.station WHERE run_id = %s",
        (str(run_id),),
    ) == [(normals.BOUNDARY_VINTAGE,)]
    assert _rows(
        factory,
        "SELECT boundary_vintage FROM control.noaa_normals_file WHERE run_id = %s",
        (str(run_id),),
    ) == [(normals.BOUNDARY_VINTAGE,)]
    served = {
        (geo_id, metric): (value, count, stations)
        for geo_id, metric, value, count, stations in _rows(
            factory,
            """
            SELECT geo_id, metric_key, value, station_count, station_ids FROM gold_noaa_normals.observation_latest
            WHERE metric_key IN ('annual_mean_temperature', 'annual_precipitation')
            """,
        )
    }
    assert served[(normals.NEW_CASTLE, "annual_mean_temperature")] == (
        Decimal("54.87"),
        3,
        "USC00076410,USC00079605,USW00013781",
    )
    assert served[(normals.SUSSEX, "annual_mean_temperature")][0] == Decimal("57.17")
    # Felton's precipitation normal is estimated (E): kept, never averaged in.
    assert served[(normals.KENT, "annual_precipitation")] == (
        Decimal("47.61"),
        1,
        "USC00072730",
    )
    assert {geo_id for geo_id, _metric in served} == {
        normals.KENT,
        normals.NEW_CASTLE,
        normals.SUSSEX,
    }
    assert _rows(
        factory,
        """
        SELECT DISTINCT year, period_start::TEXT, period_end::TEXT FROM gold_noaa_normals.observation_latest
        """,
    ) == [(2020, "1991-01-01", "2020-12-31")]


def test_an_x_flag_is_kept_apart_from_a_true_zero(normals_warehouse) -> None:
    """Covers: ETL-069 — `X` keeps NCEI's rounded zero as valid with its flag, so it reads apart from a true zero."""
    factory = normals_warehouse
    normals.run_to_gold(factory)
    rows = _rows(
        factory,
        """
        SELECT station_id, value, value_status, measurement_flag FROM gold_noaa_normals.station_observation
        WHERE variable IN ('ANN-HTDD-NORMAL', 'ANN-CLDD-NORMAL') AND value = 0
        ORDER BY station_id
        """,
    )
    assert rows == [
        ("RQC00662801", Decimal("0.0"), "valid", "X"),
        ("USC00049063", Decimal("0.0"), "valid", "X"),
    ]


def test_withheld_flags_publish_no_number_and_a_rerun_adds_nothing(
    normals_warehouse,
) -> None:
    """Covers: ETL-069 — `M` and `V` are null with the flag; unchanged bytes replay nothing."""
    factory = normals_warehouse
    header, row = list(csv.reader(io.StringIO(_station_csv("USC00072730.csv"))))
    for variable, flag, sentinel in (
        ("ANN-TAVG-NORMAL", "M", "-9999"),
        ("ANN-HTDD-NORMAL", "V", "-7777"),
    ):
        row[header.index(variable)] = sentinel
        row[header.index(f"meas_flag_{variable}")] = flag
    rewritten = io.StringIO()
    csv.writer(rewritten, quoting=csv.QUOTE_ALL, lineterminator="\n").writerows(
        [header, row]
    )
    changed = _rebuilt_archive({"USC00072730.csv": rewritten.getvalue().encode()})
    run_id, status, _written, published = normals.run_to_gold(
        factory, client=normals.FixtureClient(changed)
    )
    assert (status, published) == ("captured", 1)
    assert _rows(
        factory,
        """
        SELECT variable, value, value_status, missing_reason FROM silver_noaa_normals.station_normal
        WHERE run_id = %s AND station_id = 'USC00072730' AND variable IN ('ANN-TAVG-NORMAL', 'ANN-HTDD-NORMAL')
        ORDER BY variable
        """,
        (str(run_id),),
    ) == [
        ("ANN-HTDD-NORMAL", None, "not_applicable", "too_cold_to_compute"),
        ("ANN-TAVG-NORMAL", None, "missing", "missing"),
    ]
    # Kent's only standard temperature station is withheld: no county figure, not a zero.
    assert _rows(
        factory,
        """
        SELECT COUNT(*) FROM gold_noaa_normals.observation_latest
        WHERE geo_id = %s AND metric_key = 'annual_mean_temperature' AND run_id = %s
        """,
        (normals.KENT, str(run_id)),
    ) == [(0,)]
    _run, status, written, published = normals.run_to_gold(
        factory, client=normals.FixtureClient(changed)
    )
    assert (status, written, published) == ("unchanged", 0, 0)


def test_a_refused_archive_and_the_rules(normals_warehouse) -> None:
    """Covers: ETL-069 — a non-archive fails capture; DQ-NOAA-002 and -004 behave, then catch a fault."""
    factory = normals_warehouse
    assert _outcome(factory, normals_file_reconciliation).result == "not_applicable"
    with pytest.raises(NormalsPayloadError, match="not_an_archive"):
        normals.run_to_gold(factory, client=normals.FixtureClient(b"<html>gone</html>"))
    with pytest.raises(NormalsPayloadError, match="unexpected_header"):
        normals.run_to_gold(
            factory,
            client=normals.FixtureClient(
                _rebuilt_archive({"CAW00064757.csv": b'"A","B"\n"1","2"\n'})
            ),
        )
    assert _rows(factory, "SELECT COUNT(*) FROM control.noaa_normals_file") == [(0,)]
    run_id, _status, _written, _published = normals.run_to_gold(factory)
    assert _outcome(factory, normals_file_reconciliation).result == "pass"
    coverage = _outcome(factory, normals_value_and_geography)
    # Tuolumne Meadows is a U.S. station in a county this test does not seed;
    # the Canadian and Puerto Rico stations are outside by design.
    assert (coverage.result, coverage.observed_count) == ("warn", 1)
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_noaa_normals.station_normal WHERE run_id = %s",
                (str(run_id),),
            )
            cursor.execute(
                "DELETE FROM silver_noaa_normals.station WHERE run_id = %s",
                (str(run_id),),
            )
        writer.commit()
    finally:
        writer.close()
    lost = _outcome(factory, normals_file_reconciliation)
    assert (lost.result, lost.observed_count) == ("fail", 1)

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_noaa_normals_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]


def test_a_malformed_station_file_is_quarantined(normals_warehouse) -> None:
    """Covers: ETL-069 — one unreadable station file is quarantined; the rest publish."""
    factory = normals_warehouse
    broken = _rebuilt_archive(
        {
            "USC00075320.csv": b'"STATION","LATITUDE","LONGITUDE","ELEVATION","NAME"\n"USC00075320","north","-75.1","3.0","LEWES"\n'
        }
    )
    run_id, status, written, published = normals.run_to_gold(
        factory, client=normals.FixtureClient(broken)
    )
    assert (status, written, published) == ("captured", 55, 1)
    assert _rows(
        factory,
        "SELECT error_code FROM silver_noaa_normals.quarantine WHERE run_id = %s",
        (str(run_id),),
    ) == [("unreadable_value",)]
    assert _outcome(factory, normals_file_reconciliation).result == "pass"


def test_the_harvest_names_six_measures(normals_warehouse) -> None:
    """Covers: ETL-069 — six county metrics, each a derived summary."""
    factory = normals_warehouse
    normals.run_all(factory)
    published = dict(
        _rows(
            factory,
            "SELECT source_object_key, measure_kind FROM gold_noaa_normals.metric_publisher",
        )
    )
    assert set(published.values()) == {"derived_summary"} and len(published) == 6
    assert harvest_publisher(factory, Publisher("gold_noaa_normals")) > 0
    assert process_pending_events(factory) >= 1
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_glossary.dim_metric WHERE source_code = 'NOAA_NORMALS'",
    ) == [(6,)]
