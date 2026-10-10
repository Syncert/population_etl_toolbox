"""The ACS silver transform reads a large year in slices and resumes, against real PostgreSQL.

Covers: ETL-082
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.capture import (
    CaptureControl,
    ResponseCapture,
    persist_response_capture,
)
from data_ingestion_toolbox.census_acs.silver_census import transform
from data_ingestion_toolbox.census_acs.silver_census.replay import (
    replay_census_capture,
)
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.postgres import PostgresHookStub
from tests.support.warehouse_scope import delete_capture_graph

pytestmark = [pytest.mark.integration, pytest.mark.database]

PLACE_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "census"
    / "acs5_2023_place_10.json"
)
#: A year no other node uses, so the transform's whole-relation pass reads
#: only these rows.
YEAR = 2089
#: acs5's period starts four years before the estimate year.
TIME_SK, TIME_DATE = 20850101, "2085-01-01"
SEEDED_PLACES = ("01400", "01530", "01660")
#: Delaware's own row in the same shape the Bureau answers a state request.
STATE_PAYLOAD = json.dumps(
    [
        ["B01003_001E", "B01003_001M", "B19013_001E", "B19013_001M", "state"],
        ["1031890", "-555555555", "82174", "1100", "10"],
    ]
).encode()
RESUME_KEYS = ("etl082-resume", "etl082-whole")


def _capture(
    factory: Callable[[], connection],
    control: CaptureControl,
    payload: bytes,
    geo_level: str,
) -> tuple[UUID, UUID]:
    run_id = control.start_run(
        watermark={"dataset": "acs5", "year": YEAR, "geo_level": geo_level}
    )
    parameters = {
        "get": "B01003_001E,B01003_001M,B19013_001E,B19013_001M",
        "dataset": "acs5",
        "year": YEAR,
        "geo_level": geo_level,
        "for": f"{geo_level}:*",
        "in": "state:10",
    }
    endpoint = f"https://api.census.gov/data/{YEAR}/acs/acs5"
    request = control.start_request(
        run_id=run_id, endpoint=endpoint, parameters=parameters
    )
    capture_id = uuid4()
    persist_response_capture(
        factory,
        ResponseCapture(
            capture_id=capture_id,
            request_id=request.request_id,
            run_id=run_id,
            source_code="CENSUS_ACS",
            endpoint=endpoint,
            request_parameters=parameters,
            retrieved_at=datetime.now(timezone.utc),
            http_status=200,
            response_headers={"content-type": "application/json"},
            media_type="application/json",
            payload=payload,
            payload_schema_version="census-acs-array-v1",
            source_revision=str(YEAR),
        ),
    )
    control.finish_request(request.request_id, status="captured")
    control.finish_run(run_id, status="success")
    replay_census_capture(
        factory, capture_id=capture_id, dataset="acs5", year=YEAR, geo_level=geo_level
    )
    return run_id, capture_id


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


def _facts(factory: Callable[[], connection]) -> list[tuple]:
    return _rows(
        factory,
        """
        SELECT geo_id, variable_code, estimate_value, margin_of_error, value_status,
               capture_id::TEXT
        FROM silver_census.fact_demographics
        WHERE estimate_year = %s ORDER BY geo_id, variable_code
        """,
        (YEAR,),
    )


def _delete_facts(factory: Callable[[], connection]) -> None:
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_census.fact_demographics WHERE estimate_year = %s",
                (YEAR,),
            )
        writer.commit()
    finally:
        writer.close()


@pytest.fixture
def two_slice_year(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[Callable[[], connection]]:
    """A year holding a Delaware state slice and a Delaware place slice."""
    factory = postgres_connection_factory
    writer = factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, TIME_SK, TIME_DATE)
            seed_geography(
                cursor, geo_type="state", state_fips="10", vintage=YEAR, name="Delaware"
            )
            for place in SEEDED_PLACES:
                seed_geography(
                    cursor,
                    geo_type="place",
                    state_fips="10",
                    place_fips=place,
                    vintage=YEAR,
                    name=f"Fixture place {place}",
                )
        writer.commit()
    finally:
        writer.close()
    monkeypatch.setattr(transform, "_get_hook", lambda: PostgresHookStub(factory))
    monkeypatch.setattr(transform, "_get_approx_row_count", lambda _: 1)
    control = CaptureControl(factory, source_code="CENSUS_ACS")
    runs = [
        _capture(factory, control, PLACE_FIXTURE.read_bytes(), "place")[0],
        _capture(factory, control, STATE_PAYLOAD, "state")[0],
    ]
    yield factory
    cleanup = factory()
    try:
        with cleanup.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_census.transform_checkpoint WHERE resume_key = ANY(%s)",
                (list(RESUME_KEYS),),
            )
            cursor.execute(
                "DELETE FROM silver_census.fact_demographics WHERE estimate_year = %s",
                (YEAR,),
            )
            cursor.execute(
                "DELETE FROM silver_census.observation_revision WHERE year = %s",
                (YEAR,),
            )
            cursor.execute(
                "DELETE FROM silver_ref.geography_resolution "
                "WHERE provider_source = 'CENSUS_ACS' AND source_vintage = %s",
                (YEAR,),
            )
            delete_capture_graph(cursor, runs)
            for place in SEEDED_PLACES:
                delete_geography(cursor, f"state:10|place:{place}")
            delete_geography(cursor, "state:10")
            cursor.execute(
                "DELETE FROM silver_ref.dim_time WHERE time_sk = %s", (TIME_SK,)
            )
        cleanup.commit()
    finally:
        cleanup.close()


def _record_fetches(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    fetched: list[str] = []
    real = transform._fetch_raw_frame

    def recording(hook, year, raw_slice=transform.WHOLE_YEAR):
        if year == YEAR:
            fetched.append(raw_slice.key)
        return real(hook, year, raw_slice)

    monkeypatch.setattr(transform, "_fetch_raw_frame", recording)
    return fetched


def test_a_sliced_year_writes_exactly_the_facts_the_whole_year_writes(
    two_slice_year, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: ETL-082 — slicing by dataset, level and state changes no fact."""
    factory = two_slice_year
    expected_changed = (len(SEEDED_PLACES) + 1) * 2

    assert transform.transform_census_to_silver() == expected_changed
    whole_year = _facts(factory)
    assert len(whole_year) == expected_changed

    _delete_facts(factory)
    monkeypatch.setattr(transform, "_YEAR_SLICE_ROW_THRESHOLD", 0)
    fetched = _record_fetches(monkeypatch)
    assert transform.transform_census_to_silver() == expected_changed
    assert fetched == ["acs5/place/10", "acs5/state/10"]
    assert _facts(factory) == whole_year


def test_a_retry_under_the_same_key_resumes_after_the_last_finished_slice(
    two_slice_year, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: ETL-082 — a finished slice is not read again; an interrupted one is."""
    factory = two_slice_year
    monkeypatch.setattr(transform, "_YEAR_SLICE_ROW_THRESHOLD", 0)
    real_upsert = transform._upsert_silver_rows
    calls = {"n": 0}

    def dies_on_second_slice(hook, df, load_batch_id, ingested_at):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("Docker froze")
        return real_upsert(hook, df, load_batch_id, ingested_at)

    monkeypatch.setattr(transform, "_upsert_silver_rows", dies_on_second_slice)
    with pytest.raises(RuntimeError, match="Docker froze"):
        transform.transform_census_to_silver(resume_key="etl082-resume")
    checkpoints = _rows(
        factory,
        "SELECT year, slice_key, rows_changed FROM silver_census.transform_checkpoint "
        "WHERE resume_key = %s ORDER BY slice_key",
        ("etl082-resume",),
    )
    assert checkpoints == [(YEAR, "acs5/place/10", len(SEEDED_PLACES) * 2)]

    monkeypatch.setattr(transform, "_upsert_silver_rows", real_upsert)
    fetched = _record_fetches(monkeypatch)
    # Only the interrupted state slice is read again and written.
    assert transform.transform_census_to_silver(resume_key="etl082-resume") == 2
    assert fetched == ["acs5/state/10"]

    # Another attempt of the same run has nothing left to do.
    fetched.clear()
    assert transform.transform_census_to_silver(resume_key="etl082-resume") == 0
    assert fetched == []

    # A new run (or no key) replays every slice; equal facts change nothing.
    assert transform.transform_census_to_silver() == 0
    assert fetched == ["acs5/place/10", "acs5/state/10"]
    assert len(_facts(factory)) == (len(SEEDED_PLACES) + 1) * 2
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM silver_census.transform_checkpoint WHERE resume_key = %s",
        ("etl082-resume",),
    ) == [(2,)]


def test_a_small_year_under_a_key_is_one_whole_year_checkpoint(
    two_slice_year, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: ETL-082 — a year under the threshold is read whole and checkpointed once."""
    factory = two_slice_year
    fetched = _record_fetches(monkeypatch)
    assert (
        transform.transform_census_to_silver(resume_key="etl082-whole")
        == (len(SEEDED_PLACES) + 1) * 2
    )
    assert fetched == ["*"]
    assert _rows(
        factory,
        "SELECT slice_key FROM silver_census.transform_checkpoint "
        "WHERE resume_key = %s AND year = %s",
        ("etl082-whole", YEAR),
    ) == [("*",)]
