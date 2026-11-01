"""The ACS silver transform reads a large year in slices and resumes, against real PostgreSQL.

Covers: ETL-082, ETL-087
"""

from __future__ import annotations

import itertools
import json
from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import polars as pl
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
    """Remove the year's facts and the record of which slices produced them."""
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_census.fact_demographics WHERE estimate_year = %s",
                (YEAR,),
            )
            cursor.execute(
                "DELETE FROM silver_census.transform_slice_state WHERE year = %s",
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
            # A slice state left by an interrupted earlier session would make
            # this session's first transform skip what it must write.
            cursor.execute(
                "DELETE FROM silver_census.transform_slice_state WHERE year = %s",
                (YEAR,),
            )
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
                "DELETE FROM silver_census.transform_slice_state WHERE year = %s",
                (YEAR,),
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
    """Covers: ETL-082 â€” slicing by dataset, level and state changes no fact."""
    factory = two_slice_year
    expected_changed = (len(SEEDED_PLACES) + 1) * 2

    assert transform.transform_census_to_silver() == expected_changed
    whole_year = _facts(factory)
    assert len(whole_year) == expected_changed

    _delete_facts(factory)
    monkeypatch.setattr(transform, "_YEAR_SLICE_ROW_THRESHOLD", 0)
    fetched = _record_fetches(monkeypatch)
    assert transform.transform_census_to_silver() == expected_changed
    # Slices run concurrently, so they are read in no fixed order.
    assert sorted(fetched) == ["acs5/place/10", "acs5/state/10"]
    assert _facts(factory) == whole_year


def _range_bounds(key: str) -> tuple[str, str]:
    low, high = key.rsplit("/[", 1)[1].rstrip(")").split(",")
    return low, high


def test_an_oversized_slice_read_as_code_ranges_writes_the_same_facts(
    two_slice_year, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: ETL-082 — a state slice read as place-code ranges changes no fact."""
    factory = two_slice_year
    expected_changed = (len(SEEDED_PLACES) + 1) * 2
    monkeypatch.setattr(transform, "_YEAR_SLICE_ROW_THRESHOLD", 0)
    assert transform.transform_census_to_silver() == expected_changed
    by_state = _facts(factory)

    _delete_facts(factory)
    # The place slice is over the threshold and is cut at its place codes
    # into about four ranges; the state level is never split.
    place_rows = _rows(
        factory,
        "SELECT COUNT(*) FROM silver_census.observation_revision "
        "WHERE year = %s AND geo_level = 'place'",
        (YEAR,),
    )[0][0]
    monkeypatch.setattr(transform, "_SLICE_SPLIT_ROW_THRESHOLD", place_rows // 4)
    fetched = _record_fetches(monkeypatch)
    assert transform.transform_census_to_silver() == expected_changed
    assert "acs5/state/10" in fetched
    # Rows with no place code are read as their own part (none here).
    assert "acs5/place/10/[null]" in fetched
    ranges = sorted(
        _range_bounds(key)
        for key in fetched
        if key not in {"acs5/state/10", "acs5/place/10/[null]"}
    )
    assert 3 <= len(ranges) <= 5
    # Contiguous, and open at both ends: every place code is in one range.
    assert ranges[0][0] == "" and ranges[-1][1] == ""
    ranges.sort(key=lambda bounds: bounds[0])
    assert all(a[1] == b[0] for a, b in itertools.pairwise(ranges))
    assert _facts(factory) == by_state

    # The next run reuses the ranges the ledger records, so their
    # fingerprints hold and nothing is read again.
    fetched.clear()
    assert transform.transform_census_to_silver() == 0
    assert fetched == []


def test_a_slice_read_that_runs_too_long_is_read_again_in_halves(
    two_slice_year, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: ETL-082 — a timed-out read splits, logs, finishes, and stays split."""
    factory = two_slice_year
    monkeypatch.setattr(transform, "_YEAR_SLICE_ROW_THRESHOLD", 0)
    real = transform._fetch_raw_frame
    fetched: list[str] = []

    def place_read_times_out(hook, year, raw_slice=transform.WHOLE_YEAR):
        fetched.append(raw_slice.key)
        if raw_slice.key == "acs5/place/10":
            raise transform.SliceReadTimeout(raw_slice.key)
        return real(hook, year, raw_slice)

    monkeypatch.setattr(transform, "_fetch_raw_frame", place_read_times_out)
    assert transform.transform_census_to_silver() == (len(SEEDED_PLACES) + 1) * 2
    assert fetched[0] == "acs5/place/10" or "acs5/place/10" in fetched
    assert "acs5/place/10/[null]" in fetched
    halves = sorted(
        key
        for key in fetched
        if key.startswith("acs5/place/10/") and key != "acs5/place/10/[null]"
    )
    assert len(halves) == 2
    middle = _range_bounds(halves[0])[1] or _range_bounds(halves[1])[1]
    assert sorted(_range_bounds(key) for key in halves) == [
        ("", middle),
        (middle, ""),
    ]
    assert len(_facts(factory)) == (len(SEEDED_PLACES) + 1) * 2
    assert _rows(
        factory,
        "SELECT slice_key FROM silver_census.transform_slice_state "
        "WHERE year = %s ORDER BY slice_key",
        (YEAR,),
    ) == [(key,) for key in sorted([*halves, "acs5/place/10/[null]", "acs5/state/10"])]

    # The next run plans the halves from the ledger and skips them unchanged,
    # rather than timing out on the whole slice again.
    fetched.clear()
    assert transform.transform_census_to_silver() == 0
    assert fetched == []


def test_a_raw_read_past_its_deadline_raises_rather_than_holding_the_run(
    two_slice_year, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: ETL-082 — the deadline spans the cursor's fetches, not one statement."""
    monkeypatch.setattr(transform, "_RAW_FETCH_TIMEOUT_SECONDS", 1e-9)
    hook = PostgresHookStub(two_slice_year)
    place = transform.RawSlice(dataset="acs5", geo_level="place", state_fips="10")
    with pytest.raises(transform.SliceReadTimeout, match="acs5/place/10"):
        transform._fetch_raw_frame(hook, YEAR, place)


def test_a_retry_under_the_same_key_resumes_after_the_last_finished_slice(
    two_slice_year, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: ETL-082 â€” a finished slice is not read again; an interrupted one is."""
    factory = two_slice_year
    monkeypatch.setattr(transform, "_YEAR_SLICE_ROW_THRESHOLD", 0)
    real_upsert = transform._upsert_silver_rows

    # Slices run concurrently, so the interruption is named by slice rather
    # than by its position: the state slice dies, the place slice finishes.
    def dies_on_the_state_slice(hook, df, load_batch_id, ingested_at):
        if "state" in set(df["geo_level"].to_list()):
            raise RuntimeError("Docker froze")
        return real_upsert(hook, df, load_batch_id, ingested_at)

    monkeypatch.setattr(transform, "_upsert_silver_rows", dies_on_the_state_slice)
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

    # A new run (or no key) skips every slice finished against the same
    # content (ETL-087), so a backfill resumes across runs.
    assert transform.transform_census_to_silver() == 0
    assert fetched == []
    assert len(_facts(factory)) == (len(SEEDED_PLACES) + 1) * 2
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM silver_census.transform_checkpoint WHERE resume_key = %s",
        ("etl082-resume",),
    ) == [(2,)]


def test_a_small_year_under_a_key_is_one_whole_year_checkpoint(
    two_slice_year, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: ETL-082 â€” a year under the threshold is read whole and checkpointed once."""
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


def test_a_backfill_run_transforms_only_its_year(
    two_slice_year, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: ETL-083 â€” a one-year run reads no other year's revisions."""
    factory = two_slice_year
    fetched = _record_fetches(monkeypatch)
    assert transform.transform_census_to_silver(years=[YEAR - 1]) == 0
    assert fetched == []
    assert _facts(factory) == []

    assert (
        transform.transform_census_to_silver(years=[YEAR])
        == (len(SEEDED_PLACES) + 1) * 2
    )
    # A year-scoped run reads its year one dataset at a time (ETL-100).
    assert fetched == ["acs5/*"]


def test_a_later_run_skips_unchanged_slices_and_replays_a_changed_one(
    two_slice_year, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: ETL-087 â€” the checkpoint is keyed by content, not by run."""
    factory = two_slice_year
    monkeypatch.setattr(transform, "_YEAR_SLICE_ROW_THRESHOLD", 0)
    assert (
        transform.transform_census_to_silver(resume_key="etl087-first")
        == (len(SEEDED_PLACES) + 1) * 2
    )
    fetched = _record_fetches(monkeypatch)

    # Another run, another key, the same content: nothing is read again.
    assert transform.transform_census_to_silver(resume_key="etl087-second") == 0
    assert fetched == []

    # The Bureau revises Delaware's income: a new capture of the state slice. Both of its
    # facts now name the new capture, and only that slice is read.
    revised = STATE_PAYLOAD.replace(b'"82174"', b'"82999"')
    run_id, _ = _capture(
        factory, CaptureControl(factory, source_code="CENSUS_ACS"), revised, "state"
    )
    try:
        assert transform.transform_census_to_silver(resume_key="etl087-third") == 2
        assert fetched == ["acs5/state/10"]
        assert ("state:10", "B19013_001", 82999) in {
            (geo_id, variable, int(value))
            for geo_id, variable, value, *_ in _facts(factory)
            if value is not None
        }
    finally:
        cleanup = factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM silver_census.fact_demographics WHERE estimate_year = %s",
                    (YEAR,),
                )
                cursor.execute(
                    "DELETE FROM silver_census.transform_checkpoint "
                    "WHERE resume_key LIKE 'etl087-%%'"
                )
                cursor.execute(
                    "DELETE FROM silver_census.observation_revision "
                    "WHERE capture_id IN (SELECT capture_id FROM raw_capture.response_capture "
                    "WHERE run_id = %s)",
                    (run_id,),
                )
                delete_capture_graph(cursor, [run_id])
            cleanup.commit()
        finally:
            cleanup.close()


def _slice_reference(meta: pl.DataFrame, geo: pl.DataFrame, raw_slice) -> str:
    return transform.TransformReference(meta, geo).for_slice(YEAR, raw_slice)


_META = pl.DataFrame(
    {
        "dataset": ["acs5", "acs5"],
        "year": [YEAR, YEAR],
        "variable_name": ["B01003_001E", "B19013_001E"],
        "variable_label": ["Total", "Median household income"],
        "variable_concept": [None, None],
        "universe": [None, None],
    }
)
_GEO = pl.DataFrame(
    {
        "geo_sk": [1, 2, 3],
        "geo_level": ["state", "place", "metro"],
        "geo_id": ["state:10", "state:10|place:01400", "cbsa:20100"],
    }
)


def test_a_slice_reference_is_only_the_rows_the_slice_reads() -> None:
    """Covers: ETL-087 — a slice's fingerprint folds in its own reference rows only."""
    place_10 = transform.RawSlice(dataset="acs5", geo_level="place", state_fips="10")
    before = _slice_reference(_META, _GEO, place_10)
    assert before == _slice_reference(_META.reverse(), _GEO.reverse(), place_10)

    unrelated_geo = pl.concat(
        [
            _GEO,
            pl.DataFrame(
                {
                    "geo_sk": [4, 5, 6],
                    "geo_level": ["metro", "tract", "place"],
                    "geo_id": [
                        "cbsa:99999",
                        "state:10|county:001|tract:000100",
                        "state:11|place:50000",
                    ],
                }
            ),
        ]
    )
    unrelated_meta = pl.concat(
        [
            _META,
            pl.DataFrame(
                {
                    "dataset": ["acs5", "acs1"],
                    "year": [YEAR - 1, YEAR],
                    "variable_name": ["B25001_001E", "B25001_001E"],
                    "variable_label": ["Housing units", "Housing units"],
                    "variable_concept": [None, None],
                    "universe": [None, None],
                }
            ),
        ]
    )
    # Another level, another state, another year and another dataset.
    assert _slice_reference(unrelated_meta, unrelated_geo, place_10) == before

    # A place newly carried in its own state could map a row ledgered unmapped.
    own_state = pl.concat(
        [
            _GEO,
            pl.DataFrame(
                {
                    "geo_sk": [7],
                    "geo_level": ["place"],
                    "geo_id": ["state:10|place:01530"],
                }
            ),
        ]
    )
    assert _slice_reference(_META, own_state, place_10) != before
    # A metro slice reads every metro.
    metro = transform.RawSlice(dataset="acs5", geo_level="metro", state_fips=None)
    assert _slice_reference(_META, unrelated_geo, metro) != _slice_reference(
        _META, _GEO, metro
    )


def _seed_variable(factory, dataset: str, year: int, table_id: str) -> None:
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "INSERT INTO raw_census.acs_variables "
                "(dataset, year, variable_name, table_id, label, concept, predicate_type) "
                "VALUES (%s, %s, %s, %s, 'Estimate!!Total', 'Fixture', 'int') "
                "ON CONFLICT DO NOTHING",
                (dataset, year, f"{table_id}_001E", table_id),
            )
        writer.commit()
    finally:
        writer.close()


def _delete_variables(factory, table_id: str) -> None:
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "DELETE FROM raw_census.acs_variables WHERE table_id = %s",
                (table_id,),
            )
        writer.commit()
    finally:
        writer.close()


def test_an_unrelated_reference_change_leaves_a_finished_slice_skipped(
    two_slice_year, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: ETL-087 — new geographies and variables elsewhere replay nothing.

    The October 2026 monthly run re-transformed every slice of 1990-2021
    because the fingerprint hashed the whole reference: new metros and a
    widened 2009 tract table list invalidated years they do not touch.
    """
    factory = two_slice_year
    monkeypatch.setattr(transform, "_YEAR_SLICE_ROW_THRESHOLD", 0)
    assert transform.transform_census_to_silver() == (len(SEEDED_PLACES) + 1) * 2
    fetched = _record_fetches(monkeypatch)

    writer = factory()
    try:
        with writer.cursor() as cursor:
            seed_geography(
                cursor,
                geo_type="state",
                state_fips="11",
                vintage=YEAR,
                name="Elsewhere",
            )
        writer.commit()
    finally:
        writer.close()
    _seed_variable(factory, "acs5", YEAR - 1, "B99901")
    _seed_variable(factory, "acs1", YEAR, "B99901")
    try:
        assert transform.transform_census_to_silver() == 0
        assert fetched == []
    finally:
        _delete_variables(factory, "B99901")
        cleanup = factory()
        try:
            with cleanup.cursor() as cursor:
                delete_geography(cursor, "state:11")
            cleanup.commit()
        finally:
            cleanup.close()


def test_a_wider_table_list_for_the_year_replays_its_slices(
    two_slice_year, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: ETL-087 — new variables for a slice's dataset and year replay it."""
    factory = two_slice_year
    monkeypatch.setattr(transform, "_YEAR_SLICE_ROW_THRESHOLD", 0)
    assert transform.transform_census_to_silver() == (len(SEEDED_PLACES) + 1) * 2
    fetched = _record_fetches(monkeypatch)

    _seed_variable(factory, "acs5", YEAR, "B99902")
    try:
        # Replayed, and an identical replay changes no fact.
        assert transform.transform_census_to_silver() == 0
        assert sorted(fetched) == ["acs5/place/10", "acs5/state/10"]
    finally:
        _delete_variables(factory, "B99902")


def test_a_legacy_fingerprint_still_matching_is_restamped_not_reread(
    two_slice_year, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: ETL-087 — the first run after the per-slice reference re-reads no unchanged slice.

    Ledger rows written before it carry the whole-reference fingerprint. One
    that still matches is skipped and rewritten in the new shape with its
    serving columns untouched; one that does not is read again.
    """
    factory = two_slice_year
    monkeypatch.setattr(transform, "_YEAR_SLICE_ROW_THRESHOLD", 0)
    assert transform.transform_census_to_silver() == (len(SEEDED_PLACES) + 1) * 2

    hook = PostgresHookStub(factory)
    legacy = transform.legacy_transform_context(
        transform._load_variable_metadata(hook), transform._load_geo_dim(hook)
    )
    place, state = (
        transform.RawSlice(dataset="acs5", geo_level=level, state_fips="10")
        for level in ("place", "state")
    )
    current = transform._finished_fingerprints(hook, YEAR)
    writer = factory()
    try:
        with writer.cursor() as cursor:
            # The place slice as the old code stamped it; the state slice as
            # the old code stamped it before its content changed.
            cursor.execute(
                "UPDATE silver_census.transform_slice_state SET content_fingerprint = %s "
                "WHERE year = %s AND slice_key = %s",
                (
                    transform.slice_fingerprint(hook, YEAR, place, legacy),
                    YEAR,
                    place.key,
                ),
            )
            cursor.execute(
                "UPDATE silver_census.transform_slice_state SET content_fingerprint = %s "
                "WHERE year = %s AND slice_key = %s",
                (
                    transform.fingerprint_of(legacy, YEAR, state, (0, None)),
                    YEAR,
                    state.key,
                ),
            )
        writer.commit()
    finally:
        writer.close()
    serving_columns = _rows(
        factory,
        "SELECT slice_key, rows_changed, completed_at, last_changed_at "
        "FROM silver_census.transform_slice_state WHERE year = %s AND slice_key = %s",
        (YEAR, place.key),
    )

    fetched = _record_fetches(monkeypatch)
    assert transform.transform_census_to_silver() == 0
    assert fetched == [state.key]
    assert transform._finished_fingerprints(hook, YEAR) == current
    assert (
        _rows(
            factory,
            "SELECT slice_key, rows_changed, completed_at, last_changed_at "
            "FROM silver_census.transform_slice_state "
            "WHERE year = %s AND slice_key = %s",
            (YEAR, place.key),
        )
        == serving_columns
    )
