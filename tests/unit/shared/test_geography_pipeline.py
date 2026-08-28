"""Canonical identity and lossless offline geography replay contracts."""

from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import httpx
import pytest

from data_ingestion_toolbox.silver_ref.geography_contract import (
    canonical_geo_id,
    resolve_provider_geography,
)
from data_ingestion_toolbox.silver_ref.geography_pipeline import (
    parse_gazetteer_capture,
    parse_legacy_county_gazetteer_capture,
)
from data_ingestion_toolbox.silver_ref import geography_pipeline

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("kind", "kwargs", "expected"),
    [
        ("nation", {}, "us:1"),
        ("state", {"state_fips": 1}, "state:01"),
        ("county", {"state_fips": 1, "county_fips": 3}, "state:01|county:003"),
        ("place", {"state_fips": 1, "place_fips": 700}, "state:01|place:00700"),
    ],
)
def test_canonical_ids_preserve_exact_zero_padded_codes(kind, kwargs, expected) -> None:
    """Covers: ETL-002 — canonical identities preserve exact padded codes."""
    assert canonical_geo_id(kind, **kwargs) == expected


def test_place_is_not_constructed_as_a_county_child_or_from_a_name() -> None:
    """Covers: ETL-003 — ambiguous hierarchy and name-derived codes are rejected."""
    with pytest.raises(ValueError, match="sibling"):
        canonical_geo_id(
            "place", state_fips="55", county_fips="025", place_fips="53000"
        )
    with pytest.raises(ValueError, match="digits"):
        canonical_geo_id("place", state_fips="WI", place_fips="Madison")


@pytest.mark.parametrize(
    "provider", ["CENSUS_PEP", "CDC", "USDA_NASS", "BLS", "CENSUS_ACS"]
)
def test_planned_and_existing_providers_resolve_counties_by_exact_code(
    provider,
) -> None:
    """Covers: ETL-024 — provider codes map deterministically to shared identities."""
    result = resolve_provider_geography(
        provider, "county", state_fips="55", county_fips="25"
    )
    assert result.status == "resolved"
    assert result.method == "exact_code"
    assert result.geo_id == "state:55|county:025"


def test_fbi_agency_remains_agency_without_an_evidence_backed_bridge() -> None:
    """Covers: ETL-003 — agency identity cannot silently become a place."""
    result = resolve_provider_geography("FBI", "agency", agency_code="WI0130100")
    assert result.geo_id == "agency:WI0130100"
    assert result.geo_id != "state:55|place:48000"


def test_pep_place_is_exact_while_unsupported_or_malformed_codes_are_quarantined() -> (
    None
):
    """Covers: ETL-002, ETL-003 — planned providers resolve or report exact failures."""
    place = resolve_provider_geography(
        "CENSUS_PEP", "place", state_fips="6", place_fips="44000"
    )
    assert place.geo_id == "state:06|place:44000"
    assert place.status == "resolved"

    unsupported = resolve_provider_geography(
        "CDC", "place", state_fips="06", place_fips="44000"
    )
    assert unsupported.status == "unsupported"
    assert unsupported.reason_code == "unsupported_type"

    malformed = resolve_provider_geography(
        "USDA_NASS", "county", state_fips="WI", county_fips="025"
    )
    assert malformed.status == "unmapped"
    assert malformed.reason_code == "invalid_exact_code"


def _gazetteer_zip(text: str) -> bytes:
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w") as archive:
        archive.writestr("fixture.txt", text)
    return payload.getvalue()


def test_gazetteer_capture_replays_places_and_preserves_source_attributes() -> None:
    """Covers: ETL-024 — offline replay preserves typed geography attributes."""
    payload = _gazetteer_zip(
        "GEOID\tGEOIDFQ\tNAME\tUSPS\tLSAD\tFUNCSTAT\tALAND\tAWATER\tINTPTLAT\tINTPTLONG\n"
        "5505300\t1600000US5505300\tBeloit city\tWI\t25\tA\t44000000\t1000000\t42.5\t-89.0\n"
    )
    records = parse_gazetteer_capture(payload, geo_type="place", geography_vintage=2025)
    assert len(records) == 1
    assert records[0].geo_id == "state:55|place:05300"
    assert records[0].geoidfq == "1600000US5505300"
    assert records[0].land_area_m2 == 44_000_000
    assert len(records[0].attribute_checksum) == 64


@pytest.mark.parametrize(
    (
        "vintage",
        "line",
        "expected_geo_id",
        "expected_name",
        "expected_land",
        "expected_latitude",
    ),
    [
        (
            1990,
            "02   013 Aleutians East Borough                                             AK 000002464 000000693 0018090504 0020789982 +55229183 -161915191",
            "state:02|county:013",
            "Aleutians East Borough",
            18_090_504_000,
            55.229183,
        ),
        (
            2000,
            "AL01001Autauga County                                                      43671    17662    1543550050      21959029  595.968032    8.478429 32.523283 -86.577176",
            "state:01|county:001",
            "Autauga County",
            1_543_550_050,
            32.523283,
        ),
    ],
)
def test_legacy_county_gazetteers_preserve_canonical_attributes(
    vintage: int,
    line: str,
    expected_geo_id: str,
    expected_name: str,
    expected_land: int,
    expected_latitude: float,
) -> None:
    """Covers: ETL-024 — legacy captures preserve canonical geography attributes."""
    records = parse_legacy_county_gazetteer_capture(
        _gazetteer_zip(line + "\n"), geography_vintage=vintage
    )
    assert len(records) == 1
    assert records[0].geo_id == expected_geo_id
    assert records[0].name == expected_name
    assert records[0].land_area_m2 == expected_land
    assert records[0].latitude == pytest.approx(expected_latitude)


def test_2000_counties_include_obsolete_alaska_entities() -> None:
    """Covers: ETL-024 — historical geography retains obsolete county IDs."""

    def row(county: str, name: str) -> str:
        return (
            f"AK02{county}{name:<64}{0:>9}{0:>9}{0:>14}{0:>14}"
            f"{0.0:>12.6f}{0.0:>12.6f}{55.0:>10.6f}{-133.0:>11.6f}"
        )

    payload = _gazetteer_zip(
        "\n".join(
            [
                row("201", "Prince of Wales-Outer Ketchikan Census Area"),
                row("232", "Skagway-Hoonah-Angoon Census Area"),
                row("280", "Wrangell-Petersburg Census Area"),
            ]
        )
    )
    records = parse_legacy_county_gazetteer_capture(payload, geography_vintage=2000)
    assert {record.geo_id for record in records} == {
        "state:02|county:201",
        "state:02|county:232",
        "state:02|county:280",
    }


def test_malformed_gazetteer_is_rejected_after_capture_boundary() -> None:
    """Covers: ARC-002 — malformed source bytes cannot pass replay validation."""
    with pytest.raises(ValueError, match="not a ZIP"):
        parse_gazetteer_capture(
            b"not-source-data", geo_type="county", geography_vintage=2025
        )


def test_transient_download_retries_are_bounded_and_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-020, ETL-021 — transient retries are bounded and visible."""
    statuses = iter((503, 429, 200))
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(next(statuses), request=request, content=b"payload")

    class ControlStub:
        def __init__(self) -> None:
            self.retries: list[BaseException] = []

        def record_request_retry(self, request_id, *, error) -> None:
            self.retries.append(error)

    control = ControlStub()
    monkeypatch.setattr(geography_pipeline.time, "sleep", lambda _: None)
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        response = geography_pipeline._download_with_retry(
            client,
            "http://testserver/geography.zip",
            control=control,  # type: ignore[arg-type]
            request_id=uuid4(),
        )
    assert response.status_code == 200
    assert attempts == 3
    assert len(control.retries) == 2


def test_non_retryable_download_fails_immediately() -> None:
    """Covers: ETL-020, ETL-021 — permanent failures do not burn retry budget."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, request=request)

    class ControlStub:
        def record_request_retry(self, request_id, *, error) -> None:
            raise AssertionError("a permanent response must not be retried")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            geography_pipeline._download_with_retry(
                client,
                "http://testserver/missing.zip",
                control=ControlStub(),  # type: ignore[arg-type]
                request_id=uuid4(),
            )


def test_latest_complete_vintage_requires_every_asset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-024 — discovery selects a complete supported vintage."""

    class ClientStub:
        def __init__(self, **kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def head(self, url: str) -> SimpleNamespace:
            return SimpleNamespace(status_code=404 if "2098" in url else 405)

        def get(self, url: str, *, headers) -> SimpleNamespace:
            assert headers == {"Range": "bytes=0-3"}
            return SimpleNamespace(status_code=206)

    monkeypatch.setattr(geography_pipeline.httpx, "Client", ClientStub)
    assert geography_pipeline.resolve_latest_complete_year(2098, min_year=2097) == 2097
    assert geography_pipeline.resolve_historical_county_years(2097, min_year=2097) == [
        2097
    ]


def test_historical_county_discovery_includes_legacy_decennial_assets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-024 — history discovery includes legacy decennial assets."""
    requested: list[str] = []

    class ClientStub:
        def __init__(self, **kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def head(self, url: str) -> SimpleNamespace:
            requested.append(url)
            return SimpleNamespace(status_code=200)

    monkeypatch.setattr(geography_pipeline.httpx, "Client", ClientStub)
    assert geography_pipeline.resolve_historical_county_years(2000) == [1990, 2000]
    assert requested == [
        geography_pipeline.LEGACY_COUNTY_URLS[1990],
        geography_pipeline.LEGACY_COUNTY_URLS[2000],
    ]


def test_history_sync_backfills_missing_vintages_and_replays_latest_last(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-024 — history sync is cumulative, resumable, and ordered."""
    synced: list[tuple[int, dict[str, object]]] = []

    class HookStub:
        get_conn = object()

    monkeypatch.setattr(geography_pipeline, "_get_hook", lambda: HookStub())
    monkeypatch.setattr(
        geography_pipeline,
        "resolve_latest_complete_year",
        lambda **_kwargs: 2015,
    )
    monkeypatch.setattr(
        geography_pipeline,
        "resolve_historical_county_years",
        lambda *_args, **_kwargs: [1990, 2000, 2013, 2014, 2015],
    )
    monkeypatch.setattr(
        geography_pipeline,
        "successful_geography_vintages",
        lambda _factory, **_kwargs: {2014, 2015},
    )

    def sync(*, source_year: int, **kwargs) -> dict[str, int]:
        synced.append((source_year, kwargs))
        return {"attributes": 10, "geometries": 7, "retired": 2}

    monkeypatch.setattr(geography_pipeline, "sync_geography_reference", sync)

    assert geography_pipeline.sync_geography_history(source_year=2015) == {
        "vintages_discovered": 5,
        "vintages_synced": 4,
        "vintages_skipped": 1,
        "latest_vintage": 2015,
        "attributes": 40,
        "geometries": 28,
        "retired": 8,
    }
    assert [year for year, _ in synced] == [1990, 2000, 2013, 2015]
    assert synced[0][1]["assets"] == [geography_pipeline._historical_county_asset(1990)]
    assert synced[1][1]["assets"] == [geography_pipeline._historical_county_asset(2000)]
    assert synced[-1][1]["snapshot_scope"] == "full"


def test_sync_captures_every_asset_before_atomic_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-024 — a complete geography snapshot publishes atomically."""
    capture_payloads: dict[object, bytes] = {}
    events: list[tuple[str, object]] = []
    run_id = uuid4()

    class PublicationConnection:
        def commit(self) -> None:
            events.append(("commit", self))

        def rollback(self) -> None:
            events.append(("rollback", self))

        def close(self) -> None:
            events.append(("close", self))

    publication = PublicationConnection()

    class HookStub:
        def get_conn(self):
            return publication

    class ControlStub:
        def __init__(self, factory, *, source_code) -> None:
            assert source_code == geography_pipeline.SOURCE_CODE

        def start_run(self, *, watermark):
            events.append(("start_run", watermark))
            return run_id

        def start_request(self, **kwargs):
            assert kwargs["max_attempts"] == geography_pipeline.HTTP_MAX_ATTEMPTS
            return SimpleNamespace(request_id=uuid4())

        def finish_request(self, request_id, *, status, error=None) -> None:
            events.append(("request", status))

        def finish_run(self, completed_run_id, *, status, error=None) -> None:
            assert completed_run_id == run_id
            events.append(("run", status))

        def quarantine(self, **kwargs) -> None:
            raise AssertionError("valid fixture captures must not be quarantined")

    class RepositoryStub:
        def __init__(self, factory) -> None:
            pass

        def load_attributes(self, records, *, capture_id, connection=None) -> int:
            assert connection is publication
            events.append(("attributes", capture_id))
            return len(records)

        def load_geometries(self, records, *, capture_id, connection=None) -> int:
            assert connection is publication
            events.append(("geometries", capture_id))
            return len(records)

        def retire_missing(self, *, connection=None, **kwargs) -> int:
            assert connection is publication
            assert "state:78" in kwargs["active_geo_ids"]
            events.append(("retire", kwargs["vintage"]))
            return 0

        def reconcile_relationships(self, *, connection=None, **kwargs) -> None:
            assert connection is publication
            events.append(("relationships", kwargs["vintage"]))

    class ClientStub:
        def __init__(self, **kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

    def persist(factory, capture) -> None:
        capture_payloads[capture.capture_id] = capture.payload
        events.append(("capture", capture.capture_id))

    def download(client, url, **kwargs) -> httpx.Response:
        request = httpx.Request("GET", url)
        return httpx.Response(
            200,
            request=request,
            headers={"content-type": "application/zip"},
            content=url.encode(),
        )

    def parse_attributes(payload, *, geo_type, geography_vintage):
        assert len(capture_payloads) == 6
        width = {"state": 2, "county": 5, "place": 7}[geo_type]
        geoid = {"state": "98", "county": "98764", "place": "9854321"}[geo_type]
        return [
            geography_pipeline.GeographyRecord(
                geo_type,
                geography_pipeline.canonical_geo_id(
                    geo_type,
                    state_fips="98",
                    county_fips="764" if geo_type == "county" else None,
                    place_fips="54321" if geo_type == "place" else None,
                ),
                geoid.zfill(width),
                "98",
                "764" if geo_type == "county" else None,
                "54321" if geo_type == "place" else None,
                f"Fixture {geo_type}",
                geography_vintage,
            )
        ]

    def parse_geometry(payload, *, geo_type, boundary_vintage):
        boundary_geography = None
        if geo_type == "state":
            boundary_geography = geography_pipeline.GeographyRecord(
                "state",
                "state:78",
                "78",
                "78",
                None,
                None,
                "United States Virgin Islands",
                boundary_vintage,
                usps="VI",
            )
        return [
            geography_pipeline.GeometryRecord(
                geography_pipeline.canonical_geo_id(
                    geo_type,
                    state_fips="78" if geo_type == "state" else "98",
                    county_fips="764" if geo_type == "county" else None,
                    place_fips="54321" if geo_type == "place" else None,
                ),
                boundary_vintage,
                '{"type":"Polygon","coordinates":[]}',
                geography=boundary_geography,
            )
        ]

    monkeypatch.setattr(geography_pipeline, "_get_hook", HookStub)
    monkeypatch.setattr(geography_pipeline, "CaptureControl", ControlStub)
    monkeypatch.setattr(geography_pipeline, "GeographyRepository", RepositoryStub)
    monkeypatch.setattr(geography_pipeline.httpx, "Client", ClientStub)
    monkeypatch.setattr(geography_pipeline, "_download_with_retry", download)
    monkeypatch.setattr(geography_pipeline, "persist_response_capture", persist)
    monkeypatch.setattr(
        geography_pipeline,
        "load_captured_payload",
        lambda factory, capture_id: capture_payloads[capture_id],
    )
    monkeypatch.setattr(geography_pipeline, "parse_gazetteer_capture", parse_attributes)
    monkeypatch.setattr(geography_pipeline, "parse_boundary_capture", parse_geometry)

    assert geography_pipeline.sync_geography_reference(source_year=2098) == {
        "attributes": 5,
        "geometries": 3,
        "retired": 0,
    }
    assert [event[0] for event in events].count("capture") == 6
    assert events[-3:] == [
        ("commit", publication),
        ("close", publication),
        ("run", "success"),
    ]
    assert not any(event[0] == "rollback" for event in events)


@dataclass
class BatchCall:
    """One recorded ``execute_values`` invocation."""

    statement: str
    rows: list[Any]
    template: str | None
    page_size: int | None


class RecordingCursor:
    """Cursor double answering the loaders' lookups and recording statements."""

    def __init__(self, known_geo_ids: set[str] | None = None) -> None:
        self.statements: list[tuple[str, Any]] = []
        self.known_geo_ids = set() if known_geo_ids is None else set(known_geo_ids)
        self._result: list[tuple[Any, ...]] = []
        self._geo_sk: dict[str, int] = {}

    def __enter__(self) -> RecordingCursor:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, statement: str, parameters: Any = None) -> None:
        self.statements.append((statement, parameters))
        if "SELECT geo_id, geo_sk" in statement:
            self._result = [
                (geo_id, self._geo_sk_for(geo_id))
                for geo_id in parameters[0]
                if geo_id in self.known_geo_ids
            ]
        elif "SELECT geo_id FROM silver_ref.dim_geo_entity" in statement:
            self._result = [
                (geo_id,) for geo_id in parameters[0] if geo_id in self.known_geo_ids
            ]
        else:
            self._result = []

    def _geo_sk_for(self, geo_id: str) -> int:
        return self._geo_sk.setdefault(geo_id, 1000 + len(self._geo_sk))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._result


class RecordingConnection:
    """Connection double exposing only what the loaders use."""

    def __init__(self, cursor: RecordingCursor) -> None:
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> RecordingCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def recorded_batches(monkeypatch: pytest.MonkeyPatch) -> list[BatchCall]:
    """Record every multi-row statement the loaders submit.

    The batched loaders are a round-trip contract as much as a SQL one, so the
    tests assert on the statements themselves. ``execute_values`` itself is
    exercised for real against PostgreSQL by the database and DAG tiers.
    """
    calls: list[BatchCall] = []

    def recorder(
        _cursor: Any,
        statement: str,
        rows: Any,
        template: str | None = None,
        page_size: int | None = None,
        **_kwargs: Any,
    ) -> None:
        calls.append(BatchCall(statement, list(rows), template, page_size))

    monkeypatch.setattr(geography_pipeline, "execute_values", recorder)
    return calls


def _geography_record(
    geo_id: str, state_fips: str, vintage: int = 2023, name: str | None = None
):
    return geography_pipeline.GeographyRecord(
        geo_type="state",
        geo_id=geo_id,
        census_geoid=state_fips,
        state_fips=state_fips,
        county_fips=None,
        place_fips=None,
        name=name or f"State {state_fips}",
        geography_vintage=vintage,
    )


def _polygon_geojson() -> str:
    return (
        '{"type": "Polygon", "coordinates": '
        "[[[-77.1, 38.8], [-76.9, 38.8], [-76.9, 39.0], [-77.1, 38.8]]]}"
    )


def _repository(cursor: RecordingCursor):
    connection = RecordingConnection(cursor)
    return geography_pipeline.GeographyRepository(lambda: connection), connection


def _batch(calls: list[BatchCall], marker: str) -> BatchCall:
    matching = [call for call in calls if marker in call.statement]
    assert len(matching) == 1, f"expected exactly one {marker!r} batch, got {matching}"
    return matching[0]


def test_attribute_load_batches_instead_of_two_statements_per_record(
    recorded_batches: list[BatchCall],
) -> None:
    """Covers: ETL-024 — attribute loading stays bounded as the snapshot grows.

    The per-row loader issued two statements per record, so a production-scale
    snapshot cost tens of thousands of sequential round trips. Loading must now
    cost a fixed number of statements while still returning the row count
    callers depend on.
    """
    records = [
        _geography_record(f"state:{index:02d}", f"{index:02d}")
        for index in range(1, 61)
    ]
    cursor = RecordingCursor({record.geo_id for record in records})
    repository, connection = _repository(cursor)

    assert repository.load_attributes(records, capture_id=uuid4()) == 60

    assert len(recorded_batches) == 2
    # The only per-statement read left is the single surrogate-key lookup.
    assert len(cursor.statements) == 1
    assert "SELECT geo_id, geo_sk" in cursor.statements[0][0]
    assert connection.commits == 1
    assert connection.closed is True


def test_attribute_batch_preserves_upsert_and_version_semantics(
    recorded_batches: list[BatchCall],
) -> None:
    """Covers: ETL-024 — batching keeps the documented conflict behaviour.

    ``ON CONFLICT DO UPDATE`` on the entity with the ``LEAST``/``GREATEST``
    vintage merge and the ``updated_at`` touch, and ``ON CONFLICT DO NOTHING``
    on the version table so a replay stays idempotent.
    """
    cursor = RecordingCursor({"state:01"})
    repository, _ = _repository(cursor)
    repository.load_attributes(
        [_geography_record("state:01", "01")], capture_id=uuid4()
    )

    entity = _batch(recorded_batches, "INSERT INTO silver_ref.dim_geo_entity (")
    assert "ON CONFLICT (geo_id) DO UPDATE SET" in entity.statement
    assert "LEAST(" in entity.statement and "GREATEST(" in entity.statement
    assert "updated_at = NOW()" in entity.statement

    version = _batch(recorded_batches, "INSERT INTO silver_ref.dim_geo_entity_version")
    assert "ON CONFLICT DO NOTHING" in version.statement
    # Lineage travels with every version row.
    assert version.rows[0][2] is not None


def test_attribute_batch_folds_repeated_geo_ids_but_keeps_every_version(
    recorded_batches: list[BatchCall],
) -> None:
    """Covers: ETL-024 — a repeated identity cannot break the batched upsert.

    ``ON CONFLICT DO UPDATE`` refuses to touch a row twice in one command, so
    duplicates are folded for the entity while the vintage bounds collapse to
    the same ``LEAST``/``GREATEST`` result the per-row loop produced. Every
    supplied record still contributes its own attribute version.
    """
    records = [
        _geography_record("state:01", "01", vintage=2020, name="First"),
        _geography_record("state:01", "01", vintage=2024, name="Second"),
        _geography_record("state:02", "02", vintage=2023),
    ]
    cursor = RecordingCursor({"state:01", "state:02"})
    repository, _ = _repository(cursor)

    assert repository.load_attributes(records, capture_id=uuid4()) == 3

    entity = _batch(recorded_batches, "INSERT INTO silver_ref.dim_geo_entity (")
    assert [row[0] for row in entity.rows] == ["state:01", "state:02"]
    assert (entity.rows[0][6], entity.rows[0][7]) == (2020, 2024)

    version = _batch(recorded_batches, "INSERT INTO silver_ref.dim_geo_entity_version")
    assert [row[4] for row in version.rows] == ["First", "Second", "State 02"]


def test_geometry_load_batches_and_preserves_geometry_handling(
    recorded_batches: list[BatchCall],
) -> None:
    """Covers: ETL-024 — boundary loading stays bounded and repairs geometry.

    The per-row loader issued up to two statements per boundary. Batching must
    keep the same ``ST_MakeValid``/``ST_CollectionExtract`` repair, the same
    ``ST_IsValid`` record of the raw input, and ``ON CONFLICT DO NOTHING`` so a
    replay stays idempotent.
    """
    records = [
        geography_pipeline.GeometryRecord(
            f"state:{index:02d}", 2023, _polygon_geojson()
        )
        for index in range(1, 61)
    ]
    cursor = RecordingCursor({record.geo_id for record in records})
    repository, connection = _repository(cursor)

    assert repository.load_geometries(records, capture_id=uuid4()) == 60

    geometry = _batch(
        recorded_batches, "INSERT INTO silver_ref.dim_geo_geometry_version"
    )
    assert len(geometry.rows) == 60
    assert "ST_MakeValid" in geometry.statement
    assert "ST_CollectionExtract" in geometry.statement
    assert "ST_IsValid" in geometry.statement
    assert "ON CONFLICT DO NOTHING" in geometry.statement
    # Only the single set-based entity probe remains.
    assert len(cursor.statements) == 1
    assert connection.commits == 1


def test_batched_geometry_load_still_rejects_a_boundary_without_an_entity(
    recorded_batches: list[BatchCall],
) -> None:
    """Covers: ETL-024 — the missing-entity guard survives set-based loading.

    The guard is a real data-integrity check, not an artefact of the per-row
    loop. Moving the probe from per-row to set-based must keep it raising for
    the same inputs, naming the first offending identifier in input order, and
    must publish nothing.
    """
    records = [
        geography_pipeline.GeometryRecord("state:01", 2023, _polygon_geojson()),
        geography_pipeline.GeometryRecord("state:77", 2023, _polygon_geojson()),
        geography_pipeline.GeometryRecord("state:88", 2023, _polygon_geojson()),
    ]
    cursor = RecordingCursor({"state:01"})
    repository, connection = _repository(cursor)

    with pytest.raises(ValueError, match="boundary has no matching entity: state:77"):
        repository.load_geometries(records, capture_id=uuid4())

    assert recorded_batches == []
    assert connection.rollbacks == 1
    assert connection.commits == 0
    assert connection.closed is True


def test_empty_snapshots_issue_no_statement_and_return_zero(
    recorded_batches: list[BatchCall],
) -> None:
    """Covers: ETL-024 — an empty batch is a no-op, not an empty VALUES list."""
    cursor = RecordingCursor()
    repository, _ = _repository(cursor)
    assert repository.load_attributes([], capture_id=uuid4()) == 0
    assert repository.load_geometries([], capture_id=uuid4()) == 0
    assert recorded_batches == []
    assert cursor.statements == []


def test_relationship_reconciliation_restricts_pairs_before_intersecting() -> None:
    """Covers: ETL-024 — the spatial join keeps its candidate set bounded.

    Written as one flat join the planner drove the GiST index with every county
    boundary against every geometry row in the vintage and applied the
    same-state restriction only afterwards. The candidate set must be built from
    the cheap relational restriction first, and each overlap evaluated once.
    """
    cursor = RecordingCursor()
    repository, _ = _repository(cursor)
    repository.reconcile_relationships(
        vintage=2023, capture_id=uuid4(), active_geo_ids={"us:1"}
    )

    spatial = [
        statement for statement, _ in cursor.statements if "ST_Intersects" in statement
    ]
    assert len(spatial) == 1
    statement = spatial[0]
    assert "county_boundary AS MATERIALIZED" in statement
    assert "place_boundary AS MATERIALIZED" in statement
    assert "overlap AS MATERIALIZED" in statement
    # The overlap is computed once, not once per output column.
    assert statement.count("ST_Intersection(") == 1
    # Each place's own area is computed once per place, not once per pair.
    assert statement.count("ST_Area(boundary.geom::geography)") == 1
    assert "ON CONFLICT DO NOTHING" in statement
