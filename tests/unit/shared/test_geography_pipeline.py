"""Canonical identity and lossless offline geography replay contracts."""

from __future__ import annotations

import io
import zipfile
from types import SimpleNamespace
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
