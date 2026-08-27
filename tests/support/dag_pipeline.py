"""Runtime support for executing production DAGs against a disposable warehouse.

The DAG tier elsewhere proves graph shape, and the end-to-end tier proves the
pipeline functions. Neither proves the wiring between them: that executing the
real DAG actually invokes those functions with the right arguments, connection,
and ordering. That gap is what these helpers close.

Only the provider HTTP boundary is stubbed. Airflow, the operators, the
PostgresHook, the capture-control plane, and every warehouse write run for real
against the disposable PostGIS database, so a failure here is a genuine
orchestration defect rather than a mocking artifact.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import httpx
import pytest

from tests.support.postgres import PostgresTestConfig

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = REPOSITORY_ROOT / "tests/fixtures"

#: Airflow connection every production DAG resolves its warehouse through.
WAREHOUSE_CONN_ID = "public_data"

#: Pools the production DAGs throttle their provider calls with.
PROVIDER_POOLS: tuple[str, ...] = ("census_api", "bls_api", "fred_api", "cdc_api")

#: One bounded geography vintage is enough to exercise every dependent DAG.
FIXTURE_GEOGRAPHY_VINTAGE = 2023

#: Production-scale geography counts.
#
# The PEP DAG refuses to ingest against a toy dimension: it requires 50 states,
# 3000 counties, and 18000 places before it will touch observations. That guard
# is correct production behaviour, so the harness satisfies it honestly with a
# synthetic dimension of the right shape rather than weakening the check.
GEOGRAPHY_SCALE: dict[str, int] = {"state": 50, "county": 3143, "place": 19000}

#: Real anchor geographies the reviewed provider fixtures resolve against.
ANCHOR_GEOGRAPHIES: tuple[tuple[str, str, str | None, str | None, str], ...] = (
    ("state", "01", None, None, "Alabama"),
    ("state", "11", None, None, "District of Columbia"),
    ("state", "48", None, None, "Texas"),
    ("county", "01", "001", None, "Autauga County"),
    ("county", "11", "001", None, "District of Columbia"),
    ("county", "48", "301", None, "Loving County"),
    ("place", "11", None, "50000", "Washington city"),
)


def build_geography_records(geo_type: str) -> list[dict[str, Any]]:
    """Generate one production-scale, deterministic geography level.

    Identity is synthetic but structurally faithful: canonical FIPS widths,
    stable ordering, and the real anchor geographies the reviewed provider
    fixtures reference, so resolution exercises the same code paths. Codes are
    enumerated rather than derived by modulo, so every level reaches its target
    count instead of exhausting a smaller cyclic key space.
    """
    records = [
        {
            "geo_type": kind,
            "state_fips": state,
            "county_fips": county,
            "place_fips": place,
            "name": name,
        }
        for kind, state, county, place, name in ANCHOR_GEOGRAPHIES
        if kind == geo_type
    ]
    seen = {
        (item["state_fips"], item["county_fips"], item["place_fips"])
        for item in records
    }
    target = GEOGRAPHY_SCALE[geo_type]

    def add(state: str, county: str | None, place: str | None, name: str) -> bool:
        key = (state, county, place)
        if key not in seen:
            seen.add(key)
            records.append(
                {
                    "geo_type": geo_type,
                    "state_fips": state,
                    "county_fips": county,
                    "place_fips": place,
                    "name": name,
                }
            )
        return len(records) >= target

    if geo_type == "state":
        for index in range(1, 100):
            if add(f"{index:02d}", None, None, f"Synthetic State {index:02d}"):
                return records
        raise AssertionError(f"could not reach {target} state records")

    for state in [entry["state_fips"] for entry in build_geography_records("state")]:
        for ordinal in range(1, 1000):
            if geo_type == "county":
                filled = add(
                    state, f"{ordinal:03d}", None, f"County {state}{ordinal:03d}"
                )
            else:
                filled = add(
                    state, None, f"{ordinal:05d}", f"Place {state}{ordinal:05d}"
                )
            if filled:
                return records
    raise AssertionError(f"could not reach {target} {geo_type} records")


def _padded_geoid(entry: dict[str, Any]) -> str:
    """Return the canonical zero-padded GEOID for one geography record."""
    if entry["geo_type"] == "state":
        return entry["state_fips"]
    if entry["geo_type"] == "county":
        return f"{entry['state_fips']}{entry['county_fips']}"
    return f"{entry['state_fips']}{entry['place_fips']}"


def load_fixture(*parts: str) -> Any:
    """Load one reviewed provider fixture by path under ``tests/fixtures``."""
    path = FIXTURE_ROOT.joinpath(*parts)
    text = path.read_text(encoding="utf-8")
    return json.loads(text) if path.suffix == ".json" else text


def stub_response(payload: bytes, *, url: str = "https://fixture.invalid") -> Any:
    """Return a synthetic httpx response carrying fixture bytes."""
    return httpx.Response(
        200,
        request=httpx.Request("GET", url),
        headers={"content-type": "application/octet-stream"},
        content=payload,
    )


#: Credentials the source configurations require before they will build a
#: request. The HTTP boundary is stubbed, so these are fixture values: they
#: exercise the same configuration path without authenticating anything.
FIXTURE_CREDENTIALS: dict[str, str] = {
    "CENSUS_API_KEY": "fixture-census-key",
    "BLS_API_KEY": "fixture-bls-key",
    "FRED_API_KEY": "fixture-fred-key",
    "CDC_SOCRATA_APP_TOKEN": "fixture-cdc-token",
}


def apply_fixture_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide fixture provider credentials for the orchestrated run.

    A source DAG whose credential is absent fails at task runtime and then
    retries on its production backoff, which turns a missing fixture into a
    silent multi-minute hang rather than a visible failure.
    """
    for name, value in FIXTURE_CREDENTIALS.items():
        monkeypatch.setenv(name, value)


def disable_task_retries(monkeypatch: pytest.MonkeyPatch, dagbag: Any) -> None:
    """Run each task once so a real defect fails fast instead of backing off.

    Production retry behaviour is owned by the DAG structure tests. Here it only
    hides failures behind minutes of sleeping.
    """
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            monkeypatch.setattr(task, "retries", 0, raising=False)
            monkeypatch.setattr(
                task, "retry_delay", timedelta(seconds=0), raising=False
            )


def register_airflow_runtime(config: PostgresTestConfig) -> None:
    """Point Airflow's warehouse connection and provider pools at the test database.

    The DAGs resolve their database through a real ``PostgresHook``, so the
    connection is registered rather than stubbed. That keeps hook construction,
    connection resolution, and pool assignment inside the tested surface.
    """
    from airflow import settings
    from airflow.models import Connection, Pool
    from airflow.utils.db import initdb

    initdb()
    session = settings.Session()
    try:
        existing = (
            session.query(Connection)
            .filter(Connection.conn_id == WAREHOUSE_CONN_ID)
            .one_or_none()
        )
        if existing is not None:
            session.delete(existing)
            session.commit()
        session.add(
            Connection(
                conn_id=WAREHOUSE_CONN_ID,
                conn_type="postgres",
                host=config.host,
                port=config.port,
                login=config.user,
                password=config.password,
                schema=config.database,
            )
        )
        for pool in PROVIDER_POOLS:
            if session.query(Pool).filter(Pool.pool == pool).one_or_none() is None:
                session.add(
                    Pool(
                        pool=pool,
                        slots=4,
                        description="disposable DAG execution",
                        include_deferred=False,
                    )
                )
        session.commit()
    finally:
        session.close()


def run_dag(dagbag: Any, dag_id: str, *, logical_date: datetime | None = None) -> Any:
    """Execute one production DAG as a real DagRun and return it.

    ``DAG.test`` runs every task in-process without a scheduler, executor, or
    webserver, which is exactly the simulated Airflow environment this tier
    needs. It records failures on the task instances rather than raising, so
    callers must assert on the resulting states.
    """
    dag = dagbag.dags.get(dag_id)
    if dag is None:
        raise AssertionError(
            f"DAG '{dag_id}' is missing from the DagBag; "
            f"import errors: {dagbag.import_errors}"
        )
    return dag.test(
        execution_date=logical_date or datetime(2026, 1, 1, tzinfo=timezone.utc)
    )


def assert_dag_run_succeeded(dag_run: Any, dag_id: str) -> dict[str, str]:
    """Fail with every unsuccessful task named, not just the first one."""
    states = {
        instance.task_id: instance.state
        for instance in sorted(
            dag_run.get_task_instances(), key=lambda item: item.task_id
        )
    }
    unsuccessful = {
        task_id: state for task_id, state in states.items() if state != "success"
    }
    assert not unsuccessful, (
        f"DAG '{dag_id}' finished {dag_run.state} with unsuccessful task(s): "
        + ", ".join(f"{task}={state}" for task, state in sorted(unsuccessful.items()))
    )
    assert states, f"DAG '{dag_id}' produced no task instances."
    return states


def stub_geography_downloads(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the TIGER/Gazetteer downloads with one bounded fixture vintage.

    Capture control, the geography repository, and every warehouse write stay
    real; only the provider bytes and the vintage discovery probes are replaced,
    so the DAG still exercises capture lineage and dimension loading.
    """
    from data_ingestion_toolbox.silver_ref import geography_pipeline

    monkeypatch.setattr(
        geography_pipeline,
        "resolve_latest_complete_year",
        lambda **_kwargs: FIXTURE_GEOGRAPHY_VINTAGE,
    )
    monkeypatch.setattr(
        geography_pipeline,
        "resolve_complete_years",
        lambda **_kwargs: [FIXTURE_GEOGRAPHY_VINTAGE],
    )
    monkeypatch.setattr(
        geography_pipeline,
        "resolve_historical_county_years",
        lambda *_args, **_kwargs: [FIXTURE_GEOGRAPHY_VINTAGE],
    )
    monkeypatch.setattr(
        geography_pipeline,
        "_download_with_retry",
        lambda client, url, **_kwargs: stub_response(url.encode(), url=url),
    )

    def parse_attributes(
        _payload: bytes, *, geo_type: str, geography_vintage: int
    ) -> list[Any]:
        return [
            geography_pipeline.GeographyRecord(
                geo_type,
                geography_pipeline.canonical_geo_id(
                    geo_type,
                    state_fips=entry["state_fips"],
                    county_fips=entry["county_fips"],
                    place_fips=entry["place_fips"],
                ),
                _padded_geoid(entry),
                entry["state_fips"],
                entry["county_fips"],
                entry["place_fips"],
                entry["name"],
                geography_vintage,
            )
            for entry in build_geography_records(geo_type)
        ]

    def parse_geometry(
        _payload: bytes, *, geo_type: str, boundary_vintage: int
    ) -> list[Any]:
        polygon = json.dumps(
            {
                "type": "Polygon",
                "coordinates": [
                    [[-77.1, 38.8], [-76.9, 38.8], [-76.9, 39.0], [-77.1, 38.8]]
                ],
            }
        )
        return [
            geography_pipeline.GeometryRecord(
                geography_pipeline.canonical_geo_id(
                    geo_type,
                    state_fips=entry["state_fips"],
                    county_fips=entry["county_fips"],
                    place_fips=entry["place_fips"],
                ),
                boundary_vintage,
                polygon,
            )
            for entry in build_geography_records(geo_type)
        ]

    monkeypatch.setattr(geography_pipeline, "parse_gazetteer_capture", parse_attributes)
    monkeypatch.setattr(
        geography_pipeline, "parse_legacy_county_gazetteer_capture", parse_attributes
    )
    monkeypatch.setattr(geography_pipeline, "parse_boundary_capture", parse_geometry)


def stub_cdc_socrata(monkeypatch: pytest.MonkeyPatch) -> None:
    """Serve the reviewed CDC Socrata fixtures instead of the live provider."""
    from data_ingestion_toolbox.cdc import capture as cdc_capture
    from data_ingestion_toolbox.cdc.client import (
        SocrataMetadataResponse,
        SocrataPage,
        page_parameters,
    )

    payloads = {
        "cdi": (
            load_fixture("cdc", "cdi_metadata.json"),
            load_fixture("cdc", "cdi_observations.json"),
        ),
        "places_county": (
            load_fixture("cdc", "places_county_metadata.json"),
            load_fixture("cdc", "places_county_observations.json"),
        ),
    }

    def resolve(asset: Any) -> tuple[Any, Any]:
        key = "cdi" if "cdi" in str(asset.asset_id).lower() else asset.asset_id
        for candidate, value in payloads.items():
            if candidate in str(getattr(asset, "product_code", "")).lower():
                return value
        return payloads.get(key, payloads["cdi"])

    def metadata(asset: Any, **_kwargs: Any) -> SocrataMetadataResponse:
        document, _ = resolve(asset)
        return SocrataMetadataResponse(
            json.dumps(document).encode("utf-8"),
            {"content-type": "application/json"},
            200,
        )

    def page(
        asset: Any, *, offset: int = 0, page_size: int | None = None, **kwargs: Any
    ):
        _, rows = resolve(asset)
        window = rows[offset : offset + (page_size or len(rows))]
        return SocrataPage(
            page_parameters(asset, page_size=page_size or len(rows), offset=offset),
            json.dumps(window).encode("utf-8"),
            {"content-type": "application/json"},
            200,
            len(window),
        )

    monkeypatch.setattr(cdc_capture, "fetch_socrata_metadata", metadata)
    monkeypatch.setattr(cdc_capture, "fetch_socrata_page", page)


def stub_census_pep_downloads(monkeypatch: pytest.MonkeyPatch) -> None:
    """Serve the reviewed Census PEP bulk-release fixtures."""
    from data_ingestion_toolbox.census_pep import ingest as pep_ingest

    releases = {
        "nst": (FIXTURE_ROOT / "census_pep/nst_2025.csv").read_bytes(),
        "sub-est": (FIXTURE_ROOT / "census_pep/subcounty_2025.csv").read_bytes(),
    }

    def fetch(url: str, **_kwargs: Any) -> Any:
        lowered = url.lower()
        payload = next(
            (value for key, value in releases.items() if key in lowered),
            releases["nst"],
        )
        return pep_ingest.PEPHTTPResponse(
            payload=payload,
            status_code=200,
            response_headers={"content-type": "text/csv"},
        )

    monkeypatch.setattr(pep_ingest, "_fetch_with_retry", fetch)


def stub_census_acs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Serve the reviewed Census ACS fixtures instead of the live API.

    The ACS DAG discovers datasets from the provider catalogue before it fetches
    any observation, so the discovery boundary needs a fixture too.
    """
    from data_ingestion_toolbox.census_acs import ingest as acs_ingest

    payload = load_fixture("census", "e2e_pipeline.json")
    monkeypatch.setattr(acs_ingest, "fetch_acs_api", lambda **_kwargs: payload)

    catalogue = {
        "dataset": [
            {
                "c_vintage": 2023,
                "c_dataset": ["acs", "acs5"],
                "title": "American Community Survey 5-Year",
                "description": "Fixture ACS 5-year dataset",
                "distribution": [
                    {"accessURL": "https://api.census.gov/data/2023/acs/acs5"}
                ],
            }
        ]
    }
    for attribute in ("fetch_dataset_catalog", "fetch_acs_datasets", "fetch_datasets"):
        if hasattr(acs_ingest, attribute):
            monkeypatch.setattr(
                acs_ingest, attribute, lambda *_a, **_k: catalogue, raising=False
            )


def stub_bls(monkeypatch: pytest.MonkeyPatch) -> None:
    """Serve the reviewed BLS fixture instead of the live API."""
    from data_ingestion_toolbox.bls import ingest as bls_ingest

    payload = load_fixture("bls", "e2e_pipeline.json")
    monkeypatch.setattr(bls_ingest, "fetch_bls_api", lambda **_kwargs: payload)


def stub_fred(monkeypatch: pytest.MonkeyPatch) -> None:
    """Serve the reviewed FRED fixture instead of the live API."""
    from data_ingestion_toolbox.fred import ingest as fred_ingest

    payload = load_fixture("fred", "e2e_pipeline.json")
    monkeypatch.setattr(
        fred_ingest, "fetch_fred_observations", lambda *_args, **_kwargs: payload
    )


#: Modules pinning the production target database name.
TARGET_DATABASE_MODULES: tuple[str, ...] = (
    "data_ingestion_toolbox.bls.ingest",
    "data_ingestion_toolbox.census_acs.ingest",
    "data_ingestion_toolbox.fred.ingest",
    "data_ingestion_toolbox.cdc.config",
)


def redirect_target_database(
    monkeypatch: pytest.MonkeyPatch, config: PostgresTestConfig
) -> None:
    """Point the pinned production database name at the disposable database.

    These sources take host, port, and credentials from the Airflow connection
    but override the database with a hard-coded production name. Only that name
    is redirected, so connection resolution, pooling, and hook construction stay
    real while the writes land in the disposable database.
    """
    import importlib

    for name in TARGET_DATABASE_MODULES:
        module = importlib.import_module(name)
        monkeypatch.setattr(module, "_TARGET_DATABASE", config.database, raising=False)
        module_config = getattr(module, "CONFIG", None)
        if module_config is not None and hasattr(module_config, "target_database"):
            monkeypatch.setattr(
                module_config, "target_database", config.database, raising=False
            )


def block_live_providers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail fast on any provider call a stub does not cover.

    Without this, an unstubbed boundary reaches the real internet and the suite
    hangs on connect timeouts and retry backoff instead of failing. Loopback is
    left open because the warehouse and the Airflow metadata database are real.
    """

    def guard(_client: Any, request: Any, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError(
            "Orchestrated DAG execution attempted a live provider call to "
            f"{request.url}. Add a fixture stub for this boundary; a DAG test "
            "must never depend on the network."
        )

    monkeypatch.setattr(httpx.Client, "send", guard)


def stub_all_providers(
    monkeypatch: pytest.MonkeyPatch, config: PostgresTestConfig
) -> None:
    """Apply every provider stub, so no DAG reaches a live provider."""
    apply_fixture_credentials(monkeypatch)
    redirect_target_database(monkeypatch, config)
    for _name, apply in iter_provider_stubs():
        apply(monkeypatch)
    block_live_providers(monkeypatch)


def iter_provider_stubs() -> Iterable[tuple[str, Callable[[pytest.MonkeyPatch], None]]]:
    """Return every named provider stub, so no pipeline is silently uncovered."""
    return (
        ("geography", stub_geography_downloads),
        ("census_acs", stub_census_acs),
        ("bls", stub_bls),
        ("fred", stub_fred),
        ("cdc", stub_cdc_socrata),
        ("census_pep", stub_census_pep_downloads),
    )
