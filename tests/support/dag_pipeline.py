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
import re
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
PROVIDER_POOLS: tuple[str, ...] = (
    "census_api",
    "bls_api",
    "fred_api",
    "cdc_api",
    "fbi_cde_api",
    "usda_nass_api",
)

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
    # Wisconsin anchors back the reviewed FBI agency sample: the county names
    # are the authoritative Census names its provider county labels must match
    # exactly, and the two places are the reviewed agency-to-place crosswalk.
    ("state", "55", None, None, "Wisconsin"),
    ("county", "55", "009", None, "Brown County"),
    ("county", "55", "025", None, "Dane County"),
    ("county", "55", "105", None, "Rock County"),
    ("place", "55", None, "22575", "Edgerton city"),
    ("place", "55", None, "25950", "Fitchburg city"),
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
    "FBI_CDE_API_KEY": "fixture-fbi-cde-key",
    "USDA_NASS_API_KEY": "FIXTURE-USDA-NASS-KEY-0000-1111-2222",
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


def stub_fbi_cde(monkeypatch: pytest.MonkeyPatch) -> None:
    """Serve the reviewed FBI CDE fixtures instead of the live provider.

    The FBI DAG captures an agency reference slice per state before it captures
    any agency observation, and replay refuses to publish an agency slice whose
    reference slice does not identify it. Both boundaries therefore need
    fixtures, and each answers the actual request rather than one flat payload.
    """
    from data_ingestion_toolbox.fbi_ucr import capture as fbi_capture
    from data_ingestion_toolbox.fbi_ucr.client import (
        CdeResponse,
        observation_parameters,
    )

    def response(endpoint: str, parameters: dict, name: str) -> CdeResponse:
        payload = json.dumps(load_fixture("fbi_ucr", f"{name}.json")).encode("utf-8")
        return CdeResponse(
            endpoint, parameters, payload, {"content-type": "application/json"}, 200
        )

    def observations(product: Any, subject: Any, **_kwargs: Any) -> CdeResponse:
        names = {
            "national": f"summarized_national_{product.offense_code}",
            "state": (
                f"summarized_state_{subject.subject_code}_{product.offense_code}"
            ),
            "agency": (
                f"summarized_agency_{subject.subject_code}_{product.offense_code}"
            ),
        }
        name = names.get(subject.subject_type)
        if name is None:
            raise AssertionError(
                f"No FBI fixture registered for subject {subject.slice_key}"
            )
        return response(
            product.observation_endpoint(subject),
            observation_parameters(product),
            name,
        )

    def directory(state_code: str, **_kwargs: Any) -> CdeResponse:
        return response(
            f"/agency/byStateAbbr/{state_code}", {}, f"agency_directory_{state_code}"
        )

    monkeypatch.setattr(fbi_capture, "fetch_summarized_observations", observations)
    monkeypatch.setattr(fbi_capture, "fetch_agency_directory", directory)


def stub_usda_nass_quick_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    """Serve the reviewed USDA NASS fixtures instead of the live Quick Stats API.

    The NASS pipeline preflights every registered slice through ``get_counts``
    and refuses to publish a release whose retrieval disagrees with its own
    preflight, so this stub answers the actual request: it resolves the product
    and aggregate level from the requested slice and reports a count that
    matches the rows it then serves. The reviewed sample covers one year per
    product, so the requested year is stamped onto the served rows rather than
    the sample being widened; every other field stays exactly as reviewed.
    """
    from data_ingestion_toolbox.usda_nass import capture as nass_capture
    from data_ingestion_toolbox.usda_nass.client import (
        NassCountResponse,
        NassDataResponse,
        count_parameters,
        data_parameters,
    )

    def rows_for(product: Any, item: Any) -> list[dict[str, Any]]:
        document = load_fixture("usda_nass", f"{product.product_id}.json")
        level = document["slices"].get(item.agg_level_desc)
        if level is None:
            raise AssertionError(
                f"No USDA NASS fixture registered for {product.product_id} "
                f"at {item.agg_level_desc}"
            )
        return [{**row, "year": str(item.year)} for row in level["data"]["data"]]

    def count(product: Any, item: Any, **_kwargs: Any) -> NassCountResponse:
        rows = rows_for(product, item)
        payload = json.dumps({"count": str(len(rows))}).encode("utf-8")
        return NassCountResponse(
            count_parameters(product, item),
            payload,
            {"content-type": "application/json"},
            200,
            len(rows),
        )

    def records(product: Any, item: Any, **_kwargs: Any) -> NassDataResponse:
        rows = rows_for(product, item)
        payload = json.dumps({"data": rows}).encode("utf-8")
        return NassDataResponse(
            data_parameters(product, item),
            payload,
            {"content-type": "application/json"},
            200,
            len(rows),
        )

    monkeypatch.setattr(nass_capture, "fetch_slice_count", count)
    monkeypatch.setattr(nass_capture, "fetch_slice_records", records)


def build_pep_release_csv(url: str) -> bytes:
    """Generate a production-shaped PEP bulk release for one registered URL.

    The reviewed fixtures are bounded samples, but the PEP replay applies its
    production completeness contract: NST needs 50 states at summary level 040,
    counties 3000 principal rows at 050, and subcounty 18000 at 162. Those
    guards are correct production behaviour, so the sample is generated at
    production shape from the same synthetic geography the shared dimension
    uses, rather than weakening the checks.
    """
    lowered = url.lower()
    # The vintage is the four digits in the release filename (nst-est2025,
    # co-est2025, sub-est2025), not the first digits anywhere in the URL.
    match = re.search(r"est(\d{4})", lowered.rsplit("/", 1)[-1])
    vintage = match.group(1) if match else "2025"
    metric = f"POPESTIMATE{vintage}"

    def state_rows() -> list[dict[str, str]]:
        return [
            entry
            for entry in build_geography_records("state")
            if entry["state_fips"] != "00"
        ]

    lines: list[str]
    if "nst-est" in lowered:
        lines = [f"SUMLEV,REGION,DIVISION,STATE,NAME,{metric}"]
        lines.append("010,0,0,00,United States,331000000")
        for entry in state_rows():
            lines.append(f"040,1,1,{entry['state_fips']},{entry['name']},1000000")
    elif "co-est" in lowered:
        lines = [f"SUMLEV,STATE,COUNTY,STNAME,CTYNAME,{metric}"]
        states = {entry["state_fips"]: entry["name"] for entry in state_rows()}
        for entry in states:
            lines.append(f"040,{entry},000,{states[entry]},{states[entry]},1000000")
        for entry in build_geography_records("county"):
            state_name = states.get(entry["state_fips"], "Unknown State")
            lines.append(
                f"050,{entry['state_fips']},{entry['county_fips']},"
                f"{state_name},{entry['name']},10000"
            )
    elif "sub-est" in lowered:
        lines = [
            f"SUMLEV,STATE,COUNTY,PLACE,COUSUB,CONCIT,FUNCSTAT,NAME,STNAME,{metric}"
        ]
        states = {entry["state_fips"]: entry["name"] for entry in state_rows()}
        for entry in states:
            lines.append(
                f"040,{entry},000,00000,00000,00000,A,{states[entry]},"
                f"{states[entry]},1000000"
            )
        for entry in build_geography_records("place"):
            state_name = states.get(entry["state_fips"], "Unknown State")
            lines.append(
                f"162,{entry['state_fips']},000,{entry['place_fips']},00000,00000,A,"
                f"{entry['name']},{state_name},5000"
            )
    else:
        raise AssertionError(f"No PEP release fixture registered for {url}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def stub_census_pep_downloads(monkeypatch: pytest.MonkeyPatch) -> None:
    """Serve generated production-shaped Census PEP bulk releases."""
    from data_ingestion_toolbox.census_pep import ingest as pep_ingest

    def fetch(url: str, **_kwargs: Any) -> Any:
        return pep_ingest.PEPHTTPResponse(
            payload=build_pep_release_csv(url),
            status_code=200,
            response_headers={"content-type": "text/csv"},
        )

    monkeypatch.setattr(pep_ingest, "_fetch_with_retry", fetch)


def stub_census_acs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Serve the reviewed Census ACS fixtures instead of the live API.

    The ACS DAG discovers its datasets from the provider catalogue and reads the
    variable dictionary before fetching observations, so both catalogue
    boundaries need fixtures as well as the observation boundary.
    """
    from data_ingestion_toolbox.census_acs import ingest as acs_ingest
    from data_ingestion_toolbox.census_acs import metadata as acs_metadata

    def observations(
        year: int,
        dataset: str,
        variables: list[str],
        geo_level: str,
        state_fips: str | None = None,
    ) -> list[list[str]]:
        # The provider returns list-of-lists whose geography columns depend on
        # the requested level, and silver replay rejects a payload missing its
        # level's columns, so the sample is generated per request rather than
        # served from one flat fixture.
        geographies = {
            "us": (["us"], [["1"]]),
            "state": (["state"], [["11"]]),
            "county": (["state", "county"], [["11", "001"]]),
            "place": (["state", "place"], [["11", "50000"]]),
        }
        if geo_level not in geographies:
            raise AssertionError(
                f"No ACS fixture registered for geography level {geo_level!r}"
            )
        geo_columns, geo_rows = geographies[geo_level]
        header = [*variables, *geo_columns]
        rows = [
            [str(100 + index) for index in range(len(variables))] + geo_row
            for geo_row in geo_rows
        ]
        return [header, *rows]

    monkeypatch.setattr(acs_ingest, "fetch_acs_api", observations)

    datasets = [
        {
            "title": f"American Community Survey {label} Estimates: Detailed Tables",
            "year": FIXTURE_GEOGRAPHY_VINTAGE,
            "identifier": (
                "https://api.census.gov/data/id/"
                f"ACSDT{code}Y{FIXTURE_GEOGRAPHY_VINTAGE}"
            ),
        }
        for label, code in (("5-Year", "5"), ("1-Year", "1"))
    ]
    monkeypatch.setattr(
        acs_metadata, "fetch_acs_datasets_from_data_json", lambda: datasets
    )

    def variables(year: int, dataset: str) -> dict[str, Any]:
        return {
            "variables": {
                "B01003_001E": {
                    "label": "Estimate!!Total",
                    "concept": "TOTAL POPULATION",
                    "group": "B01003",
                    "predicateType": "int",
                }
            }
        }

    monkeypatch.setattr(acs_metadata, "fetch_variables_json", variables)


#: The LAUS area the reviewed BLS observation fixture reports against.
BLS_FIXTURE_AREA = {"area_code": "ST1100000000000", "area_text": "District of Columbia"}


def stub_bls(monkeypatch: pytest.MonkeyPatch) -> None:
    """Serve the reviewed BLS fixtures instead of the live provider.

    The BLS DAG synchronises its series, area, and area-type catalogues from
    published TSV downloads before it fetches any observation, so the catalogue
    boundary needs fixtures as well as the observation boundary.
    """
    import polars as pl

    from data_ingestion_toolbox.bls import ingest as bls_ingest
    from data_ingestion_toolbox.bls import metadata as bls_metadata

    def observations(
        series_ids: list[str] | None = None,
        start_year: int = 2023,
        end_year: int = 2023,
        api_version: str = "v2",
        **kwargs: Any,
    ) -> dict[str, Any]:
        # The DAG requests its curated series per program; a flat fixture that
        # ignores the request would report observations for a series the
        # catalogue never registered.
        requested = series_ids or kwargs.get("seriesid") or []
        return {
            "status": "REQUEST_SUCCEEDED",
            "responseTime": 1,
            "message": [],
            "Results": {
                "series": [
                    {
                        "seriesID": series_id,
                        "data": [
                            {
                                "year": str(start_year),
                                "period": "M01",
                                "periodName": "January",
                                "value": "4.5",
                                "footnotes": [{}],
                                "latest": "true",
                            }
                        ],
                    }
                    for series_id in requested
                ]
            },
        }

    monkeypatch.setattr(bls_ingest, "fetch_bls_api", observations)

    series_id = f"LAU{BLS_FIXTURE_AREA['area_code']}03"
    catalogues = {
        ".series": pl.DataFrame(
            {
                "series_id": [series_id],
                "series_title": ["Unemployment Rate: District of Columbia"],
                "seasonal": ["U"],
                "measure_code": ["03"],
                "area_code": [BLS_FIXTURE_AREA["area_code"]],
                "area_text": [BLS_FIXTURE_AREA["area_text"]],
            }
        ),
        ".area_type": pl.DataFrame(
            {"area_type_code": ["A"], "areatype_text": ["Statewide"]}
        ),
        ".area": pl.DataFrame(
            {
                "area_type_code": ["A"],
                "area_code": [BLS_FIXTURE_AREA["area_code"]],
                "area_text": [BLS_FIXTURE_AREA["area_text"]],
            }
        ),
    }

    def read_tsv(url: str) -> Any:
        for suffix, frame in catalogues.items():
            if str(url).endswith(suffix):
                return frame
        raise AssertionError(f"No BLS catalogue fixture registered for {url}")

    monkeypatch.setattr(bls_metadata, "read_bls_tsv", read_tsv)


def stub_fred(monkeypatch: pytest.MonkeyPatch) -> None:
    """Serve the reviewed FRED fixtures instead of the live API.

    The FRED DAG synchronises series metadata from the ``/series`` endpoint for
    every configured series before ingesting observations, so that boundary
    needs a fixture too.
    """
    from data_ingestion_toolbox.fred import ingest as fred_ingest
    from data_ingestion_toolbox.fred import metadata as fred_metadata

    def observations(
        series_id: str,
        observation_start: str,
        observation_end: str,
        realtime_start: str | None = None,
        realtime_end: str | None = None,
    ) -> dict[str, Any]:
        # The reviewed fixture is dated far in the future so it cannot collide
        # with real data. The DAG requests a concrete window and discards
        # anything outside it, so the sample is emitted inside that window
        # instead, keeping every configured series covered.
        released = realtime_start or observation_end
        return {
            "observations": [
                {
                    "realtime_start": released,
                    "realtime_end": realtime_end or released,
                    "date": observation_start,
                    "value": "10",
                }
            ]
        }

    monkeypatch.setattr(fred_ingest, "fetch_fred_observations", observations)

    def series_metadata(series_id: str) -> dict[str, str]:
        return {
            "id": series_id,
            "title": f"Fixture series {series_id}",
            "units": "Percent",
            "frequency": "Monthly",
            "seasonal_adjustment": "Seasonally Adjusted",
            "notes": "Deterministic fixture metadata for orchestrated DAG execution.",
            "observation_start": "1970-01-01",
            "observation_end": "2026-01-01",
        }

    monkeypatch.setattr(fred_metadata, "fetch_fred_series_metadata", series_metadata)


def _target_database_modules() -> list[Any]:
    """Import every toolbox module pinning a production target database name.

    The list is discovered rather than hard-coded: metadata, geography, and
    ingest modules each carry their own ``_TARGET_DATABASE``, and a source added
    later would silently reconnect to the production database name if this had
    to be maintained by hand.
    """
    import importlib
    import pkgutil

    import data_ingestion_toolbox

    modules = []
    for info in pkgutil.walk_packages(
        data_ingestion_toolbox.__path__, f"{data_ingestion_toolbox.__name__}."
    ):
        try:
            module = importlib.import_module(info.name)
        except Exception:  # pragma: no cover - optional extras may be absent
            continue
        if hasattr(module, "_TARGET_DATABASE"):
            modules.append(module)
    return modules


def redirect_target_database(
    monkeypatch: pytest.MonkeyPatch, config: PostgresTestConfig
) -> None:
    """Point every pinned production database name at the disposable database.

    These sources take host, port, and credentials from the Airflow connection
    but override the database with a hard-coded production name. Only that name
    is redirected, so connection resolution, pooling, and hook construction stay
    real while the writes land in the disposable database.
    """
    redirected = 0
    for module in _target_database_modules():
        monkeypatch.setattr(module, "_TARGET_DATABASE", config.database)
        redirected += 1
        module_config = getattr(module, "CONFIG", None)
        if module_config is not None and hasattr(module_config, "target_database"):
            monkeypatch.setattr(module_config, "target_database", config.database)

    assert redirected, (
        "no module pinning _TARGET_DATABASE was found; the orchestrated run "
        "would write to the production database name"
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
        ("usda_nass", stub_usda_nass_quick_stats),
        ("census_pep", stub_census_pep_downloads),
        ("fbi_ucr", stub_fbi_cde),
    )
