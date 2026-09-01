"""FastAPI routes exercised against the actual gold warehouse schema."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from tests.support.postgres import PostgresTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]


@pytest.fixture
def real_api_fixture(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[tuple[TestClient, str, str]]:
    token = uuid4().hex[:12].upper()
    metric_a = f"FRED:TEST_API_{token}_A"
    metric_b = f"FRED:TEST_API_{token}_B"
    series_a = f"TEST_API_{token}_A"
    series_b = f"TEST_API_{token}_B"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO gold_glossary.dim_metric_catalog (
                    metric_code, metric_display_name, source_code,
                    source_object_type, source_object_key,
                    valid_geo_grains, valid_time_grains
                ) VALUES
                    (%s, 'API fixture A', 'FRED', 'FRED_SERIES', %s,
                     ARRAY['NATIONAL'], ARRAY['MONTHLY']),
                    (%s, 'API fixture B', 'FRED', 'FRED_SERIES', %s,
                     ARRAY['NATIONAL'], ARRAY['MONTHLY'])
                """,
                (metric_a, series_a, metric_b, series_b),
            )
            cursor.execute(
                """
                INSERT INTO gold_fred.rpt_fred_observations (
                    source_code, observation_date, duration_start, duration_end,
                    time_sk, as_of_date, updated_at, geo_id, geo_level,
                    series_id, series_title, value, units, frequency,
                    metric_code, metric_display_name
                ) VALUES
                    ('FRED', '2097-01-01', '2097-01-01', '2097-01-31', 20970101,
                     '2097-02-01', NOW(), 'us:1', 'NATIONAL', %s, 'API fixture A',
                     10, 'Index', 'Monthly', %s, 'API fixture A'),
                    ('FRED', '2097-02-01', '2097-02-01', '2097-02-28', 20970201,
                     '2097-03-01', NOW(), 'us:1', 'NATIONAL', %s, 'API fixture A',
                     20, 'Index', 'Monthly', %s, 'API fixture A'),
                    ('FRED', '2097-02-01', '2097-02-01', '2097-02-28', 20970201,
                     '2097-03-01', NOW(), 'us:1', 'NATIONAL', %s, 'API fixture B',
                     5, 'Index', 'Monthly', %s, 'API fixture B')
                """,
                (series_a, metric_a, series_a, metric_a, series_b, metric_b),
            )
            cursor.execute(
                """
                INSERT INTO gold_fred.mv_fred_latest
                SELECT * FROM gold_fred.rpt_fred_observations
                WHERE series_id IN (%s, %s) AND observation_date = '2097-02-01'
                """,
                (series_a, series_b),
            )
        writer.commit()
    finally:
        writer.close()

    settings = PostgresTestConfig.from_environment()
    assert settings is not None
    engine = create_engine(
        "postgresql+psycopg2://",
        connect_args={
            "host": settings.host,
            "port": settings.port,
            "user": settings.user,
            "password": settings.password,
            "dbname": settings.database,
        },
        pool_pre_ping=True,
    )

    def override_db() -> Iterator[Session]:
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_db_session_dep] = override_db
    try:
        yield TestClient(app), metric_a, metric_b
    finally:
        app.dependency_overrides.clear()
        engine.dispose()
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM gold_fred.mv_fred_latest WHERE series_id IN (%s, %s)",
                    (series_a, series_b),
                )
                cursor.execute(
                    "DELETE FROM gold_fred.rpt_fred_observations WHERE series_id IN (%s, %s)",
                    (series_a, series_b),
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog WHERE metric_code IN (%s, %s)",
                    (metric_a, metric_b),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_real_catalog_latest_timeseries_distribution_and_comparison(
    real_api_fixture: tuple[TestClient, str, str],
) -> None:
    """Covers: API-024 — real-schema API calls return exact seeded results."""
    client, metric_a, metric_b = real_api_fixture

    catalog = client.get("/api/v1/catalog/metrics", params={"q": metric_a, "limit": 10})
    assert catalog.status_code == 200
    assert [item["metric_code"] for item in catalog.json()["items"]] == [metric_a]

    latest = client.get("/api/v1/observations/latest", params={"metric_code": metric_a})
    assert latest.status_code == 200
    assert latest.json()["total"] == 1
    assert latest.json()["items"][0]["value"] == "20"

    timeseries = client.get(
        "/api/v1/observations/timeseries",
        params={"metric_code": metric_a, "geo_id": "us:1"},
    )
    assert timeseries.status_code == 200
    assert [item["value"] for item in timeseries.json()["items"]] == ["10", "20"]

    distribution = client.get(
        "/api/v1/distribution/bins", params={"metric_code": metric_a, "bin_count": 5}
    )
    assert distribution.status_code == 200
    assert distribution.json()["total"] == 1
    assert distribution.json()["items"][0]["count"] == 1

    comparison = client.get(
        "/api/v1/comparison",
        params={"metric_code_a": metric_a, "metric_code_b": metric_b},
    )
    assert comparison.status_code == 200
    assert comparison.json()["items"] == [
        {
            "geo_id": "us:1",
            "geo_level": "NATIONAL",
            "state_fips": None,
            "county_fips": None,
            "state_name": None,
            "county_name": None,
            "metric_code_a": metric_a,
            "metric_code_b": metric_b,
            "period_a": "2097-02-01",
            "period_b": "2097-02-01",
            "value_a": 20.0,
            "value_b": 5.0,
            "difference": 15.0,
            "ratio": 4.0,
        }
    ]


def test_real_preflight_and_policy_guarded_analysis(
    real_api_fixture: tuple[TestClient, str, str],
) -> None:
    """Covers: API-053 — the declared analysis policy against the real glossary.

    The seeded FRED pair shares its published grains and publishes no units,
    so the preflight verdict is comparable-with-caveat; the comparison serves
    exactly that decision, and the dispatched distribution labels its derived
    bins with the owning source.
    """
    client, metric_a, metric_b = real_api_fixture

    preflight = client.get(
        "/api/v1/comparison/preflight",
        params={"metric_code_a": metric_a, "metric_code_b": metric_b},
    )
    assert preflight.status_code == 200
    verdict = preflight.json()
    assert verdict["comparable"] is True
    assert verdict["derivations"] == ["difference", "ratio"]
    assert verdict["source_code_a"] == verdict["source_code_b"] == "FRED"
    statuses = {rule["rule"]: rule["status"] for rule in verdict["rules"]}
    assert statuses["time_grains"] == "pass"
    assert statuses["units"] == "unknown"
    assert any("units" in caveat for caveat in verdict["caveats"])

    unknown = client.get(
        "/api/v1/comparison/preflight",
        params={"metric_code_a": metric_a, "metric_code_b": "NO:SUCH"},
    )
    assert unknown.status_code == 404
    assert unknown.json() == {"detail": "metric_code_b not found"}

    comparison = client.get(
        "/api/v1/comparison",
        params={"metric_code_a": metric_a, "metric_code_b": metric_b},
    )
    assert comparison.status_code == 200
    payload = comparison.json()
    assert payload["derivations"] == ["difference", "ratio"]
    assert payload["caveats"] == verdict["caveats"]
    assert payload["total"] == 1

    distribution = client.get(
        "/api/v1/distribution/bins",
        params={"metric_code": metric_a, "bin_count": 3},
    )
    assert distribution.status_code == 200
    bins = distribution.json()
    assert bins["source_code"] == "FRED"
    assert bins["derived"] is True
    assert bins["total"] == 1


def test_real_discovery_detail_freshness_and_capabilities(
    real_api_fixture: tuple[TestClient, str, str],
) -> None:
    """Covers: API-037, API-038, API-039, API-040 — discovery answers from the
    real glossary contracts: metric capability detail with a stable 404,
    per-source freshness rollup, and the capability listing, with no legacy
    relation probing left to fall back on."""
    client, metric_a, _ = real_api_fixture

    detail = client.get(f"/api/v1/catalog/metrics/{metric_a}")
    assert detail.status_code == 200
    payload = detail.json()
    assert payload["metric_code"] == metric_a
    assert payload["source_code"] == "FRED"
    assert payload["valid_geo_grains"] == ["NATIONAL"]
    assert payload["freshness_state"] == "current"
    assert payload["served_by_neutral_routes"] is True
    served_paths = {route["path"] for route in payload["observation_routes"]}
    assert "/api/v1/observations/latest" in served_paths
    assert "/api/v1/fred/observations/latest" in served_paths

    missing = client.get(f"/api/v1/catalog/metrics/NO:SUCH:{metric_a}")
    assert missing.status_code == 404
    assert missing.json() == {"detail": "metric_code not found"}

    freshness = client.get("/api/v1/catalog/freshness")
    assert freshness.status_code == 200
    rollup = freshness.json()
    by_source = {row["source_code"]: row for row in rollup["items"]}
    assert [row["source_code"] for row in rollup["items"]] == sorted(by_source)
    assert by_source["FRED"]["metric_count"] >= 2
    assert by_source["FRED"]["current_count"] >= 2
    assert by_source["FRED"]["latest_harvested_at"] is not None

    capabilities = client.get("/api/v1/catalog/capabilities")
    assert capabilities.status_code == 200
    items = capabilities.json()["items"]
    codes = [item["source_code"] for item in items]
    assert codes == sorted(codes)
    assert "FBI_UCR" in codes


@pytest.fixture
def census_bls_api_fixture(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[tuple[TestClient, str, str]]:
    """Seed matching Census and BLS source rows in the actual serving relations."""
    token = uuid4().hex[:10].upper()
    census_metric = f"ACS:acs5:TEST_{token}"
    bls_metric = f"BLS:TEST_{token}"
    census_variable = f"TEST_{token}E"
    bls_series = f"LAUTEST{token}"
    geo_id = f"state:55|county:{token[:3]}"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO gold_glossary.dim_metric_catalog (
                    metric_code, metric_display_name, source_code,
                    source_object_type, source_object_key,
                    valid_geo_grains, valid_time_grains, physical_lineage
                ) VALUES
                    (%s, 'Census API fixture', 'CENSUS_ACS', 'ACS_VARIABLE', %s,
                     ARRAY['COUNTY'], ARRAY['ANNUAL'], %s::jsonb),
                    (%s, 'BLS API fixture', 'BLS', 'BLS_SERIES', %s,
                     ARRAY['COUNTY'], ARRAY['MONTHLY'], '{}'::jsonb)
                """,
                (
                    census_metric,
                    census_variable,
                    json.dumps(
                        {
                            "schema": "gold_census",
                            "relation": "fact_acs_observation",
                            # The serving relations spell this identity with
                            # the ACS: prefix; the published key bridges it,
                            # exactly as the production publisher does.
                            "key": census_metric.removeprefix("ACS:"),
                        }
                    ),
                    bls_metric,
                    bls_series,
                ),
            )
            cursor.execute(
                """
                INSERT INTO gold_census.rpt_acs_observations (
                    observation_date, duration_start, duration_end, time_sk,
                    as_of_date, updated_at, geo_id, geo_level, state_fips,
                    county_fips, state_name, county_name, value, dataset_code,
                    vintage_year, table_id, variable_code, estimate_value,
                    units, metric_code, metric_display_name
                ) VALUES
                    ('2096-01-01', '2092-01-01', '2096-12-31', 20960101,
                     '2097-01-01', NOW(), %s, 'COUNTY', '55', %s,
                     'Wisconsin', 'API County', 100, 'acs5', 2096, 'TEST', %s,
                     100, 'people', %s, 'Census API fixture'),
                    ('2097-01-01', '2093-01-01', '2097-12-31', 20970101,
                     '2098-01-01', NOW(), %s, 'COUNTY', '55', %s,
                     'Wisconsin', 'API County', 110, 'acs5', 2097, 'TEST', %s,
                     110, 'people', %s, 'Census API fixture')
                """,
                (
                    geo_id,
                    token[:3],
                    census_variable,
                    census_metric,
                    geo_id,
                    token[:3],
                    census_variable,
                    census_metric,
                ),
            )
            cursor.execute(
                """
                INSERT INTO gold_census.mv_acs_latest
                SELECT * FROM gold_census.rpt_acs_observations
                WHERE metric_code = %s AND observation_date = '2097-01-01'
                """,
                (census_metric,),
            )
            cursor.execute(
                """
                INSERT INTO gold_bls.rpt_bls_observations (
                    observation_date, duration_start, duration_end, time_sk,
                    as_of_date, updated_at, geo_id, geo_level, state_fips,
                    county_fips, state_name, county_name, series_id, program_code,
                    value, value_type, units, metric_code, metric_display_name
                ) VALUES
                    ('2097-01-01', '2097-01-01', '2097-01-31', 20970101,
                     '2097-02-01', NOW(), %s, 'COUNTY', '55', %s,
                     'Wisconsin', 'API County', %s, 'LA', 4, 'RATE', 'percent',
                     %s, 'BLS API fixture'),
                    ('2097-02-01', '2097-02-01', '2097-02-28', 20970201,
                     '2097-03-01', NOW(), %s, 'COUNTY', '55', %s,
                     'Wisconsin', 'API County', %s, 'LA', 5, 'RATE', 'percent',
                     %s, 'BLS API fixture')
                """,
                (
                    geo_id,
                    token[:3],
                    bls_series,
                    bls_metric,
                    geo_id,
                    token[:3],
                    bls_series,
                    bls_metric,
                ),
            )
            cursor.execute(
                """
                INSERT INTO gold_bls.mv_bls_latest
                SELECT * FROM gold_bls.rpt_bls_observations
                WHERE metric_code = %s AND observation_date = '2097-02-01'
                """,
                (bls_metric,),
            )
        writer.commit()
    finally:
        writer.close()

    settings = PostgresTestConfig.from_environment()
    assert settings is not None
    engine = create_engine(
        "postgresql+psycopg2://",
        connect_args={
            "host": settings.host,
            "port": settings.port,
            "user": settings.user,
            "password": settings.password,
            "dbname": settings.database,
        },
        pool_pre_ping=True,
    )

    def override_db() -> Iterator[Session]:
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_db_session_dep] = override_db
    try:
        yield TestClient(app), census_metric, bls_metric
    finally:
        app.dependency_overrides.clear()
        engine.dispose()
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                for relation in (
                    "gold_census.mv_acs_latest",
                    "gold_census.rpt_acs_observations",
                    "gold_bls.mv_bls_latest",
                    "gold_bls.rpt_bls_observations",
                ):
                    cursor.execute(
                        f"DELETE FROM {relation} WHERE metric_code IN (%s, %s)",
                        (census_metric, bls_metric),
                    )
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog WHERE metric_code IN (%s, %s)",
                    (census_metric, bls_metric),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_real_database_contract_spans_census_bls_and_cross_source_views(
    census_bls_api_fixture: tuple[TestClient, str, str],
) -> None:
    """Covers: API-024 — all source routers and shared views use the real schema.

    Covers: API-053 — the declared compatibility policy rejects the
        annual-versus-monthly pair against the real glossary, with the
        preflight explaining exactly which rule failed.
    """
    client, census_metric, bls_metric = census_bls_api_fixture

    for source, metric, expected in (
        ("census", census_metric, ["100", "110"]),
        ("bls", bls_metric, ["4", "5"]),
    ):
        catalog = client.get(
            "/api/v1/catalog/metrics", params={"q": metric, "limit": 10}
        )
        assert catalog.status_code == 200
        assert [row["metric_code"] for row in catalog.json()["items"]] == [metric]

        latest = client.get(
            f"/api/v1/{source}/observations/latest", params={"metric_code": metric}
        )
        assert latest.status_code == 200
        assert latest.json()["total"] == 1
        assert latest.json()["items"][0]["source"] in {"CENSUS_ACS", "BLS"}

        history = client.get(
            f"/api/v1/{source}/observations/timeseries",
            params={
                "metric_code": metric,
                "geo_id": latest.json()["items"][0]["geo_id"],
            },
        )
        assert history.status_code == 200
        assert [row["value"] for row in history.json()["items"]] == expected

        common = client.get(
            "/api/v1/observations/latest", params={"metric_code": metric}
        )
        assert common.status_code == 200
        assert common.json()["total"] == 1

        distribution = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": metric, "bin_count": 1},
        )
        assert distribution.status_code == 200
        assert distribution.json()["total"] == 1

    # Covers: API-053 — an annual survey estimate and a monthly rate are not
    # comparable, and the declared policy says so instead of serving a join.
    comparison = client.get(
        "/api/v1/comparison",
        params={"metric_code_a": census_metric, "metric_code_b": bls_metric},
    )
    assert comparison.status_code == 422
    assert "time grains" in comparison.json()["detail"]

    preflight = client.get(
        "/api/v1/comparison/preflight",
        params={"metric_code_a": census_metric, "metric_code_b": bls_metric},
    )
    assert preflight.status_code == 200
    verdict = preflight.json()
    assert verdict["comparable"] is False
    assert verdict["derivations"] == []
    failed = {rule["rule"] for rule in verdict["rules"] if rule["status"] == "fail"}
    assert failed == {"time_grains"}


def test_real_neutral_observation_dispatch_and_releases(
    real_api_fixture: tuple[TestClient, str, str],
) -> None:
    """Covers: API-048 — registry dispatch against the real serving contracts.

    The neutral resource resolves the seeded FRED metric through the real
    glossary contract, answers latest from ``gold_fred.mv_fred_latest`` and
    as-released history from ``gold_fred.rpt_fred_observations``, lists the
    release identities newest-first, and serializes deterministically:
    repeating an identical request returns byte-identical JSON.
    """
    client, metric_a, _ = real_api_fixture

    latest = client.get("/api/v1/observations", params={"metric_code": metric_a})
    assert latest.status_code == 200
    payload = latest.json()
    assert payload["source_code"] == "FRED"
    assert payload["scope"] == "latest"
    assert payload["total"] == 1
    (row,) = payload["items"]
    assert row["metric_code"] == metric_a
    assert row["geo_id"] == "us:1"
    assert row["geo_level"] == "NATIONAL"
    assert row["period_start"] == "2097-02-01"
    assert row["period_end"] == "2097-02-28"
    assert row["release"] == "2097-03-01"
    assert row["as_of"] == "2097-03-01"
    assert row["value"] == "20"
    assert row["value_status"] is None
    assert row["unit"] == "Index"
    assert row["dimensions"]["series_id"].startswith("TEST_API_")
    assert row["dimensions"]["seasonal_adjustment_status"] is None
    assert row["uncertainty"] is None
    assert row["coverage"] is None

    released = client.get(
        "/api/v1/observations",
        params={"metric_code": metric_a, "scope": "as_released"},
    )
    assert released.status_code == 200
    history = released.json()
    assert history["total"] == 2
    assert [item["value"] for item in history["items"]] == ["10", "20"]
    assert [item["release"] for item in history["items"]] == [
        "2097-02-01",
        "2097-03-01",
    ]

    pinned = client.get(
        "/api/v1/observations",
        params={
            "metric_code": metric_a,
            "scope": "as_released",
            "release": "2097-02-01",
        },
    )
    assert pinned.status_code == 200
    assert pinned.json()["total"] == 1
    assert pinned.json()["items"][0]["value"] == "10"

    releases = client.get(
        "/api/v1/observations/releases", params={"metric_code": metric_a}
    )
    assert releases.status_code == 200
    listing = releases.json()
    assert listing["source_code"] == "FRED"
    assert listing["total"] == 2
    assert [item["release"] for item in listing["items"]] == [
        "2097-03-01",
        "2097-02-01",
    ]
    assert all(item["observation_count"] == 1 for item in listing["items"])

    replay = client.get(
        "/api/v1/observations",
        params={"metric_code": metric_a, "scope": "as_released"},
    )
    assert replay.status_code == 200
    assert replay.content == released.content, (
        "an unchanged publication must serialize byte-identically"
    )

    unsupported = client.get(
        "/api/v1/observations",
        params={"metric_code": metric_a, "stratum_id": "not-a-fred-filter"},
    )
    assert unsupported.status_code == 422
    assert "stratum_id" in unsupported.json()["detail"]
