"""A metric code taken from the catalog is answerable by the API.

The catalog at ``/api/v1/catalog/metrics`` is the documented discovery surface
for metric codes. Until this module existed nothing crossed it: the API's own
tests built their fixtures from one side or the other, and the web explorer
reads metrics and observations from the same endpoint family without ever
round-tripping a catalog code. Census ACS published 4,447 catalog codes that
the serving layer had never heard of, and nothing in the stack reported it.

These checks are deliberately source-agnostic. They read the reviewed
observation-dispatch registry rather than a source list, so a fifth source
cannot reintroduce the defect, and they go through the published HTTP surfaces
rather than the services behind them, because a consumer only ever has those.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.registry import OBSERVATION_DISPATCH
from data_ingestion_toolbox.fred.gold_fred import transform as fred_gold_transform
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.postgres import PostgresHookStub, PostgresTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]

#: Bounds the sweep below. The development warehouse publishes tens of
#: thousands of catalog rows, and one HTTP round trip per row would turn a
#: contract check into a load test. The sample is the source's first codes by
#: ``metric_code``, so it is deterministic rather than lucky.
SWEEP_SAMPLE = 15

ACS_TABLE = "B99997"
ACS_DATASET = "acs5"
ACS_VINTAGE = 2093
ACS_PERIOD = "2093-01-01"

FRED_VINTAGE = 2094
FRED_PERIOD = "2094-01-01"
FRED_PERIOD_END = "2094-01-31"
FRED_TIME_SK = 20940101


def _seed_time(database_cursor, time_sk: int, value: str) -> None:
    database_cursor.execute(
        """
        INSERT INTO silver_ref.dim_time (
            time_sk, date_key, year, quarter, month, day, day_of_week,
            day_name, month_name, week_of_year, is_weekend,
            is_month_start, is_month_end, is_quarter_start,
            is_quarter_end, is_year_start, is_year_end, ingested_at
        ) VALUES (
            %s, %s, EXTRACT(YEAR FROM %s::DATE), EXTRACT(QUARTER FROM %s::DATE),
            EXTRACT(MONTH FROM %s::DATE), EXTRACT(DAY FROM %s::DATE), 4,
            'Thursday', TO_CHAR(%s::DATE, 'Month'), 1, FALSE,
            TRUE, FALSE, TRUE, FALSE, TRUE, FALSE, NOW()
        ) ON CONFLICT (time_sk) DO NOTHING
        """,
        (time_sk, value, value, value, value, value, value),
    )


#: Relations the glossary harvest writes for the source it registers, beyond
#: the catalog rows themselves. Order matters for the foreign keys.
_REGISTRATION_RELATIONS = (
    "control.publisher_ready_event",
    "gold_glossary.publisher_harvest_state",
    "gold_glossary.publisher_registry",
    "gold_glossary.dim_source_system",
)


def _registration_state(
    factory: Callable[[], connection],
    source_code: str = "CENSUS_ACS",
) -> dict[str, bool]:
    """Which registration rows for ``source_code`` exist before the harvest."""
    reader = factory()
    try:
        with reader.cursor() as database_cursor:
            state = {}
            for relation in _REGISTRATION_RELATIONS:
                database_cursor.execute(
                    f"SELECT EXISTS (SELECT 1 FROM {relation} WHERE source_code = %s)",
                    (source_code,),
                )
                state[relation] = bool(database_cursor.fetchone()[0])
            return state
    finally:
        reader.close()


def _remove_registration(
    database_cursor,
    existed_before: dict[str, bool],
    source_code: str = "CENSUS_ACS",
) -> None:
    """Remove the registration rows the harvest created, and only those."""
    for relation in _REGISTRATION_RELATIONS:
        if existed_before[relation]:
            continue
        if relation == "gold_glossary.dim_source_system":
            # Another suite's catalog rows may still reference the source
            # system; it is only this fixture's to remove when nothing does.
            database_cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM gold_glossary.dim_metric_catalog "
                "WHERE source_code = %s)",
                (source_code,),
            )
            if database_cursor.fetchone()[0]:
                continue
        database_cursor.execute(
            f"DELETE FROM {relation} WHERE source_code = %s", (source_code,)
        )


@pytest.fixture
def api_client(
    postgres_test_config: PostgresTestConfig,
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[TestClient]:
    """The real application over the real warehouse."""
    engine = create_engine(
        "postgresql+psycopg2://"
        f"{postgres_test_config.user}:{postgres_test_config.password}"
        f"@{postgres_test_config.host}:{postgres_test_config.port}"
        f"/{postgres_test_config.database}"
    )

    def _session() -> Iterator[Session]:
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_db_session_dep] = _session
    try:
        yield TestClient(app, raise_server_exceptions=False)
    finally:
        app.dependency_overrides.clear()
        engine.dispose()


@pytest.fixture
def published_acs_metric(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[str]:
    """Publish one ACS metric the way production does, end to end.

    Silver rows go through the real gold refresh procedures, and the real
    glossary harvest reads ``gold_census.metric_publisher`` into the catalog.
    Neither side is told what the other spelled, which is the whole point: the
    code this fixture yields is the catalog's own, and the serving rows are
    whatever the refresh procedure composed.
    """
    token = uuid4().hex[:8].upper()
    variable_code = f"{ACS_TABLE}_{token}E"
    geo_id = "state:95"

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as database_cursor:
            _seed_time(database_cursor, 20930101, ACS_PERIOD)
            geo_sk = seed_geography(
                database_cursor,
                geo_type="state",
                state_fips="95",
                vintage=ACS_VINTAGE,
                name="Catalog agreement state",
            )
            database_cursor.execute(
                """
                INSERT INTO gold_census.dim_acs_table (
                    dataset_code, vintage_year, table_id, table_title,
                    concept, universe, survey_span_years, reference_url
                ) VALUES (%s, %s, %s, 'Catalog agreement table',
                          'Total population', 'Total population', 5,
                          'https://example.test/acs')
                ON CONFLICT (dataset_code, vintage_year, table_id) DO NOTHING
                RETURNING acs_table_sk
                """,
                (ACS_DATASET, ACS_VINTAGE, ACS_TABLE),
            )
            row = database_cursor.fetchone()
            if row is None:
                database_cursor.execute(
                    """
                    SELECT acs_table_sk FROM gold_census.dim_acs_table
                    WHERE dataset_code = %s AND vintage_year = %s AND table_id = %s
                    """,
                    (ACS_DATASET, ACS_VINTAGE, ACS_TABLE),
                )
                row = database_cursor.fetchone()
            acs_table_sk = row[0]
            database_cursor.execute(
                """
                INSERT INTO gold_census.dim_acs_variable (
                    acs_table_sk, dataset_code, vintage_year, variable_code,
                    variable_label, concept, universe, value_role
                ) VALUES (%s, %s, %s, %s, 'Total population',
                          'Total population', 'Total population', 'ESTIMATE')
                """,
                (acs_table_sk, ACS_DATASET, ACS_VINTAGE, variable_code),
            )
            database_cursor.execute(
                """
                INSERT INTO silver_census.fact_demographics (
                    time_sk, geo_sk, duration_start, duration_end,
                    estimate_year, dataset, table_id, variable_code,
                    geo_level, geo_id, state_fips, estimate_value,
                    margin_of_error, variable_label, load_batch_id
                ) VALUES (20930101, %s, %s, '2093-12-31', %s, %s, %s, %s,
                          'state', %s, '95', 1234, 12,
                          'Total population', gen_random_uuid())
                """,
                (
                    geo_sk,
                    ACS_PERIOD,
                    ACS_VINTAGE,
                    ACS_DATASET,
                    ACS_TABLE,
                    variable_code,
                    geo_id,
                ),
            )
            database_cursor.execute(
                "CALL gold_census.refresh_dashboard_serving_layer_acs(%s, %s, TRUE)",
                (ACS_PERIOD, "2093-12-31"),
            )
        writer.commit()
    finally:
        writer.close()

    # The real harvest registers the source as a side effect: a
    # publisher_registry row, a publisher_harvest_state row, a dim_source_system
    # upsert, and any ready events. On a bootstrapped warehouse none of those
    # exist yet, and a later suite asserts on that emptiness (DB-024), so the
    # cleanup below removes exactly the rows this fixture caused to exist.
    registered_before = _registration_state(postgres_connection_factory)
    harvest_publisher(postgres_connection_factory, Publisher("gold_census"))

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as database_cursor:
            database_cursor.execute(
                """
                SELECT metric_code FROM gold_glossary.dim_metric_catalog
                WHERE source_code = 'CENSUS_ACS' AND source_object_key = %s
                """,
                (f"{ACS_DATASET}:{variable_code}",),
            )
            published = database_cursor.fetchone()
    finally:
        reader.close()
    assert published is not None, "the harvest published no catalog row to check"

    try:
        yield published[0]
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as database_cursor:
                database_cursor.execute(
                    "DELETE FROM gold_census.mv_acs_latest WHERE variable_code = %s",
                    (variable_code,),
                )
                database_cursor.execute(
                    "DELETE FROM gold_census.rpt_acs_observations "
                    "WHERE variable_code = %s",
                    (variable_code,),
                )
                database_cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog "
                    "WHERE source_object_key = %s AND source_code = 'CENSUS_ACS'",
                    (f"{ACS_DATASET}:{variable_code}",),
                )
                database_cursor.execute(
                    "DELETE FROM gold_census.dim_acs_variable WHERE variable_code = %s",
                    (variable_code,),
                )
                database_cursor.execute(
                    "DELETE FROM silver_census.fact_demographics "
                    "WHERE variable_code = %s",
                    (variable_code,),
                )
                database_cursor.execute(
                    "DELETE FROM gold_census.dim_acs_table WHERE table_id = %s",
                    (ACS_TABLE,),
                )
                delete_geography(database_cursor, geo_id)
                database_cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk = 20930101"
                )
                _remove_registration(database_cursor, registered_before)
            cleanup.commit()
        finally:
            cleanup.close()


@pytest.fixture
def published_fred_metric(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[str]:
    """Publish one FRED metric the way production does, end to end.

    The ACS fixture above guarantees this guard is exercised on a source whose
    grains have always been derived. FRED is the second, and until 2026-09-12
    it was the counter-case: ``gold_fred.metric_publisher`` declared
    ``ARRAY['NATIONAL']`` for every series, so the sweep would have reported a
    national grain for a series serving nothing without ever reading a row.
    """
    series_id = f"TEST_AGREEMENT_FRED_{uuid4().hex[:8].upper()}"

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as database_cursor:
            _seed_time(database_cursor, FRED_TIME_SK, FRED_PERIOD)
            seed_geography(
                database_cursor,
                geo_type="nation",
                vintage=FRED_VINTAGE,
                name="Catalog agreement nation",
            )
            database_cursor.execute(
                """
                INSERT INTO silver_fred.fact_economic_indicators (
                    time_sk, duration_start, duration_end, observation_date,
                    series_id, domain, value, is_missing, series_title,
                    unit_of_measure, frequency, seasonal_adjustment,
                    source_system, load_batch_id, ingested_at
                ) VALUES (
                    %s, %s, %s, %s, %s, 'fixture', 7.5, FALSE,
                    'Catalog agreement series', 'Index', 'Monthly',
                    'Not Adjusted', 'FRED', gen_random_uuid(), NOW()
                )
                """,
                (FRED_TIME_SK, FRED_PERIOD, FRED_PERIOD_END, FRED_PERIOD, series_id),
            )
        writer.commit()
    finally:
        writer.close()

    fred_gold_transform.refresh_fred_elements(
        PostgresHookStub(postgres_connection_factory)
    )

    # Serve before harvesting. The grain is read from the served relation, so
    # the reverse order publishes the empty array -- which the ingest DAG
    # already gets right and a fixture is free to get wrong.
    refresher = postgres_connection_factory()
    try:
        with refresher.cursor() as database_cursor:
            database_cursor.execute(
                "CALL gold_fred.refresh_dashboard_serving_layer_fred(%s, %s, TRUE)",
                (FRED_PERIOD, FRED_PERIOD_END),
            )
        refresher.commit()
    finally:
        refresher.close()

    registered_before = _registration_state(postgres_connection_factory, "FRED")
    harvest_publisher(postgres_connection_factory, Publisher("gold_fred"))

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as database_cursor:
            database_cursor.execute(
                """
                SELECT metric_code FROM gold_glossary.dim_metric_catalog
                WHERE source_code = 'FRED' AND source_object_key = %s
                """,
                (series_id,),
            )
            published = database_cursor.fetchone()
    finally:
        reader.close()
    assert published is not None, "the harvest published no FRED catalog row"

    try:
        yield published[0]
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as database_cursor:
                for statement in (
                    "DELETE FROM gold_fred.mv_fred_latest WHERE series_id = %s",
                    "DELETE FROM gold_fred.rpt_fred_observations WHERE series_id = %s",
                    "DELETE FROM gold_glossary.dim_metric_catalog "
                    "WHERE source_code = 'FRED' AND source_object_key = %s",
                    # gold_fred.fact_fred_observation is a view over silver, so
                    # the silver delete below is what removes the fact rows.
                    "DELETE FROM gold_fred.dim_fred_series WHERE series_id = %s",
                    "DELETE FROM silver_fred.fact_economic_indicators WHERE series_id = %s",
                ):
                    database_cursor.execute(statement, (series_id,))
                database_cursor.execute(
                    "DELETE FROM control.serving_refresh_chunk_state "
                    "WHERE source_code = 'FRED'"
                )
                database_cursor.execute(
                    "DELETE FROM control.serving_refresh_state WHERE source_code = 'FRED'"
                )
                _remove_registration(database_cursor, registered_before, "FRED")
            cleanup.commit()
        finally:
            cleanup.close()


def _current_catalog_codes(client: TestClient, source_code: str) -> list[str]:
    response = client.get(
        "/api/v1/catalog/metrics",
        params={"source_code": source_code, "limit": 1000},
    )
    assert response.status_code == 200, response.text
    return sorted(
        item["metric_code"]
        for item in response.json()["items"]
        if item.get("freshness_state") == "current"
    )


def _answers(client: TestClient, metric_code: str) -> int:
    response = client.get(
        "/api/v1/observations", params={"metric_code": metric_code, "limit": 1}
    )
    assert response.status_code == 200, f"{metric_code}: {response.text}"
    return int(response.json()["total"])


def test_a_catalog_code_is_answerable_by_the_observation_resource(
    api_client: TestClient, published_acs_metric: str
) -> None:
    """Covers: API-067 — a code read from the catalog answers with rows.

    Covers: ARC-005 — this is the ACS defect end to end. The catalog published
    ``CENSUS_ACS:<dataset>:<variable>`` and the serving relations stored
    ``ACS:<dataset>:<variable>``, so this request answered ``{"total": 0}``
    while the same rows were sitting in ``gold_census.mv_acs_latest``.
    """
    assert published_acs_metric.startswith("CENSUS_ACS:")
    assert _answers(api_client, published_acs_metric) >= 1


def test_the_serving_relation_stores_the_code_the_catalog_publishes(
    postgres_connection_factory: Callable[[], connection],
    published_acs_metric: str,
) -> None:
    """Covers: DB-025 — the two published surfaces spell one identity."""
    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as database_cursor:
            database_cursor.execute(
                "SELECT COUNT(*) FROM gold_census.mv_acs_latest WHERE metric_code = %s",
                (published_acs_metric,),
            )
            latest = database_cursor.fetchone()[0]
            database_cursor.execute(
                "SELECT COUNT(*) FROM gold_census.rpt_acs_observations "
                "WHERE metric_code = %s",
                (published_acs_metric,),
            )
            history = database_cursor.fetchone()[0]
    finally:
        database_connection.close()

    assert latest >= 1, (
        f"the catalog publishes {published_acs_metric} but "
        "gold_census.mv_acs_latest holds no row under that code"
    )
    assert history >= 1


def test_no_acs_serving_row_survives_under_the_abandoned_spelling(
    postgres_connection_factory: Callable[[], connection],
    published_acs_metric: str,
) -> None:
    """Covers: DB-026 — a re-serve leaves no row under the retired spelling.

    The catalog's published codes do not change under this identity decision,
    so nothing in the catalog retires. What must not survive is a serving row
    still keyed on the abandoned ``ACS:`` spelling: the refresh deletes and
    re-inserts by ``metric_code``, so a partially re-served warehouse would
    carry both identities and answer differently depending on which one a
    consumer happened to hold.
    """
    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as database_cursor:
            for relation in (
                "gold_census.mv_acs_latest",
                "gold_census.rpt_acs_observations",
            ):
                database_cursor.execute(
                    f"SELECT COUNT(*) FROM {relation} WHERE metric_code LIKE 'ACS:%%'"
                )
                stale = database_cursor.fetchone()[0]
                assert stale == 0, (
                    f"{relation} still holds {stale} row(s) under the abandoned "
                    "'ACS:' spelling; the catalog advertises none of them"
                )
    finally:
        database_connection.close()


def test_every_registered_source_answers_each_current_catalog_code(
    api_client: TestClient, published_acs_metric: str, published_cdc_metric: str
) -> None:
    """Covers: DB-025 — no registered source advertises a code it cannot serve.

    Source-agnostic by construction: the sources come from the reviewed
    observation-dispatch registry, never from a list written here.

    A source whose catalog holds no ``current`` code contributes nothing, not
    because an unresolvable code is tolerable but because an unpopulated
    warehouse publishes no codes at all -- that is a warehouse with no content
    for the source, not a source advertising something it cannot serve. The
    final assertion is what stops that from becoming a vacuous pass: at least
    one registered source must actually have been exercised, and the fixture
    above guarantees Census ACS is one of them on any warehouse.
    """
    unresolvable: list[str] = []
    exercised: list[str] = []

    for source_code in sorted(OBSERVATION_DISPATCH):
        codes = _current_catalog_codes(api_client, source_code)
        if not codes:
            continue
        for metric_code in codes[:SWEEP_SAMPLE]:
            exercised.append(f"{source_code}:{metric_code}")
            if _answers(api_client, metric_code) < 1:
                unresolvable.append(
                    f"{source_code} publishes current catalog code "
                    f"'{metric_code}', which /api/v1/observations answers with "
                    "no rows"
                )

    assert not unresolvable, "\n".join(unresolvable)
    assert exercised, (
        "no registered source published a current catalog code, so this guard "
        "proved nothing; the warehouse under test carries no catalog content"
    )
    # Census ACS is identified by a metric-code column. CDC is identified by
    # `identity_columns` -- three lineage keys composed into a row predicate --
    # and no fixture published one, so on a warehouse with no CDC content this
    # sweep skipped that whole strategy while reading green (DB-032).
    assert f"CDC:{published_cdc_metric}" in exercised, exercised


def _current_catalog_grains(
    client: TestClient, source_code: str
) -> dict[str, list[str]]:
    response = client.get(
        "/api/v1/catalog/metrics",
        params={"source_code": source_code, "active_only": "true", "limit": 1000},
    )
    assert response.status_code == 200, response.text
    return {
        item["metric_code"]: list(item.get("valid_geo_grains") or [])
        for item in sorted(
            response.json()["items"], key=lambda item: item["metric_code"]
        )
    }


def test_every_published_grain_of_a_current_code_answers_in_the_vocabulary(
    api_client: TestClient,
    published_acs_metric: str,
    published_fred_metric: str,
    published_cdc_metric: str,
) -> None:
    """Covers: DB-028 — a grain read from the catalog can be sent straight back.

    The consumer guide promises one geography-grain vocabulary on served rows
    and in ``valid_geo_grains``. Four sources broke it four ways, and no tier
    saw any of them: CDC and PEP published and served ``nation`` where the
    contract says ``NATIONAL``; USDA NASS published ``NATION`` from one column
    and filtered on another whose word is ``NATIONAL``, so every national
    statistic in its catalog was unanswerable; FBI projected a source
    identifier as the row's grain; and Census ACS declared grains from its
    dataset code, advertising 2,487 metric/grain pairs nothing served.

    Source-agnostic like DB-025: the sources come from the reviewed dispatch
    registry. For each sampled current code, every grain the catalog
    publishes for it must answer at least one row when sent as ``geo_level``,
    and every row that comes back must carry that grain, in the vocabulary.
    The ACS fixture publishes exactly the grain it seeded, so the guard is
    exercised on every warehouse rather than only where content happens to
    exist.
    """
    from apps.api.registry import GEO_GRAINS

    unanswered: list[str] = []
    off_vocabulary: list[str] = []
    exercised: list[str] = []

    for source_code in sorted(OBSERVATION_DISPATCH):
        grains_by_code = _current_catalog_grains(api_client, source_code)
        for metric_code, grains in list(grains_by_code.items())[:SWEEP_SAMPLE]:
            for grain in grains:
                exercised.append(f"{metric_code}@{grain}")
                if grain not in GEO_GRAINS:
                    off_vocabulary.append(
                        f"{source_code} publishes grain '{grain}' for '{metric_code}', "
                        f"which is not in the vocabulary {GEO_GRAINS}"
                    )
                response = api_client.get(
                    "/api/v1/observations",
                    params={"metric_code": metric_code, "geo_level": grain, "limit": 5},
                )
                assert response.status_code == 200, (
                    f"{metric_code}@{grain}: {response.text}"
                )
                payload = response.json()
                if int(payload["total"]) < 1:
                    unanswered.append(
                        f"{source_code} publishes grain '{grain}' for '{metric_code}', "
                        "which /api/v1/observations answers with no rows"
                    )
                for row in payload["items"]:
                    if row.get("geo_level") != grain:
                        off_vocabulary.append(
                            f"{metric_code}@{grain} served a row whose geo_level is "
                            f"{row.get('geo_level')!r}"
                        )

    assert not unanswered, "\n".join(unanswered)
    assert not off_vocabulary, "\n".join(off_vocabulary)
    # The fixture seeded one state row, so the derived grain is exactly STATE:
    # a declared grain would have advertised NATIONAL and COUNTY here too.
    assert f"{published_acs_metric}@STATE" in exercised
    assert f"{published_acs_metric}@NATIONAL" not in exercised
    assert f"{published_acs_metric}@COUNTY" not in exercised
    # And FRED, whose grain was declared rather than derived until 2026-09-12.
    # Its fixture serves one national row, so NATIONAL is the whole set: the
    # assertion is that the grain came from that row, not from the view.
    assert f"{published_fred_metric}@NATIONAL" in exercised
    assert f"{published_fred_metric}@STATE" not in exercised
    assert f"{published_fred_metric}@COUNTY" not in exercised
    # And CDC, the source this sweep's docstring names first and had never
    # actually asked: it is identified by `identity_columns`, and nothing
    # published a code under that strategy until DB-032 seeded one.
    assert f"{published_cdc_metric}@STATE" in exercised


# ---------------------------------------------------------------------------
# DB-030 — the published-grain sweep reaches every route that accepts a grain
# ---------------------------------------------------------------------------

PEP_VINTAGE = 2095
PEP_YEAR = 2095
PEP_ESTIMATE_DATE = "2095-07-01"  # make_date(year, 7, 1), per the fact's own check
PEP_DATASET = "pep_agreement_test"


@pytest.fixture
def published_pep_metric(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[str]:
    """Publish one Census PEP metric at the national grain, end to end.

    PEP is the source the grain mapping exists for: its reporting view
    projects ``revision.geo_type AS geo_level``, so the served relation stores
    ``nation`` under a column named for the vocabulary. The catalog publishes
    ``NATIONAL`` for it, and the two must still meet -- which is what nothing
    exercised, because no fixture published a PEP metric at all.

    Seeded through the real views: silver rows, then `population_estimate_revision`
    and `_latest` and `rpt_pep_observations` and `mv_pep_latest` compose
    themselves, then the real glossary harvest reads `gold_pep.metric_publisher`.
    Neither side is told what the other spelled.
    """
    from tests.support.capture_seed import seed_capture

    token = uuid4().hex[:8].upper()
    # POPESTIMATE-family codes carry a non-negative check; any other code
    # is unconstrained, and the sweep cares about the grain, not the measure.
    measure_code = f"AGREEMENT_{token}"

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as database_cursor:
            capture_id = seed_capture(database_cursor, "CENSUS_PEP")
            geo_sk = seed_geography(
                database_cursor,
                geo_type="nation",
                vintage=PEP_VINTAGE,
                name="Grain sweep nation",
            )
            database_cursor.execute(
                """
                INSERT INTO silver_pep.pep_dataset (
                    dataset_code, title, transport, geography_levels,
                    summary_levels, variable_families, parser_version,
                    text_encoding, release_page_url, decennial_base, is_active,
                    created_at, updated_at, series_kind, era, native_grain
                ) VALUES (
                    %s, 'Grain sweep dataset', 'bulk_csv', ARRAY['nation'],
                    ARRAY['010'], ARRAY['POP'], '1', 'utf-8',
                    'https://www.census.gov/grain-sweep', 2090, TRUE, NOW(), NOW(),
                    'postcensal', 'test', '010'
                ) ON CONFLICT (dataset_code) DO NOTHING
                """,
                (PEP_DATASET,),
            )
            database_cursor.execute(
                """
                INSERT INTO silver_pep.dim_measure (
                    metric_code, display_name, unit, value_type, is_component,
                    allows_negative, population_universe, updated_at
                ) VALUES (%s, 'Grain sweep population', 'persons', 'count',
                          FALSE, FALSE, 'resident population', NOW())
                """,
                (measure_code,),
            )
            database_cursor.execute(
                """
                INSERT INTO silver_pep.pep_release (
                    dataset_code, vintage_year, product_code, data_url,
                    layout_url, release_date, observation_start_year,
                    observation_end_year, geography_basis_date, schema_version,
                    status, media_type, created_at, updated_at, series_kind
                ) VALUES (
                    %s, %s, 'alldata',
                    'https://www2.census.gov/grain-sweep/data.csv',
                    'https://www2.census.gov/grain-sweep/layout.txt',
                    %s, %s, %s, %s, '1', 'published', 'text/csv',
                    NOW(), NOW(), 'postcensal'
                ) ON CONFLICT DO NOTHING
                """,
                (
                    PEP_DATASET,
                    PEP_VINTAGE,
                    PEP_ESTIMATE_DATE,
                    PEP_YEAR,
                    PEP_YEAR,
                    PEP_ESTIMATE_DATE,
                ),
            )
            database_cursor.execute(
                """
                INSERT INTO silver_pep.release_load (
                    capture_id, dataset_code, release_vintage, product_code,
                    source_record_count, observation_count, completeness_status,
                    validated_at
                ) VALUES (%s, %s, %s, 'alldata', 1, 1, 'complete', NOW())
                """,
                (capture_id, PEP_DATASET, PEP_VINTAGE),
            )
            # The fact keys back to the revision it was parsed from, so the
            # parsed row exists first -- the same order the loader writes in.
            database_cursor.execute(
                """
                INSERT INTO silver_pep.observation_revision (
                    capture_id, source_row_index, source_column_index,
                    source_header, dataset_code, release_vintage, product_code,
                    observation_year, metric_code, unit, summary_level,
                    state_fips_source, name_source, value_source, value,
                    value_status, parser_version, parsed_at
                ) VALUES (
                    %s, 1, 1, %s, %s, %s, 'alldata', %s, %s, 'persons', '010',
                    NULL, 'United States', '331000000', 331000000,
                    'valid', '1', NOW()
                )
                """,
                (
                    capture_id,
                    measure_code,
                    PEP_DATASET,
                    PEP_VINTAGE,
                    PEP_YEAR,
                    measure_code,
                ),
            )
            database_cursor.execute(
                """
                INSERT INTO silver_pep.fact_population_estimate (
                    capture_id, source_row_index, source_column_index,
                    dataset_code, release_vintage, product_code, metric_code,
                    observation_year, estimate_date, geo_id, geo_sk, geo_type,
                    geography_basis_date, resolution_status, summary_level,
                    source_geo_code, source_name, value_source, value, unit,
                    transformed_at
                ) VALUES (
                    %s, 1, 1, %s, %s, 'alldata', %s, %s, %s,
                    'nation:us', %s,
                    'nation', %s, 'resolved', '010', '1', 'United States',
                    '331000000', 331000000, 'persons', NOW()
                )
                """,
                (
                    capture_id,
                    PEP_DATASET,
                    PEP_VINTAGE,
                    measure_code,
                    PEP_YEAR,
                    PEP_ESTIMATE_DATE,
                    geo_sk,
                    PEP_ESTIMATE_DATE,
                ),
            )
        writer.commit()
    finally:
        writer.close()

    registered_before = _registration_state(postgres_connection_factory, "CENSUS_PEP")
    harvest_publisher(postgres_connection_factory, Publisher("gold_pep"))

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as database_cursor:
            database_cursor.execute(
                """
                SELECT metric_code FROM gold_glossary.dim_metric_catalog
                WHERE source_code = 'CENSUS_PEP' AND source_object_key = %s
                """,
                (measure_code,),
            )
            published = database_cursor.fetchone()
    finally:
        reader.close()
    assert published is not None, "the harvest published no PEP catalog row"

    try:
        yield published[0]
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as database_cursor:
                database_cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog "
                    "WHERE source_object_key = %s AND source_code = 'CENSUS_PEP'",
                    (measure_code,),
                )
                database_cursor.execute(
                    "DELETE FROM silver_pep.fact_population_estimate "
                    "WHERE metric_code = %s",
                    (measure_code,),
                )
                database_cursor.execute(
                    "DELETE FROM silver_pep.observation_revision "
                    "WHERE dataset_code = %s",
                    (PEP_DATASET,),
                )
                database_cursor.execute(
                    "DELETE FROM silver_pep.release_load WHERE dataset_code = %s",
                    (PEP_DATASET,),
                )
                database_cursor.execute(
                    "DELETE FROM silver_pep.pep_release WHERE dataset_code = %s",
                    (PEP_DATASET,),
                )
                database_cursor.execute(
                    "DELETE FROM silver_pep.dim_measure WHERE metric_code = %s",
                    (measure_code,),
                )
                database_cursor.execute(
                    "DELETE FROM silver_pep.pep_dataset WHERE dataset_code = %s",
                    (PEP_DATASET,),
                )
                delete_geography(database_cursor, "nation:us")
                _remove_registration(database_cursor, registered_before, "CENSUS_PEP")
            cleanup.commit()
        finally:
            cleanup.close()


def test_every_published_grain_answers_on_every_route_that_accepts_one(
    api_client: TestClient, published_pep_metric: str
) -> None:
    """Covers: DB-030 — the sweep asks every route, not only the neutral one.

    DB-028 sweeps `/api/v1/observations` and nothing else, and its docstring
    says of the four defects it was written for that "no tier saw any of
    them". API-092 was the fifth, on a route it does not ask: the
    source-scoped latest route filtered the caller's vocabulary word against
    a relation storing the source's own, so `geo_level=NATIONAL` reached none
    of Census PEP's national rows. Two gaps let it through -- the sweep never
    asked that route, and no fixture published a PEP metric, so even the
    neutral sweep was vacuous for the source the mapping exists for.
    """
    from apps.api.registry import GEO_GRAINS, SERVING_CONTRACTS

    segments = {
        contract.source_code: segment for segment, contract in SERVING_CONTRACTS.items()
    }
    unanswered: list[str] = []
    off_vocabulary: list[str] = []
    exercised: list[str] = []

    for source_code, segment in sorted(segments.items()):
        grains_by_code = _current_catalog_grains(api_client, source_code)
        for metric_code, grains in list(grains_by_code.items())[:SWEEP_SAMPLE]:
            for grain in grains:
                assert grain in GEO_GRAINS, (
                    f"{source_code} publishes grain '{grain}', which is not in "
                    f"the vocabulary {GEO_GRAINS}"
                )
                route = f"/api/v1/{segment}/observations/latest"
                exercised.append(f"{segment}:{metric_code}@{grain}")
                response = api_client.get(
                    route,
                    params={
                        "metric_code": metric_code,
                        "geo_level": grain,
                        "limit": 5,
                    },
                )
                assert response.status_code == 200, (
                    f"{route} {metric_code}@{grain}: {response.text}"
                )
                payload = response.json()
                if int(payload["total"]) < 1:
                    unanswered.append(
                        f"{source_code} publishes grain '{grain}' for "
                        f"'{metric_code}', which {route} answers with no rows"
                    )
                for row in payload["items"]:
                    if row.get("geo_level") != grain:
                        off_vocabulary.append(
                            f"{route} {metric_code}@{grain} served a row whose "
                            f"geo_level is {row.get('geo_level')!r}"
                        )

    assert not unanswered, "\n".join(unanswered)
    assert not off_vocabulary, "\n".join(off_vocabulary)
    # The PEP fixture seeds one national row, and PEP is the source whose
    # relation stores `nation` under a column named `geo_level`. If this pair
    # is absent the sweep proved nothing about the case it exists for.
    assert f"pep:{published_pep_metric}@NATIONAL" in exercised, exercised


def test_every_catalog_code_answers_on_every_route_that_accepts_one(
    api_client: TestClient, published_pep_metric: str
) -> None:
    """Covers: DB-031 — the other half of the same route family.

    DB-025 asks this question of `/api/v1/observations`; DB-030 asks the grain
    question of each source's own latest route. `/{segment}/observations/timeseries`
    accepts no `geo_level`, so no grain sweep reaches it -- and it accepts the
    same `metric_code`, and was equally unanswerable for every Census PEP
    metric until the serving contracts learned how their relations compose an
    identity.

    The geography it asks for is one the source actually published, taken from
    the latest route's own answer: asking for a geography nothing covers would
    pass while proving nothing.
    """
    from apps.api.registry import SERVING_CONTRACTS

    unresolvable: list[str] = []
    exercised: list[str] = []

    for segment, contract in sorted(SERVING_CONTRACTS.items()):
        for metric_code in _current_catalog_codes(api_client, contract.source_code)[
            :SWEEP_SAMPLE
        ]:
            latest = api_client.get(
                f"/api/v1/{segment}/observations/latest",
                params={"metric_code": metric_code, "limit": 1},
            )
            assert latest.status_code == 200, latest.text
            published = latest.json()["items"]
            if not published:
                unresolvable.append(
                    f"{contract.source_code} publishes current catalog code "
                    f"'{metric_code}', which /api/v1/{segment}/observations/latest "
                    "answers with no rows"
                )
                continue
            geo_id = published[0]["geo_id"]
            exercised.append(f"{segment}:{metric_code}@{geo_id}")
            history = api_client.get(
                f"/api/v1/{segment}/observations/timeseries",
                params={"metric_code": metric_code, "geo_id": geo_id, "limit": 5},
            )
            assert history.status_code == 200, history.text
            if int(history.json()["total"]) < 1:
                unresolvable.append(
                    f"{contract.source_code} publishes '{metric_code}' for "
                    f"'{geo_id}', which "
                    f"/api/v1/{segment}/observations/timeseries answers with no rows"
                )

    assert not unresolvable, "\n".join(unresolvable)
    assert exercised, (
        "no serving contract's source published a current catalog code, so "
        "this guard proved nothing; the warehouse under test carries no "
        "catalog content for them"
    )
    # Census PEP is the source whose relations compose an identity the catalog
    # does not publish. If it was not exercised the sweep proved nothing about
    # the case it exists for.
    assert any(entry.startswith("pep:") for entry in exercised), exercised


# ---------------------------------------------------------------------------
# DB-032 — the sweeps reach a source identified by `identity_columns`
# ---------------------------------------------------------------------------

CDC_ASSET = "cdi"
#: Socrata publishes a release watermark as epoch seconds, and both the
#: publisher view and ``gold_cdc.latest_release_observation`` order releases by
#: ``release_watermark::BIGINT``. A far-future value keeps this fixture's
#: release the newest one for its asset whatever else the database holds, so
#: the seeded row is the one the latest surface serves.
CDC_WATERMARK = "3975004800"
#: CDC periods are years, not dates: the fact table stores them as integers.
CDC_PERIOD = 2096


def _digest_token() -> str:
    """A 64-character lowercase hex token.

    ``dim_stratum.stratum_id`` and ``fact_health_observation.source_record_id``
    are both constrained to ``^[0-9a-f]{64}$`` -- they are content digests in
    production, and the schema says so. A fixture that invented a readable
    identifier would be rejected by the same constraint that keeps a
    hand-edited row out of the warehouse.
    """
    return uuid4().hex + uuid4().hex


@pytest.fixture
def published_cdc_metric(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[str]:
    """Publish one CDC metric end to end, for the third identity strategy.

    The registry identifies a metric's serving rows three ways, and
    ``identity_columns`` -- where the service binds ``lineage.get(field)`` for
    each declared column and refuses the metric if the lineage publishes no
    such key -- had never been asked to answer a code the catalog published.
    CDC is the smallest source that uses it, and its rows carry a stratum and
    an adjustment status, so the sweeps exercise more of the envelope than a
    single-series source does.
    """
    from tests.support.capture_seed import seed_capture

    token = uuid4().hex[:8].upper()
    measure_id = f"SWEEP_{token}"
    value_type_id = "crude"
    stratum_id = _digest_token()
    source_record_id = _digest_token()

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as database_cursor:
            capture_id = seed_capture(database_cursor, "CDC")
            # Both silver_cdc tables carry a foreign key to the ingestion run,
            # so the run the capture was recorded under is the one to cite --
            # a fresh uuid would name a run that never happened.
            database_cursor.execute(
                "SELECT run_id FROM raw_capture.response_capture WHERE capture_id = %s",
                (capture_id,),
            )
            run_id = database_cursor.fetchone()[0]
            geo_sk = seed_geography(
                database_cursor,
                geo_type="state",
                state_fips="94",
                vintage=CDC_PERIOD,
                name="Sweep state",
            )
            database_cursor.execute(
                """
                INSERT INTO silver_cdc.dim_dataset_release (
                    asset_id, release_watermark, socrata_id, title,
                    methodology_url, geography_basis, parser_contract_version,
                    estimate_method, population_basis, metadata_capture_id,
                    source_run_id, source_record_count, quarantine_count,
                    status, reconciled_at, published_at, created_at, updated_at
                ) VALUES (
                    %s, %s, 'abcd-1234', 'Sweep dataset',
                    'https://www.cdc.gov/sweep', 'state', '1',
                    'model-based', 'adults', %s, %s, 1, 0,
                    'published', NOW(), NOW(), NOW(), NOW()
                )
                """,
                (CDC_ASSET, CDC_WATERMARK, capture_id, run_id),
            )
            database_cursor.execute(
                """
                INSERT INTO silver_cdc.dim_measure (
                    asset_id, measure_id, value_type_id, measure_label, topic,
                    value_type_label, unit, adjustment_status, estimate_method,
                    population_basis, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, 'Sweep measure', 'Sweep topic',
                    'Crude prevalence', 'percent', 'crude', 'model-based',
                    'adults', NOW(), NOW()
                )
                """,
                (CDC_ASSET, measure_id, value_type_id),
            )
            database_cursor.execute(
                """
                INSERT INTO silver_cdc.dim_stratum (stratum_id, strata, created_at)
                VALUES (
                    %s,
                    '[["OVERALL", "Overall", "OVR", "Overall"]]'::jsonb,
                    NOW()
                )
                """,
                (stratum_id,),
            )
            database_cursor.execute(
                """
                INSERT INTO silver_cdc.fact_health_observation (
                    asset_id, release_watermark, source_record_id, source_run_id,
                    capture_id, source_row_index, measure_id, value_type_id,
                    stratum_id, period_start, period_end, geo_id, geo_sk,
                    geo_type, geography_status, value_source, value,
                    value_status, unit, adjustment_status, estimate_method,
                    population_basis, transformation_version
                ) VALUES (
                    %s, %s, %s, %s, %s, 0, %s, %s, %s,
                    %s, %s, 'state:94', %s, 'state', 'resolved',
                    '12.5', 12.5, 'valid', 'percent', 'crude', 'model-based',
                    'adults', '1'
                )
                """,
                (
                    CDC_ASSET,
                    CDC_WATERMARK,
                    source_record_id,
                    run_id,
                    capture_id,
                    measure_id,
                    value_type_id,
                    stratum_id,
                    CDC_PERIOD,
                    CDC_PERIOD,
                    geo_sk,
                ),
            )
        writer.commit()
    finally:
        writer.close()

    registered_before = _registration_state(postgres_connection_factory, "CDC")
    harvest_publisher(postgres_connection_factory, Publisher("gold_cdc"))

    source_object_key = f"{CDC_ASSET}:{measure_id}:{value_type_id}"
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as database_cursor:
            database_cursor.execute(
                """
                SELECT metric_code FROM gold_glossary.dim_metric_catalog
                WHERE source_code = 'CDC' AND source_object_key = %s
                """,
                (source_object_key,),
            )
            published = database_cursor.fetchone()
    finally:
        reader.close()
    assert published is not None, "the harvest published no CDC catalog row"

    try:
        yield published[0]
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as database_cursor:
                # `cdi` is one of two asset ids the schema permits, so every
                # delete names this fixture's own release, measure or stratum.
                # A delete scoped to the asset alone would take a neighbouring
                # fixture's rows with it.
                database_cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog "
                    "WHERE source_object_key = %s AND source_code = 'CDC'",
                    (source_object_key,),
                )
                database_cursor.execute(
                    "DELETE FROM silver_cdc.fact_health_observation "
                    "WHERE asset_id = %s AND release_watermark = %s",
                    (CDC_ASSET, CDC_WATERMARK),
                )
                database_cursor.execute(
                    "DELETE FROM silver_cdc.dim_measure "
                    "WHERE asset_id = %s AND measure_id = %s",
                    (CDC_ASSET, measure_id),
                )
                database_cursor.execute(
                    "DELETE FROM silver_cdc.dim_dataset_release "
                    "WHERE asset_id = %s AND release_watermark = %s",
                    (CDC_ASSET, CDC_WATERMARK),
                )
                database_cursor.execute(
                    "DELETE FROM silver_cdc.dim_stratum WHERE stratum_id = %s",
                    (stratum_id,),
                )
                delete_geography(database_cursor, "state:94")
                _remove_registration(database_cursor, registered_before, "CDC")
            cleanup.commit()
        finally:
            cleanup.close()


def test_a_source_identified_by_its_lineage_columns_answers_its_catalog_code(
    api_client: TestClient, published_cdc_metric: str
) -> None:
    """Covers: DB-032 — the third identity strategy, actually exercised.

    `identity_columns` binds `lineage.get(field)` for each declared column and
    refuses a metric whose lineage publishes no such key. DB-025 and DB-028
    sweep every registered source, but a source publishing no catalog row
    contributes nothing -- and until now none of the three sources using this
    strategy published one, so the strategy had never answered a code the
    catalog composed. Seeding Census PEP is what revealed DB-030; this is the
    same gap, one strategy over.
    """
    from apps.api.registry import GEO_GRAINS

    catalog = api_client.get(
        "/api/v1/catalog/metrics",
        params={"source_code": "CDC", "active_only": "true", "limit": 50},
    )
    assert catalog.status_code == 200, catalog.text
    published = {
        item["metric_code"]: list(item.get("valid_geo_grains") or [])
        for item in catalog.json()["items"]
    }
    assert published_cdc_metric in published, published

    grains = published[published_cdc_metric]
    assert grains, "the publisher derived no grain from the seeded row"
    for grain in grains:
        assert grain in GEO_GRAINS, grain
        answer = api_client.get(
            "/api/v1/observations",
            params={
                "metric_code": published_cdc_metric,
                "geo_level": grain,
                "limit": 5,
            },
        )
        assert answer.status_code == 200, answer.text
        payload = answer.json()
        assert int(payload["total"]) >= 1, (
            f"CDC publishes grain '{grain}' for '{published_cdc_metric}', "
            "which /api/v1/observations answers with no rows"
        )
        for row in payload["items"]:
            assert row["geo_level"] == grain, row
            # The stratified envelope this source is served through: its own
            # stratum and adjustment status travel with the value.
            assert row["dimensions"].get("stratum_id"), row


# ---------------------------------------------------------------------------
# DB-033 — a stratum the warehouse accepts is a stratum the API can serve
# ---------------------------------------------------------------------------


def test_the_stratum_the_warehouse_accepts_is_the_stratum_the_api_serves(
    api_client: TestClient, published_cdc_metric: str
) -> None:
    """Covers: DB-033 — the CDC source-explorer route serves its own rows.

    `CdcObservation.strata` is `list[Any]`, and `jsonb` is not. A stratum
    stored as an object was accepted by every write path and then crashed
    `CdcObservation.model_validate`, which the caller sees as
    `500 The API failed to complete this request` -- with no way to tell which
    row is unserveable, and the row still there for the next page that
    includes it. Migration 019 refuses the shape at the write; this is the
    other half, and the only tier that can see it: the row the fixture
    publishes is fetched back through the route that declares the list.

    It is also what keeps the DB-032 fixture honest. A fixture is only
    evidence while the rows it seeds are rows the warehouse could hold, and
    this one seeded a shape no parser produces until the route it feeds was
    asked to serve it.
    """
    answer = api_client.get(
        "/api/v1/cdc/observations", params={"dataset": CDC_ASSET, "limit": 50}
    )
    assert answer.status_code == 200, answer.text
    payload = answer.json()
    seeded = [
        item for item in payload["items"] if item["release_watermark"] == CDC_WATERMARK
    ]
    assert seeded, (
        f"the CDC source-explorer route answers no row for release "
        f"{CDC_WATERMARK}, which the catalog publishes as "
        f"'{published_cdc_metric}'"
    )
    for item in seeded:
        # The published shape, not merely a truthy one: a mapping validates as
        # neither, and that is exactly what used to reach the response model.
        assert isinstance(item["strata"], list), item["strata"]
        assert all(isinstance(entry, list) for entry in item["strata"]), item["strata"]


# ---------------------------------------------------------------------------
# DB-034 — a route answers the metric code it publishes
# ---------------------------------------------------------------------------


def test_every_route_answers_the_metric_code_it_published(
    api_client: TestClient, published_pep_metric: str
) -> None:
    """Covers: DB-034 — a route's own answer is a request it accepts.

    The source-scoped routes project `metric_code` from the source's own
    serving relation, and Census PEP's relation composes its identity from a
    dataset and a measure. So a page answered for the catalog's
    `CENSUS_PEP:<measure>` comes back carrying `CENSUS_PEP:<dataset>:<measure>`
    -- and asking for more of that metric, by the only identity the page gave,
    used to be an empty 200. A route refusing an identity it published in its
    own response is the route disagreeing with itself.

    Source-agnostic: the contracts come from the reviewed registry and the
    codes from the served catalog, so a contract added later is covered
    without an edit here. A source with no catalog content contributes
    nothing, and the PEP assertion below is what stops that from passing
    vacuously -- PEP is the source whose two identities differ at all.
    """
    from apps.api.registry import SERVING_CONTRACTS

    disagreements: list[str] = []
    exercised: list[str] = []

    for segment, contract in sorted(SERVING_CONTRACTS.items()):
        for catalog_code in _current_catalog_codes(api_client, contract.source_code)[
            :SWEEP_SAMPLE
        ]:
            first = api_client.get(
                f"/api/v1/{segment}/observations/latest",
                params={"metric_code": catalog_code, "limit": 1},
            )
            assert first.status_code == 200, first.text
            published = first.json()["items"]
            if not published:
                continue
            served_code = published[0]["metric_code"]
            geo_id = published[0]["geo_id"]
            exercised.append(f"{segment}:{catalog_code}->{served_code}")
            for route, params in (
                (
                    f"/api/v1/{segment}/observations/latest",
                    {"metric_code": served_code, "limit": 1},
                ),
                (
                    f"/api/v1/{segment}/observations/timeseries",
                    {"metric_code": served_code, "geo_id": geo_id, "limit": 1},
                ),
            ):
                answer = api_client.get(route, params=params)
                assert answer.status_code == 200, answer.text
                if int(answer.json()["total"]) < 1:
                    disagreements.append(
                        f"{route} published metric_code '{served_code}' for "
                        f"catalog code '{catalog_code}', and answers no rows "
                        "when asked for it"
                    )

    assert not disagreements, "\n".join(disagreements)
    # Census PEP is the source whose published identity and catalog identity
    # differ at all. If it was not exercised the sweep proved nothing.
    assert any(
        entry.startswith(f"pep:{published_pep_metric}->") for entry in exercised
    ), exercised
