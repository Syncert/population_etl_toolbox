"""Every declared serving expression resolves against the real relations.

The observation dispatch registry is a reviewed set of SQL fragments per
source: the relation for each scope, the metric-identity column, the period
and release expressions, the geography expressions, each published
dimension, each filter condition, and the paging order. A typo in any of
them is a statement PostgreSQL refuses, which the API answers as a sanitized
503 -- and the existing coverage cannot see it for a source with no content.

`test_real_database_contract` drives the neutral resource for the one seeded
source (FRED). `test_catalog_serving_agreement` sweeps every source, but
through the *catalog*: it asks for the metrics a source publishes, so on a
warehouse where a source has published nothing there is nothing to ask and
the sweep passes over it. Six of the seven sources are in that position on a
clean bootstrap, and a broken expression for any of them would reach a
deployment.

This drives the production path instead, with the catalog lookup -- and only
that -- stubbed: a synthetic metric row per source carrying the identity and
the lineage the registry declares, and a metric code no row can match. Every
statement therefore runs against the real serving relations and answers zero
rows, which is exactly what proves each expression resolves.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.registry import OBSERVATION_DISPATCH
from apps.api.services import neutral_observations_service as neutral
from tests.support.postgres import PostgresTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]

#: A metric code the identity column of no serving relation can hold. The
#: prefix is a published source code so nothing rejects its shape, and the
#: tail is what makes it match nothing.
UNMATCHABLE_METRIC = "DISPATCH_PROBE:no-such-measure"


@pytest.fixture
def warehouse_session(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[Session]:
    """A session on the bootstrapped warehouse, rolled back afterwards."""
    del postgres_connection_factory
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
    )
    session = Session(bind=engine)
    try:
        yield session
    finally:
        session.rollback()
        session.close()
        engine.dispose()


def _synthetic_metric(source_code: str) -> dict[str, object]:
    """A catalog row shaped the way the registry says this source's rows are.

    The identity a source binds on is its own: a `metric_code` column for
    some, a lineage `key` for others, and a set of `identity_columns` read
    out of the published lineage for the rest. Each is filled with a value no
    row can hold, which is what makes the statement run and answer nothing.
    """
    dispatch = OBSERVATION_DISPATCH[source_code]
    lineage: dict[str, object] = {
        "schema": dispatch.lineage_schema,
        "relation": dispatch.lineage_relation,
    }
    if dispatch.lineage_key_column is not None:
        lineage["key"] = "no-such-key"
    for field in dispatch.identity_columns:
        lineage[field] = "no-such-identity"
    return {
        "metric_code": UNMATCHABLE_METRIC,
        "source_code": source_code,
        "units": None,
        "physical_lineage": lineage,
    }


@pytest.mark.parametrize("source_code", sorted(OBSERVATION_DISPATCH))
@pytest.mark.parametrize("scope", ["latest", "as_released"])
def test_a_sources_declared_expressions_execute(
    warehouse_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    source_code: str,
    scope: str,
) -> None:
    """Covers: API-128 — every dispatch expression is SQL the warehouse accepts."""
    monkeypatch.setattr(
        neutral, "resolve_metric", lambda db, code: _synthetic_metric(source_code)
    )
    answer = neutral.list_neutral_observations(
        warehouse_session,
        metric_code=UNMATCHABLE_METRIC,
        scope=scope,
        release=None,
        filters={},
        limit=1,
        offset=0,
    )
    assert answer is not None
    assert answer.total == 0, (
        f"{source_code} answered rows for a metric code nothing can hold, so "
        f"this probe is not proving what it claims"
    )
    assert answer.items == []


@pytest.mark.parametrize("source_code", sorted(OBSERVATION_DISPATCH))
def test_every_declared_filter_executes_for_its_source(
    warehouse_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    source_code: str,
) -> None:
    """Covers: API-128 — a filter a source declares is a condition that runs.

    Read from the dispatch entry's own `supported_filters()`, which is what
    `/catalog/capabilities` publishes, so a filter advertised to clients and
    broken in SQL cannot pass. Each is sent one at a time: a value that
    matches nothing is enough to make PostgreSQL parse and plan the
    condition, which is where a wrong column name fails.
    """
    dispatch = OBSERVATION_DISPATCH[source_code]
    declared = [name for name in dispatch.supported_filters()]
    assert declared, f"{source_code} declares no observation filter"
    monkeypatch.setattr(
        neutral, "resolve_metric", lambda db, code: _synthetic_metric(source_code)
    )
    probes = {
        "geo_level": "COUNTY",
        "geo_id": "state:99|county:999",
        "state_fips": "99",
        "county_fips": "999",
        "year_from": 1799,
        "year_to": 1800,
    }
    for name in declared:
        answer = neutral.list_neutral_observations(
            warehouse_session,
            metric_code=UNMATCHABLE_METRIC,
            scope="latest",
            release=None,
            filters={name: probes.get(name, "probe-value")},
            limit=1,
            offset=0,
        )
        assert answer is not None and answer.total == 0, name


@pytest.mark.parametrize("source_code", sorted(OBSERVATION_DISPATCH))
def test_a_declared_reduction_executes_for_its_source(
    warehouse_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    source_code: str,
) -> None:
    """Covers: API-128 — the ranked reductions are SQL too.

    Each reduction wraps the source's own relation in a `ROW_NUMBER()` over
    the declared order, so it exercises the order columns as well as the
    projection. A source that refuses a reduction is asked for it and must
    explain rather than execute.
    """
    dispatch = OBSERVATION_DISPATCH[source_code]
    monkeypatch.setattr(
        neutral, "resolve_metric", lambda db, code: _synthetic_metric(source_code)
    )
    for reduction, scope in (
        ("newest_per_geography", "latest"),
        ("newest_release_per_period", "as_released"),
    ):
        refusal = neutral.reduction_refusal(dispatch, reduction)
        if refusal is not None:
            with pytest.raises(neutral.NeutralQueryError):
                neutral.list_neutral_observations(
                    warehouse_session,
                    metric_code=UNMATCHABLE_METRIC,
                    scope=scope,
                    release=None,
                    filters={},
                    limit=1,
                    offset=0,
                    **{reduction: True},
                )
            continue
        answer = neutral.list_neutral_observations(
            warehouse_session,
            metric_code=UNMATCHABLE_METRIC,
            scope=scope,
            release=None,
            filters={},
            limit=1,
            offset=0,
            **{reduction: True},
        )
        assert answer is not None and answer.total == 0, reduction
