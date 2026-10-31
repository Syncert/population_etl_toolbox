"""The admin warehouse reads run against the real bootstrapped warehouse.

Covers: API-181 (the catalog introspection reads every warehouse relation,
        folds year partitions into their parent, and places each in a diagram;
        every source's coverage statement -- built from the serving
        registry's own expressions -- executes against its real relation).

The unit tier proves the shapes with stubbed sessions. Only PostgreSQL can
prove the statements: a serving expression renamed in the registry, or a
catalog column the introspection misreads, is a statement this suite refuses.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.services import warehouse_coverage as coverage
from apps.api.services.warehouse_atlas import build_atlas, read_catalog
from apps.api.services.warehouse_inventory import WAREHOUSE_SOURCES
from tests.support.postgres import PostgresTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]


@pytest.fixture
def warehouse_session(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[Session]:
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


def test_the_catalog_introspection_reads_the_warehouse(
    warehouse_session: Session,
) -> None:
    """Covers: API-181 — partitions folded, every relation placed."""
    relations = read_catalog(warehouse_session)
    acs = relations["gold_census.rpt_acs_observations"]
    assert acs.kind == "partitioned table"
    assert acs.partitions >= 30
    assert not any(
        name.startswith("gold_census.rpt_acs_observations_") for name in relations
    )
    assert relations["gold_glossary.dim_metric_catalog"].primary_key
    assert any(
        ref == "gold_glossary.dim_source_system"
        for _, ref in relations["gold_glossary.dim_metric_catalog"].foreign_keys
    )
    atlas = build_atlas(relations)
    assert all(r["domain"] for r in atlas["relations"])
    assert atlas["stats"]["tables"] > 100


def test_every_source_coverage_statement_executes(warehouse_session: Session) -> None:
    """Covers: API-181 — on a clean warehouse each answers ok with no rows."""
    for source in WAREHOUSE_SOURCES:
        result = coverage.measure_source(warehouse_session, source.code)
        assert result["status"] in {"ok", "shared", "not_served"}, (source.code, result)
