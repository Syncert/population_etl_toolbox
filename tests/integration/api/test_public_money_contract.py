"""The taxes and public money reads run against the real warehouse relations.

Covers: API-194
"""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.services.public_money_service import governor_terms, public_money
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


def test_every_statement_resolves_and_an_empty_warehouse_answers_nothing(
    warehouse_session: Session,
) -> None:
    """Covers: API-194 — the routes' SQL runs on the bootstrapped relations; empty is empty, not zero."""
    answer = public_money(warehouse_session, "state:55|county:025")
    assert answer.payments == []
    assert answer.income_sources == []
    assert answer.tax_burden.series == []
    assert all(point.value is None for point in answer.property_tax.values)

    terms = governor_terms(warehouse_session, "state:55")
    assert [measure.measure for measure in terms.measures] == [
        "tax_burden",
        "payments_share",
    ]
    assert terms.items == []
