"""The where-public-money-goes read runs against the real warehouse relations.

Covers: API-196
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import date

import pytest
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.services.where_money_goes_service import where_money_goes
from tests.support import usaspending as usa
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


def test_every_statement_resolves_and_an_empty_place_answers_nothing(
    warehouse_session: Session,
) -> None:
    """Covers: API-196 — the read's SQL runs on the bootstrapped relations; empty is empty, not zero."""
    for geo_id in ("us:1", "state:55", "state:55|county:025"):
        answer = where_money_goes(warehouse_session, geo_id, today=date(2025, 3, 1))
        for layer in answer.layers:
            if layer.lines:
                continue
            assert layer.total is None
            assert layer.unavailable_reason
        warehouse_session.rollback()


def test_dane_county_grants_are_served_as_usaspending_published_them(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
    warehouse_session: Session,
) -> None:
    """Covers: API-196 — fiscal 2024 awards recorded in Dane County, with no share where no total is published."""
    factory = usa.reviewed_warehouse(postgres_connection_factory, request)
    usa.run_to_gold(factory)
    answer = where_money_goes(warehouse_session, usa.DANE, today=usa.TODAY)
    by_type = next(
        layer for layer in answer.layers if layer.layer == "federal_awards_by_type"
    )
    assert by_type.fiscal_year == 2024
    grants = next(line for line in by_type.lines if line.key == "grants")
    assert grants.amount == usa.DANE_FY2024_GRANTS
    assert grants.label == "Grants"
    # The fixtures publish no all-award county read, so no share is made.
    assert (grants.share_pct, by_type.total) == (None, None)
    assert (
        grants.unavailable_reason
        == "all-award total not published for this place and year"
    )

    state = where_money_goes(warehouse_session, usa.WISCONSIN, today=usa.TODAY)
    state_types = next(
        layer for layer in state.layers if layer.layer == "federal_awards_by_type"
    )
    assert (state_types.fiscal_year, state_types.total) == (
        2024,
        usa.WI_FY2024_ALL_AWARDS,
    )
