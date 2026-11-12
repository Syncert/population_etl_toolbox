"""Real PostgreSQL NCES IPEDS capture-to-gold contract.

Covers: ETL-131
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from tests.support import nces_ipeds

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def ipeds_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return nces_ipeds.reviewed_warehouse(postgres_connection_factory, request)


def _rows(
    factory: Callable[[], connection], sql: str, parameters: tuple = ()
) -> list[tuple]:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


def test_every_file_reaches_gold_and_dane_sums_its_institutions(
    ipeds_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-131 -- each served file stored whole; Dane's sums are its institutions' figures."""
    results = nces_ipeds.load_files(ipeds_warehouse)
    by_stem = {
        stem: (status, stored, published)
        for stem, _run, status, stored, published in results
    }
    assert by_stem["HD2023"] == ("captured", 89, 1)
    assert by_stem["IC2023"] == ("captured", 87, 1)
    assert by_stem["EF2023A"][2] == 1
    assert by_stem["HD2012"] == ("captured", 1, 1)
    assert by_stem["EF2024A"] == ("not_published", 0, 0)
    assert by_stem["HD2015"] == ("not_published", 0, 0)
    assert "IC2024" not in by_stem

    served = dict(
        _rows(
            ipeds_warehouse,
            """
            SELECT metric_key || '@' || period_year::TEXT, value
            FROM gold_nces_ipeds.area_latest WHERE geo_id = %s
            """,
            (nces_ipeds.DANE,),
        )
    )
    assert served["ipeds:housing_capacity@2023"] == 9484
    assert served["ipeds:fall_enrolment_total@2023"] == 69371
    assert served["ipeds:housing_capacity@2012"] == 8471
    assert served["ipeds:fall_enrolment_total@2012"] == 42269
    ((count, unplaced),) = _rows(
        ipeds_warehouse,
        """
        SELECT institution_count, unplaced_institution_count
        FROM gold_nces_ipeds.area_latest
        WHERE geo_id = %s AND metric_key = 'ipeds:housing_capacity' AND period_year = 2023
        """,
        (nces_ipeds.DANE,),
    )
    assert (count, unplaced) == (2, 0)

    ((state_total,),) = _rows(
        ipeds_warehouse,
        """
        SELECT value FROM gold_nces_ipeds.area_latest
        WHERE geo_id = 'state:55' AND metric_key = 'ipeds:housing_capacity' AND period_year = 2023
        """,
    )
    assert state_total == 65846


def test_a_not_applicable_capacity_is_kept_without_a_number(
    ipeds_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-131 -- Madison College provides no housing: shown as not applicable, never 0."""
    nces_ipeds.load_files(ipeds_warehouse)
    rows = dict(
        (unitid, (name, capacity, status, county))
        for unitid, name, capacity, status, county in _rows(
            ipeds_warehouse,
            """
            SELECT unitid, institution_name, housing_capacity, housing_capacity_status,
                   county_geo_id
            FROM gold_nces_ipeds.institution_year
            WHERE period_year = 2023 AND county_geo_id = %s
            """,
            (nces_ipeds.DANE,),
        )
    )
    assert rows[nces_ipeds.UW_MADISON] == (
        "University of Wisconsin-Madison",
        8949,
        "valid",
        nces_ipeds.DANE,
    )
    assert rows["238263"][1:3] == (None, "not_applicable")
    assert len(rows) == 9


def test_a_second_run_reads_only_what_is_unanswered_and_history_writes_nothing_new(
    ipeds_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-131 -- published files are not asked again; identical bytes are unchanged."""
    nces_ipeds.load_files(ipeds_warehouse)
    (before,) = _rows(
        ipeds_warehouse, "SELECT COUNT(*) FROM silver_nces_ipeds.institution_value"
    )
    again = nces_ipeds.load_files(ipeds_warehouse)
    assert {status for _stem, _run, status, _stored, _published in again} == {
        "not_published"
    }
    history = nces_ipeds.load_files(ipeds_warehouse, conf={"history": True})
    statuses = {stem: status for stem, _run, status, _stored, _published in history}
    assert (
        statuses["HD2023"] == statuses["IC2023"] == statuses["EF2023A"] == "unchanged"
    )
    (after,) = _rows(
        ipeds_warehouse, "SELECT COUNT(*) FROM silver_nces_ipeds.institution_value"
    )
    assert after == before
