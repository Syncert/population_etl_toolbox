"""Return A master files load every agency, resumably (ETL-107).

The fixture is 47 verbatim 2023 records from the FBI's ``reta-2023.txt``:
every agency in Wisconsin's Dane (FBI county 013) and Brown (005) counties,
Richland County's (052), three Travis County, Texas, agencies, Camden County
Police (New Jersey, the year's only negative entry) and Wisconsin Dells (three
counties). The load runs through a stand-in download that serves the
fixture as the zip the CDE serves.
"""

from __future__ import annotations

import dataclasses
import io
import zipfile
from collections.abc import Callable, Iterator
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from psycopg2 import errors
from psycopg2.extensions import connection

from data_ingestion_toolbox.fbi_ucr.return_a.fetch import FetchedFile, object_key
from data_ingestion_toolbox.fbi_ucr.return_a.layout import iter_records, violent_total
from data_ingestion_toolbox.fbi_ucr.return_a.load import load
from tests.support import fbi_release
from tests.support.capture_seed import delete_geography, seed_geography

pytestmark = [pytest.mark.integration, pytest.mark.database]

SAMPLE = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "fbi_ucr"
    / "return_a"
    / "reta-2023-sample.txt"
)
RELEASE = "2026-08-20"
DANE = "state:55|county:025"

#: Counties the sample names beyond the reviewed FBI geographies.
EXTRA_GEOGRAPHIES: tuple[dict[str, object], ...] = (
    {"geo_type": "state", "state_fips": "48", "name": "Texas"},
    {
        "geo_type": "county",
        "state_fips": "55",
        "county_fips": "103",
        "name": "Richland County",
    },
    {
        "geo_type": "county",
        "state_fips": "48",
        "county_fips": "453",
        "name": "Travis County",
    },
)


def _zip(text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        # A fixed timestamp: the same file must be the same bytes, as the
        # CDE's own unchanged file is.
        member = zipfile.ZipInfo("reta-2023.txt", date_time=(2026, 8, 20, 0, 0, 0))
        archive.writestr(member, text.encode("latin-1"))
    return buffer.getvalue()


def _fetch(year: int) -> FetchedFile:
    assert year == 2023
    return FetchedFile(
        data_year=2023,
        object_key=object_key(2023),
        payload=_zip(SAMPLE.read_text(encoding="latin-1")),
        last_modified=date.fromisoformat(RELEASE),
        http_status=200,
        response_headers={"Content-Type": "application/zip"},
    )


@pytest.fixture
def return_a_warehouse(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[Callable[[], connection]]:
    seeded: list[str] = []
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            for entry in EXTRA_GEOGRAPHIES:
                cursor.execute(
                    "SELECT 1 FROM silver_ref.dim_geo_entity WHERE geo_type = %s"
                    " AND state_fips = %s AND county_fips IS NOT DISTINCT FROM %s",
                    (entry["geo_type"], entry["state_fips"], entry.get("county_fips")),
                )
                if cursor.fetchone() is None:
                    geo_sk = seed_geography(cursor, vintage=2023, **entry)  # type: ignore[arg-type]
                    cursor.execute(
                        "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_sk = %s",
                        (geo_sk,),
                    )
                    seeded.append(cursor.fetchone()[0])
        writer.commit()
    finally:
        writer.close()
    try:
        yield from fbi_release.reviewed_warehouse(postgres_connection_factory)
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                for geo_id in seeded:
                    cursor.execute("SAVEPOINT extra_geo")
                    try:
                        delete_geography(cursor, geo_id)
                    except errors.ForeignKeyViolation:
                        cursor.execute("ROLLBACK TO SAVEPOINT extra_geo")
            cleanup.commit()
        finally:
            cleanup.close()


def _rows(factory: Callable[[], connection], sql: str, *params: object) -> list[tuple]:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()
    finally:
        reader.close()


def test_every_agency_loads_and_dane_county_is_read_with_madison(
    return_a_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-107 — every agency, every month, Madison in Dane's rate."""
    summary = load(return_a_warehouse, [2023], fetch=_fetch)
    assert summary["release_key"] == RELEASE
    assert [item["status"] for item in summary["years"]] == ["loaded"]
    assert summary["years"][0]["records"] == 47

    # The Sheriff's January matches the CDE's own summarized row (6), as do
    # Fitchburg (13) and Madison (71).
    january = dict(
        _rows(
            return_a_warehouse,
            """
            SELECT subject_code, value FROM silver_fbi.fact_crime_observation
            WHERE product_id = 'return_a_violent_crime' AND period = '01-2023'
              AND measure_id = 'V:offense:absolute_total'
              AND subject_code = ANY(%s)
            """,
            ["WI0130000", "WI0137000", "WI0130100"],
        )
    )
    assert january == {
        "WI0130000": Decimal("6"),
        "WI0137000": Decimal("13"),
        "WI0130100": Decimal("71"),
    }
    # A month with no report is absent, never zero.
    assert _rows(
        return_a_warehouse,
        """
        SELECT value, value_status FROM silver_fbi.fact_crime_observation
        WHERE product_id = 'return_a_violent_crime' AND subject_code = 'WI0131000'
          AND period = '01-2023' AND measure_id = 'V:offense:absolute_total'
        """,
    ) == [(None, "not_reported")]

    # County codes resolve from the county agency's own name.
    crosswalk = dict(
        _rows(
            return_a_warehouse,
            """
            SELECT state_code || fbi_county_code, geo_id
            FROM silver_fbi.return_a_county_crosswalk
            WHERE status = 'resolved' AND method = 'county_agency_name'
            """,
        )
    )
    assert crosswalk["WI13"] == DANE
    assert crosswalk["WI5"] == "state:55|county:009"
    assert crosswalk["TX227"] == "state:48|county:453"
    # Camden County has no county agency in the sample and New Jersey none
    # resolved to order by: it stays unresolved rather than guessed.
    assert _rows(
        return_a_warehouse,
        """
        SELECT status, reason_code FROM silver_fbi.return_a_county_crosswalk
        WHERE state_code = 'NJ' AND fbi_county_code = 4
        """,
    ) == [("unresolved", "no_county_agency_in_state")]

    # Dane County's January: the rate now counts Madison, and coverage says
    # who did not report rather than reading 100 %.
    records = list(iter_records(SAMPLE.read_text(encoding="latin-1")))
    dane = [
        record
        for record in records
        if record.county_codes == (13,)
        and not record.covered_by
        and record.state_code == "WI"
    ]
    reporting = [record for record in dane if record.months[0].is_reported]
    offenses = sum(violent_total(record.months[0].actual) for record in reporting)
    covered = sum(record.population for record in reporting)
    linked = sum(record.population for record in dane)
    row = _rows(
        return_a_warehouse,
        """
        SELECT unavailable_reason, offenses, covered_population,
               linked_population, contributing_oris, not_loaded_oris,
               ROUND(rate, 4)
        FROM gold_fbi.latest_county_agency_rate
        WHERE product_id = 'return_a_violent_crime' AND geo_id = %s
          AND period = '01-2023'
        """,
        DANE,
    )
    assert len(row) == 1
    (
        reason,
        served_offenses,
        served_covered,
        served_linked,
        contributing,
        missing,
        rate,
    ) = row[0]
    assert reason is None
    assert missing == []
    assert "WI0130100" in contributing
    assert (served_offenses, served_covered, served_linked) == (
        offenses,
        covered,
        linked,
    )
    assert served_covered > 400_000 > 117_115
    assert rate == round(Decimal(offenses) / Decimal(covered) * 100_000, 4)


def test_a_rerun_and_a_resumed_run_change_nothing(
    return_a_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-107 — a year already loaded with the same file is skipped."""
    load(return_a_warehouse, [2023], fetch=_fetch)
    count = "SELECT COUNT(*) FROM silver_fbi.fact_crime_observation"
    before = _rows(return_a_warehouse, count)
    again = load(return_a_warehouse, [2023], fetch=_fetch)
    assert [item["status"] for item in again["years"]] == ["already_loaded"]
    assert _rows(return_a_warehouse, count) == before
    # A forced reload of the year (its ledger row gone) replaces, never adds.
    writer = return_a_warehouse()
    try:
        with writer.cursor() as cursor:
            cursor.execute("DELETE FROM control.fbi_return_a_year")
        writer.commit()
    finally:
        writer.close()
    reloaded = load(return_a_warehouse, [2023], fetch=_fetch)
    assert [item["status"] for item in reloaded["years"]] == ["loaded"]
    assert _rows(return_a_warehouse, count) == before


def test_a_year_that_fails_is_recorded_and_the_run_moves_on(
    return_a_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-107 — a failed download or bad file costs only its own year."""

    def flaky(year: int) -> FetchedFile:
        if year == 2024:
            raise RuntimeError("signed URL answered 503 six times")
        if year == 2022:
            # A file that holds another year's records is refused whole.
            return dataclasses.replace(
                _fetch(2023), data_year=2022, object_key=object_key(2022)
            )
        return _fetch(year)

    summary = load(return_a_warehouse, [2024, 2023, 2022], fetch=flaky)
    assert {item["data_year"]: item["status"] for item in summary["years"]} == {
        2024: "fetch_failed",
        2023: "loaded",
        2022: "load_failed",
    }
    assert summary["failed_years"] == [2024, 2022]
    # The release is never published with a requested year missing.
    assert summary["published"] is False
    assert _rows(
        return_a_warehouse,
        "SELECT DISTINCT status FROM silver_fbi.dim_ucr_dataset_release "
        "WHERE release_key = %s",
        RELEASE,
    ) == [("replaying",)]
    assert _rows(
        return_a_warehouse,
        "SELECT data_year FROM control.fbi_return_a_year ORDER BY data_year",
    ) == [(2023,)]
    assert _rows(
        return_a_warehouse,
        "SELECT status FROM control.ingestion_request WHERE run_id = %s "
        "AND endpoint = 'master_files/reta/reta-2024.zip'",
        summary["run_id"],
    ) == [("failed",)]
    assert _rows(
        return_a_warehouse,
        "SELECT status FROM control.ingestion_run WHERE run_id = %s",
        summary["run_id"],
    ) == [("failed",)]
    # A rerun of what remains skips the loaded year and publishes.
    rerun = load(return_a_warehouse, [2023], fetch=_fetch)
    assert [item["status"] for item in rerun["years"]] == ["already_loaded"]
    assert rerun["published"] is True


def test_return_a_totals_are_checked_against_the_cde(
    return_a_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-107, DQ-FBI-011 — summed totals equal the CDE's own."""
    from data_ingestion_toolbox.quality.sources import fbi_return_a_matches_cde

    captured = fbi_release.persist_fixture_release(return_a_warehouse)
    fbi_release.run_pipeline(return_a_warehouse, captured)
    load(return_a_warehouse, [2023], fetch=_fetch)
    reader = return_a_warehouse()
    try:
        with reader.cursor() as cursor:
            outcomes = fbi_return_a_matches_cde(cursor, {})
    finally:
        reader.close()
    # The Sheriff's months agree exactly. The CDE fixture was captured on
    # 2026-08-27 and the FBI revised Fitchburg and UW-Green Bay since (the
    # live CDE now gives Fitchburg 13 in January, as this file does): the
    # rule names each revised month rather than letting it pass.
    [outcome] = outcomes
    assert outcome.result == "fail"
    assert outcome.observed_count == 15
    assert not [line for line in outcome.evidence if "WI0130000" in line]
    assert "return_a_violent_crime WI0137000 01-2023 13 <> 7" in outcome.evidence


def test_the_derived_route_serves_the_county_rate_from_the_warehouse(
    return_a_warehouse: Callable[[], connection],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-181 — the served page is the gold view's, coverage included."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from apps.api.services.derived_ratio_service import list_derived_observations
    from data_ingestion_toolbox.semantics.derived_ratios import SERVE_DRAFTS_ENV

    monkeypatch.setenv(SERVE_DRAFTS_ENV, "1")
    load(return_a_warehouse, [2023], fetch=_fetch)
    engine = create_engine("postgresql+psycopg2://", creator=return_a_warehouse)
    try:
        with Session(engine) as db:
            newest = list_derived_observations(
                db,
                ratio_id="violent_crime_county_agency_rate",
                scope="latest",
                release=None,
                filters={"geo_id": DANE},
                limit=1,
                offset=0,
                newest_per_geography=True,
            )
            months = list_derived_observations(
                db,
                ratio_id="violent_crime_county_agency_rate",
                scope="latest",
                release=None,
                filters={"state_fips": "55", "year_from": 2023, "year_to": 2023},
                limit=100,
                offset=0,
                period_start="2023-01-01",
            )
    finally:
        engine.dispose()

    assert newest.total == 1
    december = newest.items[0]
    # 105 offenses over 558,909 people covered, 98 % of the county's agencies.
    assert (december.geo_name, december.period_start, december.value) == (
        "Dane County",
        "2023-12-01",
        "18.79",
    )
    assert "WI0130100" in december.coverage.contributing_agencies
    assert december.coverage.not_loaded_agencies == []
    by_county = {item.geo_name: item for item in months.items}
    assert {"Brown County", "Dane County", "Richland County"} <= set(by_county)
    dane = by_county["Dane County"]
    assert (dane.value, dane.numerator.value) == ("19.46", "108")
    assert (dane.coverage.covered_population, dane.coverage.linked_population) == (
        "555061",
        "569830",
    )
    assert dane.coverage.coverage_share == "0.9741"
