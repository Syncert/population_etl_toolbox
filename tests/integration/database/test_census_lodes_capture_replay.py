"""Real PostgreSQL LEHD LODES capture-to-gold contract.

Covers: ETL-063
"""

from __future__ import annotations

import gzip
import hashlib
from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_lodes.client import LodesIntegrityError
from data_ingestion_toolbox.census_lodes.schema import (
    REQUIRED_RELATIONS,
    ensure_census_lodes_schema,
)
from data_ingestion_toolbox.glossary.harvest import (
    Publisher,
    harvest_publisher,
    process_pending_events,
)
from data_ingestion_toolbox.quality.sources import (
    lodes_od_workplace_agreement,
    lodes_slice_reconciliation,
)
from tests.support import census_lodes as lodes

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def lodes_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return lodes.reviewed_warehouse(postgres_connection_factory, request)


def _rows(
    factory: Callable[[], connection], sql: str, parameters: tuple | None = None
) -> list[tuple]:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


def _hand_sum(name: str, column: str, county: str) -> int:
    lines = gzip.decompress(lodes.fixture_bytes(f"{name}.gz")).decode().splitlines()
    index = lines[0].split(",").index(column)
    return sum(int(line.split(",")[index]) for line in lines[1:] if line[:5] == county)


def test_a_state_year_reaches_gold_as_county_and_state_sums(lodes_warehouse) -> None:
    """Covers: ETL-063 — county totals equal hand-summed blocks; commuting splits add up."""
    factory = lodes_warehouse
    _run, status, rows, published = lodes.run_to_gold(factory)
    assert status == "captured" and rows > 0 and published == 1
    served = dict(
        _rows(
            factory,
            """
            SELECT metric_key, value::BIGINT FROM gold_census_lodes.observation_latest
            WHERE geo_id = %s AND year = 2023
            """,
            (lodes.KENT,),
        )
    )
    assert served["resident_workers"] == _hand_sum(
        "de_rac_S000_JT00_2023.csv", "C000", "10001"
    )
    assert served["jobs"] == _hand_sum("de_wac_S000_JT00_2023.csv", "C000", "10001")
    assert served["live_and_work"] + served["inbound"] == served["jobs"]
    release = _rows(
        factory,
        "SELECT DISTINCT release_key, geo_level FROM gold_census_lodes.observation_latest ORDER BY 2",
    )
    assert release == [("20251202_1657", "COUNTY"), ("20251202_1657", "STATE")]
    state_jobs = _rows(
        factory,
        "SELECT value::BIGINT FROM gold_census_lodes.observation_latest WHERE geo_id = 'state:10' AND metric_key = 'jobs'",
    )
    assert state_jobs == [
        (
            sum(
                _hand_sum("de_wac_S000_JT00_2023.csv", "C000", county)
                for county in ("10001", "10005")
            ),
        )
    ]
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_census_lodes.observation_latest WHERE geo_id = 'state:10' AND metric_key = 'outbound_in_state'",
    ) == [(0,)]


def test_columns_not_published_reach_silver_as_not_available(lodes_warehouse) -> None:
    """Covers: ETL-063 — 2008's demographics and JT00's firm columns are not zeros."""
    factory = lodes_warehouse
    lodes.run_to_gold(factory, "de", 2023)
    lodes.run_to_gold(factory, "de", 2008)
    statuses = dict(
        _rows(
            factory,
            """
            SELECT area.column_code || ':' || slice.year, area.value_status || ':' || COALESCE(area.value::TEXT, 'null')
            FROM silver_census_lodes.fact_area AS area
            JOIN control.census_lodes_slice AS slice USING (run_id)
            WHERE area.geo_id = %s AND area.family = 'wac' AND area.column_code IN ('CR01', 'CFA01', 'CNS18')
            """,
            (lodes.KENT,),
        )
    )
    assert statuses["CR01:2008"] == "not_available:null"
    assert statuses["CFA01:2023"] == "not_available:null"
    assert statuses["CR01:2023"].startswith("valid:")
    assert statuses["CNS18:2008"].startswith("valid:")
    # 2008 has no origin-destination fixture: the state published none here,
    # so the year serves jobs and residents and no commuting row, not zeros.
    measures = {
        row[0]
        for row in _rows(
            factory,
            "SELECT metric_key FROM gold_census_lodes.observation_latest WHERE year = 2008",
        )
    }
    assert measures == {"resident_workers", "jobs"}


def test_a_state_year_without_workplace_files_serves_no_workplace_row(
    lodes_warehouse,
) -> None:
    """Covers: ETL-063 — a state that published no WAC or OD file (Alaska from 2017) has no jobs row, not zero."""
    factory = lodes_warehouse
    listed = [
        line
        for line in lodes.fixture_bytes("lodes_de.sha256sum").decode().splitlines()
        if "_rac_" in line or not line.endswith("_2023.csv")
    ]
    _run, status, _rows_written, published = lodes.run_to_gold(
        factory,
        client=lodes.FixtureClient(
            {"lodes_de.sha256sum": ("\n".join(listed) + "\n").encode()}
        ),
    )
    assert (status, published) == ("captured", 1)
    assert sorted(
        _rows(
            factory,
            "SELECT family, status FROM control.census_lodes_file ORDER BY family",
        )
    ) == [
        ("od_aux", "not_published"),
        ("od_main", "not_published"),
        ("rac", "captured"),
        ("wac", "not_published"),
    ]
    measures = {
        row[0]
        for row in _rows(
            factory, "SELECT metric_key FROM gold_census_lodes.observation_latest"
        )
    }
    assert measures == {"resident_workers"}


def test_the_same_vintage_fetches_nothing_and_a_new_one_is_kept_beside_it(
    lodes_warehouse,
) -> None:
    """Covers: ETL-063 — an unchanged vintage writes nothing new; a new vintage keeps both checksums."""
    factory = lodes_warehouse
    first, _status, _rows_written, _published = lodes.run_to_gold(factory)
    client = lodes.FixtureClient()
    second, status, rows, published = lodes.run_to_gold(factory, client=client)
    assert (status, rows, published) == ("unchanged", 0, 0)
    assert client.calls == ["version.txt", "lodes_de.sha256sum"]
    revised_rac = gzip.decompress(
        lodes.fixture_bytes("de_rac_S000_JT00_2023.csv.gz")
    ).replace(b"\n100010401001000,45,", b"\n100010401001000,46,")
    listed = lodes.fixture_bytes("lodes_de.sha256sum").decode().splitlines()
    listed = [
        f"{hashlib.sha256(revised_rac).hexdigest()}  de_rac_S000_JT00_2023.csv"
        if line.endswith("de_rac_S000_JT00_2023.csv")
        else line
        for line in listed
    ]
    third, status, _rows_written, published = lodes.run_to_gold(
        factory,
        client=lodes.FixtureClient(
            {
                "version.txt": lodes.fixture_bytes("version.txt").replace(
                    b"20251202_1657", b"20261202_0900"
                ),
                "lodes_de.sha256sum": ("\n".join(listed) + "\n").encode(),
                "de_rac_S000_JT00_2023.csv.gz": gzip.compress(revised_rac),
            }
        ),
    )
    assert (status, published) == ("captured", 1)
    history = _rows(
        factory,
        """
        SELECT release_key, value::BIGINT FROM gold_census_lodes.observation_revision
        WHERE metric_key = 'resident_workers' AND geo_id = %s ORDER BY release_key
        """,
        (lodes.KENT,),
    )
    assert [row[0] for row in history] == ["20251202_1657", "20261202_0900"]
    assert history[1][1] == history[0][1] + 1
    assert _rows(
        factory,
        "SELECT release_key FROM gold_census_lodes.observation_latest WHERE metric_key = 'resident_workers' AND geo_id = %s",
        (lodes.KENT,),
    ) == [("20261202_0900",)]
    assert len({first, second, third}) == 3


def test_a_checksum_mismatch_fails_capture_and_the_rules_run(lodes_warehouse) -> None:
    """Covers: ETL-063 — a file that is not the listed one fails; DQ-LODES-002 and -004 pass then catch a loss."""
    factory = lodes_warehouse

    def outcome(executor):
        reader = factory()
        try:
            with reader.cursor() as cursor:
                return executor(cursor, {})[0]
        finally:
            reader.close()

    assert outcome(lodes_slice_reconciliation).result == "not_applicable"
    with pytest.raises(LodesIntegrityError, match="checksum_mismatch"):
        lodes.run_to_gold(
            factory,
            client=lodes.FixtureClient(
                {"de_rac_S000_JT00_2023.csv.gz": gzip.compress(b"h_geocode,C000\n")}
            ),
        )
    assert _rows(
        factory, "SELECT COUNT(*) FROM control.census_lodes_file WHERE family = 'rac'"
    ) == [(0,)]
    lodes.run_to_gold(factory)
    assert (
        outcome(lodes_slice_reconciliation).result,
        outcome(lodes_od_workplace_agreement).result,
    ) == ("pass", "pass")
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_census_lodes.fact_flow WHERE work_geo_id = %s AND part = 'aux'",
                (lodes.KENT,),
            )
            cursor.execute(
                "DELETE FROM silver_census_lodes.fact_area WHERE family = 'rac'"
            )
        writer.commit()
    finally:
        writer.close()
    assert (
        outcome(lodes_slice_reconciliation).result,
        outcome(lodes_slice_reconciliation).observed_count,
    ) == ("fail", 1)
    disagreement = outcome(lodes_od_workplace_agreement)
    assert (disagreement.result, disagreement.observed_count) == ("warn", 1)

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_census_lodes_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]


def test_the_harvest_names_each_measure(lodes_warehouse) -> None:
    """Covers: ETL-063 — five metrics, county and state grains, harvested into the glossary."""
    factory = lodes_warehouse
    lodes.run_to_gold(factory)
    published = dict(
        _rows(
            factory,
            "SELECT source_object_key, valid_geo_grains FROM gold_census_lodes.metric_publisher",
        )
    )
    assert set(published) == {
        "resident_workers",
        "jobs",
        "live_and_work",
        "inbound",
        "outbound_in_state",
    }
    assert published["jobs"] == ["COUNTY", "STATE"] and published[
        "outbound_in_state"
    ] == ["COUNTY"]
    assert harvest_publisher(factory, Publisher("gold_census_lodes")) > 0
    assert process_pending_events(factory) >= 1
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_glossary.dim_metric WHERE source_code = 'CENSUS_LODES'",
    ) == [(5,)]
