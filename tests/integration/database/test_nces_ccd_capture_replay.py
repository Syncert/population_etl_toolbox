"""Real PostgreSQL NCES Common Core of Data capture-to-gold contract.

Covers: ETL-070
"""

from __future__ import annotations

import csv
import io
import zipfile
from collections.abc import Callable
from decimal import Decimal
from unittest import mock

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import (
    Publisher,
    harvest_publisher,
    process_pending_events,
)
from data_ingestion_toolbox.nces_ccd.client import CcdPayloadError
from data_ingestion_toolbox.nces_ccd.registry import SchoolFile
from data_ingestion_toolbox.nces_ccd.schema import (
    REQUIRED_RELATIONS,
    ensure_nces_ccd_schema,
)
from data_ingestion_toolbox.quality.sources import (
    ccd_file_reconciliation,
    ccd_placement_and_plausibility,
)
from tests.support import nces_ccd as ccd

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def ccd_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return ccd.reviewed_warehouse(postgres_connection_factory, request)


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


def _outcome(factory: Callable[[], connection], executor):  # noqa: ANN001, ANN202
    reader = factory()
    try:
        with reader.cursor() as cursor:
            return executor(cursor, {})[0]
    finally:
        reader.close()


def _latest(factory: Callable[[], connection], metric: str, geo_id: str) -> tuple:
    rows = _rows(
        factory,
        """
        SELECT value, value_status, schools_with_value, schools_without_value, completeness
        FROM gold_nces_ccd.observation_latest WHERE metric_key = %s AND geo_id = %s AND year = 2024
        """,
        (metric, geo_id),
    )
    return rows[0] if rows else ()


def _rewritten(item: SchoolFile, replace: Callable[[bytes], bytes]) -> bytes:
    member = zipfile.ZipFile(io.BytesIO(ccd.fixture_bytes(item) or b"")).read(
        item.member
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(item.member, replace(member))
    return buffer.getvalue()


def test_county_and_state_figures_are_sums_of_placed_schools(ccd_warehouse) -> None:
    """Covers: ETL-070 — schools roll up through EDGE county codes, with completeness counts."""
    factory = ccd_warehouse
    published = [ccd.run_to_gold(factory, item) for item in ccd.REVIEWED]
    assert [(status, rows, done) for _run, status, rows, done in published] == [
        ("captured", 555, 1),
        ("captured", 555, 1),
        ("captured", 539, 1),
        ("captured", 550, 1),
        ("captured", 2168, 1),
    ]
    assert _latest(factory, "frpl_eligible", ccd.PROVIDENCE_RI) == (
        Decimal("56037"),
        "valid",
        197,
        3,
        "partial",
    )
    assert _latest(factory, "teacher_fte", ccd.KENT_DE) == (
        Decimal("1867.64"),
        "valid",
        54,
        0,
        "complete",
    )
    assert _latest(factory, "student_membership", ccd.PROVIDENCE_RI) == (
        Decimal("87970"),
        "valid",
        197,
        0,
        "complete",
    )
    assert _latest(factory, "operating_schools", ccd.NEW_CASTLE_DE)[0] == Decimal("129")
    assert _latest(factory, "charter_schools", ccd.PROVIDENCE_RI)[0] == Decimal("38")
    # Delaware reports direct certification, not FRPL: the FRPL rows have no
    # value and are never zero, and direct certification is not substituted.
    assert _latest(factory, "frpl_eligible", ccd.SUSSEX_DE) == (
        None,
        "missing",
        0,
        49,
        "partial",
    )
    assert _latest(factory, "direct_certification", ccd.SUSSEX_DE)[0] == Decimal("8266")
    state = _latest(factory, "teacher_fte", ccd.DELAWARE)
    assert state[0] == Decimal("1867.64") + Decimal("5553.81") + Decimal("2201.20")
    # The BIE school (operating code 59) sits in an unseeded North Dakota
    # county: kept in silver, never resolved as state 59, never served.
    assert _rows(
        factory,
        "SELECT operating_state_fips, state_fips, geography_status FROM silver_nces_ccd.school_location WHERE ncessch = %s",
        (ccd.BIE_SCHOOL,),
    ) == [("59", "38", "unmapped")]
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_nces_ccd.observation_latest WHERE geo_id LIKE 'state:59%%' OR geo_id LIKE 'state:38%%'",
    ) == [(0,)]


def test_withheld_counts_are_never_zero_and_bad_rows_are_quarantined(
    ccd_warehouse,
) -> None:
    """Covers: ETL-070 — Not reported, Missing and Suppressed keep no number; a bad row is refused alone."""
    factory = ccd_warehouse
    ccd.run_to_gold(factory, ccd.GEOCODE)
    run_id, _status, _rows_written, _published = ccd.run_to_gold(factory, ccd.STAFF)
    assert _rows(
        factory,
        """
        SELECT value_status, missing_reason, COUNT(*), COUNT(value) FROM silver_nces_ccd.school_count
        WHERE run_id = %s AND value_status <> 'valid' GROUP BY 1, 2 ORDER BY 1, 2
        """,
        (str(run_id),),
    ) == [
        ("missing", "missing", 4, 0),
        ("missing", "not_reported", 2, 0),
        ("suppressed", "suppressed", 1, 0),
    ]

    def damage(member: bytes) -> bytes:
        lines = member.split(b"\n")
        lines[1] = lines[1].replace(b",Reported", b",Approximated", 1)
        lines[2] = lines[2].replace(b"2024-2025", b"2023-2024", 1)
        return b"\n".join(lines)

    broken = _rewritten(ccd.STAFF, damage)
    run_id, status, written, _published = ccd.run_to_gold(
        factory, ccd.STAFF, client=ccd.FixtureClient({ccd.STAFF.stem: broken})
    )
    assert (status, written) == ("captured", 548)
    assert _rows(
        factory,
        "SELECT error_code FROM silver_nces_ccd.quarantine WHERE run_id = %s ORDER BY error_code",
        (str(run_id),),
    ) == [("unknown_flag",), ("wrong_school_year",)]


def test_an_unchanged_read_replays_nothing_and_a_later_release_supersedes(
    ccd_warehouse,
) -> None:
    """Covers: ETL-070 — same bytes add nothing; a 1b release beside 1a keeps both and serves 1b."""
    factory = ccd_warehouse
    ccd.run_to_gold(factory, ccd.GEOCODE)
    ccd.run_to_gold(factory, ccd.STAFF)
    _run, status, written, published = ccd.run_to_gold(factory, ccd.STAFF)
    assert (status, written, published) == ("unchanged", 0, 0)
    release_1b = SchoolFile(
        ccd.STAFF.component, 2024, ccd.STAFF.stem.replace("_1a_", "_1b_"), "1b"
    )
    member = zipfile.ZipFile(io.BytesIO(ccd.fixture_bytes(ccd.STAFF) or b"")).read(
        ccd.STAFF.member
    )
    header, *rows = list(csv.reader(io.StringIO(member.decode("latin-1"))))
    first = next(row for row in rows if row[header.index("NCESSCH")].startswith("10"))
    first[header.index("TEACHERS")] = str(Decimal(first[header.index("TEACHERS")]) + 1)
    rewritten = io.StringIO()
    csv.writer(rewritten, lineterminator="\n").writerows([header, *rows])
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(release_1b.member, rewritten.getvalue().encode("latin-1"))
    with mock.patch(
        "data_ingestion_toolbox.nces_ccd.silver_nces_ccd.load.FILES",
        (*ccd.registered_files(), release_1b),
    ):
        _run, status, _written, published = ccd.run_to_gold(
            factory,
            release_1b,
            client=ccd.FixtureClient({release_1b.stem: buffer.getvalue()}),
        )
    assert (status, published) == ("captured", 1)
    assert _rows(
        factory,
        "SELECT release_version FROM control.nces_ccd_file WHERE component = 'staff' ORDER BY version_rank, created_at",
    ) == [("1a",), ("1a",), ("1b",)]
    served = _rows(
        factory,
        "SELECT DISTINCT ccd_file FROM gold_nces_ccd.observation_latest WHERE metric_key = 'teacher_fte'",
    )
    assert served == [(release_1b.stem,)]
    assert _rows(
        factory,
        "SELECT COUNT(DISTINCT release_key) FROM gold_nces_ccd.observation_revision WHERE metric_key = 'teacher_fte' AND geo_id = %s",
        (ccd.DELAWARE,),
    ) == [(2,)]


def test_a_refused_file_and_the_rules(ccd_warehouse) -> None:
    """Covers: ETL-070 — a non-zip fails capture; DQ-NCES-002 and -004 behave, then catch a fault."""
    factory = ccd_warehouse
    assert _outcome(factory, ccd_file_reconciliation).result == "not_applicable"
    with pytest.raises(CcdPayloadError, match="member_missing"):
        ccd.run_to_gold(
            factory,
            ccd.STAFF,
            client=ccd.FixtureClient({ccd.STAFF.stem: b"<html>moved</html>"}),
        )
    with pytest.raises(CcdPayloadError, match="unexpected_header"):
        ccd.run_to_gold(
            factory,
            ccd.STAFF,
            client=ccd.FixtureClient(
                {ccd.STAFF.stem: _rewritten(ccd.STAFF, lambda _m: b"A,B\r\n1,2\r\n")}
            ),
        )
    assert _rows(factory, "SELECT COUNT(*) FROM control.nces_ccd_file") == [(0,)]
    ccd.run_all(factory)
    assert _outcome(factory, ccd_file_reconciliation).result == "pass"
    placement = _outcome(factory, ccd_placement_and_plausibility)
    # The BIE school's North Dakota county is not in the seeded geography.
    assert (placement.result, placement.observed_count) == ("warn", 1)
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                UPDATE silver_nces_ccd.school_count SET value = 1
                WHERE measure = 'student_membership' AND ncessch = (
                    SELECT ncessch FROM silver_nces_ccd.school_count
                    WHERE measure = 'frpl_eligible' AND value > 1 ORDER BY ncessch LIMIT 1
                )
                """
            )
        writer.commit()
    finally:
        writer.close()
    implausible = _outcome(factory, ccd_placement_and_plausibility)
    assert (implausible.result, implausible.observed_count) == ("warn", 2)
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM silver_nces_ccd.school_count
                WHERE run_id = (SELECT run_id FROM control.nces_ccd_file WHERE component = 'staff')
                """
            )
        writer.commit()
    finally:
        writer.close()
    lost = _outcome(factory, ccd_file_reconciliation)
    assert (lost.result, lost.observed_count) == ("fail", 1)

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_nces_ccd_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]


def test_the_harvest_names_every_published_measure(ccd_warehouse) -> None:
    """Covers: ETL-070 — FRPL and direct certification are distinct metrics, each a derived summary."""
    factory = ccd_warehouse
    ccd.run_all(factory)
    published = dict(
        _rows(
            factory,
            "SELECT source_object_key, measure_kind FROM gold_nces_ccd.metric_publisher",
        )
    )
    assert set(published) == {
        "student_membership",
        "operating_schools",
        "charter_schools",
        "teacher_fte",
        "frpl_eligible",
        "free_lunch_eligible",
        "reduced_price_lunch_eligible",
        "direct_certification",
    }
    assert set(published.values()) == {"derived_summary"}
    basis = dict(
        _rows(
            factory,
            "SELECT measure, observation_basis FROM gold_nces_ccd.measure_definition",
        )
    )
    assert "Community Eligibility Provision" in basis["frpl_eligible"]
    assert "never a substitute" in basis["direct_certification"]
    assert harvest_publisher(factory, Publisher("gold_nces_ccd")) > 0
    assert process_pending_events(factory) >= 1
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_glossary.dim_metric WHERE source_code = 'NCES_CCD'",
    ) == [(8,)]
