"""Real PostgreSQL FBI UCR capture-to-gold deployment contract."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.fbi_ucr.gold_fbi.publisher import (
    FbiPublicationError,
    publish_release,
)
from data_ingestion_toolbox.fbi_ucr.silver_fbi.replay import (
    FbiReplayError,
    load_captured_slices,
    replay_captured_run,
    replay_slices,
)
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import fbi_release
from tests.support.capture_seed import delete_geography, seed_geography
from data_ingestion_toolbox.fbi_ucr.metadata import load_latest_accepted_release
from data_ingestion_toolbox.fbi_ucr.registry import ALL_PRODUCTS
from data_ingestion_toolbox.fbi_ucr.registry import (
    SUMMARIZED_VIOLENT_CRIME as PRODUCT_V,
)
from tests.support.fbi_release import (
    OBSERVATIONS_PER_SUBJECT,
    PERIODS,
    PRODUCT,
    agency_directory_endpoint,
)

pytestmark = [pytest.mark.integration, pytest.mark.database]

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "fbi_ucr"

_slice_fixtures = fbi_release.slice_fixtures
_persist_fixture_release = fbi_release.persist_fixture_release
_run_pipeline = fbi_release.run_pipeline


@pytest.fixture
def fbi_warehouse(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[Callable[[], connection]]:
    """Seed the reviewed geographies and remove all FBI state afterwards."""
    yield from fbi_release.reviewed_warehouse(postgres_connection_factory)


def test_fbi_release_replays_reconciles_and_publishes_idempotently(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: ARC-002, DB-003, DB-006 — a full release reaches gold twice."""
    captured = _persist_fixture_release(fbi_warehouse)

    first = _run_pipeline(fbi_warehouse, captured)
    second = _run_pipeline(fbi_warehouse, captured)

    subjects = len(PRODUCT.subjects)
    assert (
        first
        == second
        == (
            subjects * OBSERVATIONS_PER_SUBJECT,
            subjects * OBSERVATIONS_PER_SUBJECT,
        )
    )

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT subject_type, COUNT(*)
                FROM gold_fbi.crime_observation
                GROUP BY subject_type ORDER BY subject_type
                """
            )
            assert cursor.fetchall() == [
                ("agency", len(PRODUCT.agency_scope) * OBSERVATIONS_PER_SUBJECT),
                ("national", OBSERVATIONS_PER_SUBJECT),
                ("state", OBSERVATIONS_PER_SUBJECT),
            ]
            cursor.execute("SELECT COUNT(*) FROM gold_fbi.reporting_coverage")
            assert cursor.fetchone() == (subjects * PERIODS,)
            cursor.execute(
                "SELECT COUNT(*) FROM control.publisher_ready_event "
                "WHERE source_code = 'FBI_UCR'"
            )
            assert cursor.fetchone()[0] >= 1
    finally:
        reader.close()


def test_provider_totals_and_agency_grain_stay_separable(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-012 — provider totals are never mixed with agency reports."""
    captured = _persist_fixture_release(fbi_warehouse)
    _run_pipeline(fbi_warehouse, captured)

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT subject_type, geography_status, geography_basis,
                       COUNT(DISTINCT subject_code)
                FROM gold_fbi.crime_observation
                GROUP BY 1, 2, 3 ORDER BY 1, 2
                """
            )
            rows = cursor.fetchall()
            assert (
                "national",
                "provider_geo_exact",
                "provider-published national total",
                1,
            ) in rows
            assert (
                "state",
                "provider_geo_exact",
                "provider-published state total",
                1,
            ) in rows
            assert not [row for row in rows if row[0] == "agency" and row[1] != row[1]]
            cursor.execute(
                """
                SELECT DISTINCT geography_basis FROM gold_fbi.crime_observation
                WHERE subject_type = 'agency'
                """
            )
            assert cursor.fetchall() == [
                ("agency-reported for one law-enforcement agency",)
            ]
            cursor.execute(
                """
                SELECT measure_form, counted_entity_basis, unit, COUNT(*)
                FROM gold_fbi.crime_observation
                WHERE subject_type = 'national'
                GROUP BY 1, 2, 3 ORDER BY 1, 2
                """
            )
            assert cursor.fetchall() == [
                ("absolute_total", "clearance", "count", PERIODS),
                ("absolute_total", "offense", "count", PERIODS),
                ("rate", "clearance", "per_100000_population", PERIODS),
                ("rate", "offense", "per_100000_population", PERIODS),
            ]
    finally:
        reader.close()


def test_agency_geography_status_matches_its_reviewed_evidence(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-004 — bridges follow evidence, never an agency name."""
    captured = _persist_fixture_release(fbi_warehouse)
    _run_pipeline(fbi_warehouse, captured)

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT subject_code, geography_status
                FROM gold_fbi.crime_observation
                WHERE subject_type = 'agency' ORDER BY subject_code
                """
            )
            assert cursor.fetchall() == [
                ("WI0050700", "agency_county_bridged"),
                ("WI0130000", "agency_county_bridged"),
                ("WI0137000", "agency_place_bridged"),
                ("WI0400100", "agency_only"),
                ("WI0540300", "agency_place_bridged"),
                ("WIWSP0000", "agency_only"),
            ]
            cursor.execute(
                """
                SELECT relationship_type, source_label, geo_id,
                       resolution_method, resolution_status, confidence_class
                FROM gold_fbi.agency_geography
                WHERE ori = 'WI0540300' ORDER BY relationship_type, source_label
                """
            )
            assert cursor.fetchall() == [
                # A county established from the provider's label: exact and
                # uniqueness-checked, backed by no review, and published as
                # `derived` so it is distinguishable from the place below,
                # which a reviewed crosswalk establishes (ETL-050).
                (
                    "county",
                    "DANE",
                    "state:55|county:025",
                    "county_label_match",
                    "resolved",
                    "derived",
                ),
                (
                    "county",
                    "ROCK",
                    "state:55|county:105",
                    "county_label_match",
                    "resolved",
                    "derived",
                ),
                (
                    "place",
                    "Edgerton city",
                    "state:55|place:22575",
                    "reviewed_place_crosswalk",
                    "resolved",
                    "reviewed",
                ),
                (
                    "state",
                    "WI",
                    "state:55",
                    "exact_state_code",
                    "resolved",
                    "exact",
                ),
            ]
            cursor.execute(
                """
                SELECT COUNT(*) FROM gold_fbi.agency_geography
                WHERE ori IN ('WI0400100', 'WIWSP0000')
                  AND relationship_type IN ('county', 'place')
                """
            )
            assert cursor.fetchone() == (0,)
            cursor.execute(
                """
                SELECT COUNT(*) FROM silver_ref.bridge_geo_relationship_version
                AS bridge
                JOIN silver_ref.dim_geo_entity AS agency
                  ON agency.geo_sk = bridge.parent_geo_sk
                WHERE agency.geo_type = 'agency'
                """
            )
            assert cursor.fetchone()[0] > 0
    finally:
        reader.close()


def test_county_filter_keeps_agency_grain_and_deduplicates(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-012 — a county filter is never a county total."""
    captured = _persist_fixture_release(fbi_warehouse)
    _run_pipeline(fbi_warehouse, captured)

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT observation_grain, result_label
                FROM gold_fbi.agency_observation_area_filter
                WHERE filter_geography_type = 'county'
                """
            )
            assert cursor.fetchall() == [
                (
                    "agency",
                    "agency-reported for agencies associated with this county",
                )
            ]
            cursor.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT observation_sk)
                FROM gold_fbi.agency_observation_area_filter
                WHERE ori = 'WI0540300' AND filter_geography_type = 'county'
                """
            )
            rows, distinct_observations = cursor.fetchone()
            assert rows == 2 * OBSERVATIONS_PER_SUBJECT
            assert distinct_observations == OBSERVATIONS_PER_SUBJECT
            cursor.execute(
                """
                SELECT COUNT(DISTINCT observation_sk)
                FROM gold_fbi.agency_observation_area_filter
                WHERE filter_geo_id = 'state:55|county:025'
                  AND filter_geography_type = 'county'
                """
            )
            dane_observations = cursor.fetchone()[0]
            cursor.execute(
                """
                SELECT COUNT(*) FROM gold_fbi.crime_observation
                WHERE subject_code IN ('WI0130000', 'WI0137000', 'WI0540300')
                """
            )
            assert dane_observations == cursor.fetchone()[0]
    finally:
        reader.close()


def test_ambiguous_county_evidence_is_withheld_from_gold(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-011 — an ambiguous name match never becomes a relationship."""
    writer = fbi_warehouse()
    try:
        with writer.cursor() as cursor:
            seed_geography(
                cursor,
                geo_type="county",
                state_fips="55",
                county_fips="997",
                vintage=2023,
                name="Dane County",
            )
        writer.commit()
    finally:
        writer.close()

    try:
        captured = _persist_fixture_release(fbi_warehouse)
        _run_pipeline(fbi_warehouse, captured)

        reader = fbi_warehouse()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT resolution_status, geo_id, reason_code
                    FROM silver_fbi.agency_geography_relationship
                    WHERE ori = 'WI0130000' AND relationship_type = 'county'
                    """
                )
                assert cursor.fetchall() == [
                    ("ambiguous", None, "ambiguous_county_name")
                ]
                cursor.execute(
                    """
                    SELECT DISTINCT geography_status
                    FROM silver_fbi.fact_crime_observation
                    WHERE subject_code = 'WI0130000'
                    """
                )
                assert cursor.fetchall() == [("ambiguous",)]
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM gold_fbi.crime_observation
                    WHERE subject_code = 'WI0130000'
                    """
                )
                assert cursor.fetchone() == (0,)
        finally:
            reader.close()
    finally:
        remover = fbi_warehouse()
        try:
            with remover.cursor() as cursor:
                cursor.execute("DELETE FROM silver_fbi.fact_crime_observation")
                cursor.execute("DELETE FROM silver_fbi.agency_geography_relationship")
                delete_geography(cursor, "state:55|county:997")
            remover.commit()
        finally:
            remover.close()


def test_an_unresolved_county_label_is_not_read_as_an_unlabelled_agency(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-050 — a label that resolves nothing says so, not nothing.

    `agency_only` is a fact about the provider: it published no county
    association at all, which is what `NOT SPECIFIED` means in the Agency
    resource. An agency whose county label *was* published and failed to
    resolve read as `agency_only` too, so the one state an operator can act on
    -- a label this normalisation does not reach, a county the reference does
    not hold -- was indistinguishable from the one they cannot.

    Brown County is removed from the reference here because WI0050700 is the
    one fixture agency labelled with it alone, so exactly one agency's
    resolution changes and the rest of the release stays as the other nodes
    assert it.
    """
    writer = fbi_warehouse()
    try:
        with writer.cursor() as cursor:
            delete_geography(cursor, "state:55|county:009")
        writer.commit()
    finally:
        writer.close()

    try:
        captured = _persist_fixture_release(fbi_warehouse)
        _run_pipeline(fbi_warehouse, captured)

        reader = fbi_warehouse()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT resolution_status, geo_id, confidence_class, reason_code
                    FROM silver_fbi.agency_geography_relationship
                    WHERE ori = 'WI0050700' AND relationship_type = 'county'
                    """
                )
                assert cursor.fetchall() == [
                    ("unresolved", None, "unresolved", "county_label_unmatched")
                ]
                cursor.execute(
                    """
                    SELECT DISTINCT subject_code, geography_status
                    FROM gold_fbi.crime_observation
                    WHERE subject_code IN ('WI0050700', 'WIWSP0000')
                    ORDER BY subject_code
                    """
                )
                assert cursor.fetchall() == [
                    # The provider named a county for this one and it did not
                    # resolve; the label is still there as evidence.
                    ("WI0050700", "agency_county_unresolved"),
                    # The provider named none. Two different facts, two
                    # different statuses.
                    ("WIWSP0000", "agency_only"),
                ]
        finally:
            reader.close()
    finally:
        remover = fbi_warehouse()
        try:
            with remover.cursor() as cursor:
                cursor.execute("DELETE FROM silver_fbi.fact_crime_observation")
                cursor.execute("DELETE FROM silver_fbi.agency_geography_relationship")
                seed_geography(
                    cursor,
                    geo_type="county",
                    state_fips="55",
                    county_fips="009",
                    vintage=2023,
                    name="Brown County",
                )
            remover.commit()
        finally:
            remover.close()


def test_missing_reports_stay_distinct_from_reported_zeros(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-005 — no report and a reported zero are different rows."""
    captured = _persist_fixture_release(fbi_warehouse)
    _run_pipeline(fbi_warehouse, captured)

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT period, value_status, value, participation_status
                FROM gold_fbi.crime_observation
                WHERE subject_code = 'WI0400100'
                  AND measure_form = 'absolute_total'
                  AND counted_entity_basis = 'offense'
                  AND period IN ('01-2023', '03-2023')
                ORDER BY period
                """
            )
            assert cursor.fetchall() == [
                ("01-2023", "reported", 3, "full_participation"),
                ("03-2023", "not_reported", None, "no_participation"),
            ]
            cursor.execute(
                """
                SELECT COUNT(*) FROM gold_fbi.crime_observation
                WHERE subject_code = 'WI0050700' AND value = 0
                  AND value_status = 'reported'
                """
            )
            assert cursor.fetchone()[0] > 0
    finally:
        reader.close()


def test_every_published_observation_has_a_coverage_interpretation(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-004 — an observation cannot publish without its coverage."""
    captured = _persist_fixture_release(fbi_warehouse)
    _run_pipeline(fbi_warehouse, captured)

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) FROM silver_fbi.fact_crime_observation AS fact
                LEFT JOIN silver_fbi.fact_reporting_participation AS coverage
                  ON coverage.product_id = fact.product_id
                 AND coverage.release_key = fact.release_key
                 AND coverage.subject_type = fact.subject_type
                 AND coverage.subject_code = fact.subject_code
                 AND coverage.period = fact.period
                WHERE coverage.participation_sk IS NULL
                """
            )
            assert cursor.fetchone() == (0,)
            cursor.execute(
                """
                SELECT COUNT(*) FROM gold_fbi.crime_observation
                WHERE participation_status IS NULL OR coverage_basis IS NULL
                """
            )
            assert cursor.fetchone() == (0,)
    finally:
        reader.close()


def test_reference_dependency_failure_blocks_the_release(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-014 — a release without its reference slice cannot replay."""
    captured = _persist_fixture_release(
        fbi_warehouse, omit=(agency_directory_endpoint("WI"),)
    )

    with pytest.raises(FbiReplayError, match="missing required capture slices"):
        replay_captured_run(
            fbi_warehouse,
            run_id=captured.run_id,
            product=PRODUCT,
            release_key=captured.release_key,
        )

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT complete, status FROM control.fbi_ucr_release "
                "WHERE run_id = %s",
                (str(captured.run_id),),
            )
            assert cursor.fetchone() == (False, "quarantined")
            cursor.execute("SELECT COUNT(*) FROM gold_fbi.crime_observation")
            assert cursor.fetchone() == (0,)
    finally:
        reader.close()


def test_unreconciled_release_cannot_publish(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-014 — publication requires a reconciled silver release."""
    captured = _persist_fixture_release(fbi_warehouse)

    with pytest.raises(FbiPublicationError, match="not reconciled"):
        publish_release(
            fbi_warehouse,
            run_id=captured.run_id,
            product_id=PRODUCT.product_id,
            release_key=captured.release_key,
        )


def test_changed_revision_is_retained_and_latest_selection_projects_it(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-013, DB-022 — a revised release keeps both refresh dates."""
    first = _persist_fixture_release(fbi_warehouse)
    _run_pipeline(fbi_warehouse, first)
    revised = _persist_fixture_release(
        fbi_warehouse, national_fixture="summarized_national_V_revised"
    )
    _run_pipeline(fbi_warehouse, revised)

    assert revised.release_key != first.release_key

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT release_key, status FROM silver_fbi.dim_ucr_dataset_release
                ORDER BY release_key
                """
            )
            assert cursor.fetchall() == [
                (first.release_key, "published"),
                (revised.release_key, "published"),
            ]
            cursor.execute(
                """
                SELECT DISTINCT release_key FROM gold_fbi.latest_release_observation
                """
            )
            assert cursor.fetchall() == [(revised.release_key,)]
            cursor.execute(
                """
                SELECT release_key, value FROM gold_fbi.crime_observation
                WHERE subject_type = 'national' AND period = '01-2023'
                  AND measure_form = 'absolute_total'
                  AND counted_entity_basis = 'offense'
                ORDER BY release_key
                """
            )
            values = cursor.fetchall()
            assert [row[0] for row in values] == [
                first.release_key,
                revised.release_key,
            ]
            assert values[1][1] == values[0][1] + 25
    finally:
        reader.close()


def test_release_replays_from_stored_bytes_with_no_provider_access(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-040 — replay reads durable captures, never the provider."""
    captured = _persist_fixture_release(fbi_warehouse)

    slices = load_captured_slices(fbi_warehouse, run_id=captured.run_id)
    result = replay_slices(PRODUCT, slices, release_key=captured.release_key)

    assert set(slices) == set(_slice_fixtures())
    assert result.observations
    assert not result.quarantined


def test_publisher_keeps_one_row_per_measure_across_published_releases(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: ARC-001 — a second published release adds no publisher row.

    The publisher view once grouped by release key, so a second published
    release doubled every measure. The glossary harvest upserts on
    (source_code, source_object_key) and failed outright -- and because
    ``harvest_all_publishers`` isolates each publisher, the FBI catalog stopped
    following the warehouse without failing the DAG.
    """
    first = _persist_fixture_release(fbi_warehouse)
    _run_pipeline(fbi_warehouse, first)
    revised = _persist_fixture_release(
        fbi_warehouse, national_fixture="summarized_national_V_revised"
    )
    _run_pipeline(fbi_warehouse, revised)

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT source_object_key),
                       COUNT(DISTINCT source_watermark)
                FROM gold_fbi.metric_publisher
                """
            )
            total, distinct_keys, watermarks = cursor.fetchone()
            assert total == distinct_keys
            assert watermarks == 1
            cursor.execute(
                "SELECT DISTINCT source_watermark FROM gold_fbi.metric_publisher"
            )
            assert cursor.fetchall() == [(revised.release_key,)]
    finally:
        reader.close()

    assert harvest_publisher(fbi_warehouse, Publisher("gold_fbi")) == 4


def test_publisher_contract_exposes_measure_identity(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: ARC-001 — the publisher view owns no shared glossary objects."""
    captured = _persist_fixture_release(fbi_warehouse)
    _run_pipeline(fbi_warehouse, captured)

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT source_code, source_object_key, units,
                       aggregation_characteristic, valid_time_grains,
                       physical_lineage
                FROM gold_fbi.metric_publisher
                ORDER BY source_object_key
                """
            )
            rows = cursor.fetchall()
            assert {row[0] for row in rows} == {"FBI_UCR"}
            assert {row[1] for row in rows} == {
                f"{PRODUCT.product_id}:{PRODUCT.measure_id(basis, form)}"
                for basis in ("offense", "clearance")
                for form in ("absolute_total", "rate")
            }
            characteristics = {row[1]: row[3] for row in rows}
            assert (
                characteristics[f"{PRODUCT.product_id}:V:offense:absolute_total"]
                == "additive_within_subject"
            )
            assert characteristics[f"{PRODUCT.product_id}:V:offense:rate"] == (
                "non_additive"
            )
            assert all(row[4] == ["MONTHLY"] for row in rows)
            assert all(
                json.loads(json.dumps(row[5]))["schema"] == "gold_fbi" for row in rows
            )
    finally:
        reader.close()


def _counts(connection_factory: Callable[[], connection]) -> dict[str, list[tuple]]:
    reader = connection_factory()
    try:
        with reader.cursor() as cursor:
            counts: dict[str, list[tuple]] = {}
            for name, sql in {
                "releases": """
                    SELECT product_id, COUNT(*) FROM silver_fbi.dim_ucr_dataset_release
                    GROUP BY product_id ORDER BY product_id
                """,
                "facts": """
                    SELECT product_id, COUNT(*) FROM silver_fbi.fact_crime_observation
                    GROUP BY product_id ORDER BY product_id
                """,
                "revisions": """
                    SELECT product_id, COUNT(*) FROM silver_fbi.observation_revision
                    GROUP BY product_id ORDER BY product_id
                """,
                "measures": """
                    SELECT product_id, offense_code, array_agg(DISTINCT measure_id
                           ORDER BY measure_id)
                    FROM gold_fbi.crime_observation
                    GROUP BY product_id, offense_code ORDER BY product_id
                """,
            }.items():
                cursor.execute(sql)
                counts[name] = cursor.fetchall()
            return counts
    finally:
        reader.close()


def test_every_registered_offense_publishes_as_its_own_product(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-003, DB-006, ETL-052 — ten offenses publish once, idempotently."""
    products = [fbi_release.fixture_scoped(product) for product in ALL_PRODUCTS]
    captured = {
        product.product_id: _persist_fixture_release(fbi_warehouse, product=product)
        for product in products
    }
    for product in products:
        transformed, published = _run_pipeline(
            fbi_warehouse, captured[product.product_id], product
        )
        assert transformed > 0
        assert published == transformed
    first = _counts(fbi_warehouse)

    # A second replay of every stored release writes no new row anywhere.
    for product in products:
        _run_pipeline(fbi_warehouse, captured[product.product_id], product)
    assert _counts(fbi_warehouse) == first

    product_ids = sorted(product.product_id for product in ALL_PRODUCTS)
    assert first["releases"] == [(product_id, 1) for product_id in product_ids]
    assert [row[0] for row in first["facts"]] == product_ids
    assert sorted(
        (row[0], row[1], list(row[2])) for row in first["measures"]
    ) == sorted(
        (
            product.product_id,
            product.offense_code,
            sorted(
                product.measure_id(basis, form)
                for basis in ("offense", "clearance")
                for form in ("absolute_total", "rate")
            ),
        )
        for product in ALL_PRODUCTS
    )

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            # The provider's -1 rate sentinel is quarantined, never published
            # as a negative value or rewritten to zero.
            cursor.execute(
                "SELECT COUNT(*) FROM gold_fbi.crime_observation WHERE value < 0"
            )
            assert cursor.fetchone() == (0,)
            cursor.execute(
                """
                SELECT DISTINCT error_code FROM silver_fbi.slice_quarantine
                """
            )
            assert set(cursor.fetchall()) <= {("negative_measure_value",)}
            cursor.execute(
                """
                SELECT COUNT(DISTINCT source_dataset), COUNT(*)
                FROM gold_fbi.measure_export
                """
            )
            assert cursor.fetchone() == (len(ALL_PRODUCTS), 4 * len(ALL_PRODUCTS))
    finally:
        reader.close()

    assert harvest_publisher(fbi_warehouse, Publisher("gold_fbi")) == 4 * len(
        ALL_PRODUCTS
    )


def test_states_beyond_wisconsin_resolve_to_their_canonical_geography(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-052 — a state or territory resolves by its FIPS contract.

    Pennsylvania and the U.S. Virgin Islands carry no agency directory and no
    Wisconsin evidence; each resolves to its own canonical geography row, and
    the territory's months, none of which it reported, stay null.
    """
    product = fbi_release.fixture_scoped(PRODUCT_V, ("PA", "VI", "WI"))
    captured = _persist_fixture_release(fbi_warehouse, product=product)
    _run_pipeline(fbi_warehouse, captured, product)

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT subject_code, geo_id, geography_status,
                       COUNT(*) FILTER (WHERE value IS NOT NULL)
                FROM gold_fbi.crime_observation
                WHERE subject_type = 'state'
                GROUP BY 1, 2, 3 ORDER BY 1
                """
            )
            rows = cursor.fetchall()
    finally:
        reader.close()

    assert [row[:3] for row in rows] == [
        ("PA", "state:42", "provider_geo_exact"),
        ("VI", "state:78", "provider_geo_exact"),
        ("WI", "state:55", "provider_geo_exact"),
    ]
    reported = {row[0]: row[3] for row in rows}
    assert reported["PA"] > 0
    assert reported["VI"] == 0


def test_only_a_published_release_of_the_same_scope_is_the_previous_one(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-052 — a scope change or an unpublished capture re-ingests.

    The capture decides ``unchanged`` against the previous release. A release
    that was captured and never published is not one (its replay failed, and
    ``unchanged`` would leave it unpublished forever), and a release captured
    for a narrower subject scope is not one either (``unchanged`` would never
    capture the states the registry added).
    """
    narrow = fbi_release.fixture_scoped(PRODUCT_V, ("WI",))
    wide = fbi_release.fixture_scoped(PRODUCT_V, ("PA", "VI", "WI"))

    captured = _persist_fixture_release(fbi_warehouse, product=narrow)
    assert load_latest_accepted_release(fbi_warehouse, narrow) is None

    _run_pipeline(fbi_warehouse, captured, narrow)
    previous = load_latest_accepted_release(fbi_warehouse, narrow)
    assert previous is not None
    assert previous.release_key == captured.release_key
    assert load_latest_accepted_release(fbi_warehouse, wide) is None
