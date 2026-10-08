"""Real PostgreSQL Census SAIPE/SAHIE capture-to-gold contract.

Covers: ETL-054
"""

from __future__ import annotations

import json
from collections.abc import Callable

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_saipe_sahie.registry import SAHIE, SAIPE
from data_ingestion_toolbox.census_saipe_sahie.schema import (
    REQUIRED_RELATIONS,
    ensure_census_sae_schema,
)
from data_ingestion_toolbox.glossary.harvest import (
    Publisher,
    harvest_publisher,
    process_pending_events,
)
from tests.support import census_sae

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def sae_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return census_sae.reviewed_warehouse(postgres_connection_factory, request)


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


def test_both_datasets_replay_into_gold_with_their_bounds(
    sae_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-054 -- every fixture row reaches gold with its interval, at three grains."""
    for dataset in (SAIPE, SAHIE):
        _run_id, facts, published = census_sae.run_to_gold(sae_warehouse, dataset)
        expected = sum(
            len(census_sae.fixture_rows(dataset.dataset_id, level)) - 1
            for level in census_sae.GEO_LEVELS
        )
        assert facts == expected * len(dataset.measures)
        assert published == 3

    summary = _rows(
        sae_warehouse,
        """
        SELECT dataset_id, geo_level, COUNT(*),
               COUNT(*) FILTER (WHERE confidence_lower <= value AND value <= confidence_upper),
               COUNT(*) FILTER (WHERE geography_status = 'resolved')
        FROM gold_census_sae.estimate_latest
        GROUP BY dataset_id, geo_level ORDER BY dataset_id, geo_level
        """,
    )
    assert summary == [
        ("sahie", "COUNTY", 6, 6, 6),
        ("sahie", "NATIONAL", 2, 2, 2),
        ("sahie", "STATE", 102, 102, 2),
        ("saipe", "COUNTY", 15, 15, 15),
        ("saipe", "NATIONAL", 5, 5, 5),
        ("saipe", "STATE", 255, 255, 5),
    ]

    # The fixture's own numbers, unchanged: Kent County, Delaware.
    rows = census_sae.fixture_rows("saipe", "county")
    header = rows[0]
    kent = next(row for row in rows[1:] if row[header.index("county")] == "001")
    served = _rows(
        sae_warehouse,
        """
        SELECT value::TEXT, confidence_lower::TEXT, confidence_upper::TEXT, estimate_method
        FROM gold_census_sae.estimate_latest
        WHERE metric_key = 'saipe:SAEMHI' AND geo_id = 'state:10|county:001'
        """,
    )
    assert served == [
        (
            str(kent[header.index("SAEMHI_PT")]),
            str(kent[header.index("SAEMHI_LB90")]),
            str(kent[header.index("SAEMHI_UB90")]),
            "model-based annual estimate (SAIPE)",
        )
    ]

    resolution = _rows(
        sae_warehouse,
        """
        SELECT status, COUNT(*) FROM silver_ref.geography_resolution
        WHERE provider_source = 'CENSUS_SAIPE_SAHIE' GROUP BY status ORDER BY status
        """,
    )
    # One ledger row per (dataset, grain, code): 1 + 51 + 3 per dataset.
    assert resolution == [("resolved", 10), ("unmapped", 100)]


def test_publisher_states_the_model_based_basis_and_harvests(
    sae_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-054 -- one glossary row per measure, grains derived from served rows."""
    census_sae.run_to_gold(sae_warehouse, SAIPE)
    census_sae.run_to_gold(sae_warehouse, SAHIE)
    published = _rows(
        sae_warehouse,
        """
        SELECT source_object_key, valid_geo_grains, units, physical_lineage->>'relation', source_watermark
        FROM gold_census_sae.metric_publisher ORDER BY source_object_key
        """,
    )
    assert [row[0] for row in published] == [
        "sahie:NUI",
        "sahie:PCTUI",
        "saipe:SAEMHI",
        "saipe:SAEPOV0_17",
        "saipe:SAEPOVALL",
        "saipe:SAEPOVRT0_17",
        "saipe:SAEPOVRTALL",
    ]
    for _key, grains, _units, relation, watermark in published:
        assert grains == ["COUNTY", "NATIONAL", "STATE"]
        assert relation == "estimate_revision"
        assert watermark == "2023"

    assert harvest_publisher(sae_warehouse, Publisher("gold_census_sae")) > 0
    assert process_pending_events(sae_warehouse) >= 1
    harvested = _rows(
        sae_warehouse,
        """
        SELECT COUNT(*) FROM gold_glossary.dim_metric
        WHERE source_code = 'CENSUS_SAIPE_SAHIE'
          AND valid_geo_grains @> ARRAY['COUNTY', 'STATE', 'NATIONAL']
        """,
    )
    assert harvested == [(7,)]


def test_the_publish_event_finds_its_publisher_without_a_prior_harvest(
    sae_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-054 -- the event reaches the catalog though the schema does not spell the source.

    `gold_census_sae` publishes CENSUS_SAIPE_SAHIE, so a lookup by schema
    suffix found nothing and the event failed "publisher not found" on the
    first real load. The event harvests by the code the publisher's rows name.
    """
    census_sae.run_to_gold(sae_warehouse, SAIPE)
    assert process_pending_events(sae_warehouse) >= 1
    assert _rows(
        sae_warehouse,
        "SELECT COUNT(*) FROM gold_glossary.dim_metric WHERE source_code = 'CENSUS_SAIPE_SAHIE'",
    ) == [(5,)]


def test_rerun_is_idempotent_and_a_changed_response_keeps_both_checksums(
    sae_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-054 -- the same bytes add no estimate; a revision is a second row."""
    first_run, _facts, _published = census_sae.run_to_gold(sae_warehouse, SAIPE)
    second_run, _facts, _published = census_sae.run_to_gold(sae_warehouse, SAIPE)
    latest = _rows(
        sae_warehouse, "SELECT COUNT(*) FROM gold_census_sae.estimate_latest"
    )
    assert latest == [(275,)]

    rows = census_sae.fixture_rows("saipe", "county")
    header = rows[0]
    rows[1][header.index("SAEMHI_PT")] = 1
    revised = httpx.Response(200, content=json.dumps(rows).encode())
    third_run, _facts, _published = census_sae.run_to_gold(
        sae_warehouse,
        SAIPE,
        client=census_sae.FixtureClient(SAIPE, {"county": revised}),
    )
    geo_id = f"state:{rows[1][header.index('state')]}|county:{rows[1][header.index('county')]}"
    history = _rows(
        sae_warehouse,
        """
        SELECT revision.value::TEXT, capture.payload_checksum, revision.run_id::TEXT
        FROM gold_census_sae.estimate_revision AS revision
        JOIN raw_capture.response_capture AS capture ON capture.capture_id = revision.capture_id
        WHERE revision.metric_key = 'saipe:SAEMHI' AND revision.geo_id = %s
        ORDER BY revision.retrieved_at
        """,
        (geo_id,),
    )
    assert [row[2] for row in history] == [
        str(first_run),
        str(second_run),
        str(third_run),
    ]
    assert history[0][1] == history[1][1] != history[2][1]
    assert history[2][0] == "1"
    current = _rows(
        sae_warehouse,
        "SELECT value::TEXT FROM gold_census_sae.estimate_latest WHERE metric_key = 'saipe:SAEMHI' AND geo_id = %s",
        (geo_id,),
    )
    assert current == [("1",)]
    assert _rows(
        sae_warehouse, "SELECT COUNT(*) FROM gold_census_sae.estimate_latest"
    ) == [(275,)]


def test_a_malformed_slice_is_quarantined_and_never_served(
    sae_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-054 -- a rejected payload holds its slice back; a bad row is set aside."""
    rows = census_sae.fixture_rows("sahie", "county")
    header = rows[0]
    rows[2][header.index("IPRCAT")] = "3"
    override = httpx.Response(200, content=json.dumps(rows).encode())
    run_id, facts, published = census_sae.run_to_gold(
        sae_warehouse,
        SAHIE,
        client=census_sae.FixtureClient(SAHIE, {"county": override}),
    )
    assert published == 3
    assert facts == (1 + 51 + 2) * len(SAHIE.measures)
    quarantined = _rows(
        sae_warehouse,
        "SELECT source_row_index, error_code FROM silver_census_sae.observation_quarantine WHERE run_id = %s",
        (str(run_id),),
    )
    assert quarantined == [(2, "category_mismatch")]
    assert _rows(
        sae_warehouse,
        "SELECT COUNT(*) FROM gold_census_sae.estimate_latest WHERE dataset_id = 'sahie' AND geo_level = 'COUNTY'",
    ) == [(4,)]


def test_unpublished_grain_is_recorded_empty_and_schema_reapplies(
    sae_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-054 -- a 204 grain is an empty slice; the DDL is rerunnable."""
    run_id, facts, published = census_sae.run_to_gold(
        sae_warehouse,
        SAIPE,
        client=census_sae.FixtureClient(SAIPE, {"county": httpx.Response(204)}),
    )
    assert facts == (1 + 51) * len(SAIPE.measures)
    assert published == 2
    assert _rows(
        sae_warehouse,
        "SELECT geo_level, status FROM control.census_sae_slice WHERE run_id = %s ORDER BY geo_level",
        (str(run_id),),
    ) == [("county", "empty"), ("state", "published"), ("us", "published")]

    class _Hook:
        def get_conn(self) -> connection:
            return sae_warehouse()

    ensure_census_sae_schema(_Hook())
    present = _rows(
        sae_warehouse,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    )
    assert present == [(len(REQUIRED_RELATIONS),)]
