"""Real PostgreSQL USDA NASS capture-to-gold deployment contract."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.capture import (
    CaptureControl,
    ResponseCapture,
    persist_response_capture,
)
from data_ingestion_toolbox.usda_nass.capture import (
    SLICE_CAPTURED,
    SLICE_OVER_LIMIT,
    CapturedNassRelease,
    CapturedNassSlice,
    persist_release_state,
)
from data_ingestion_toolbox.usda_nass.client import (
    API_COUNT_PATH,
    API_DATA_PATH,
    count_parameters,
    data_parameters,
)
from data_ingestion_toolbox.usda_nass.gold_nass.publisher import (
    NassPublicationError,
    publish_release,
)
from data_ingestion_toolbox.usda_nass.metadata import (
    NassSliceCount,
    ReleaseDecision,
    summarize_release,
)
from data_ingestion_toolbox.usda_nass.registry import (
    NassProduct,
    NassSlice,
    enabled_products,
    get_product,
)
from data_ingestion_toolbox.usda_nass.silver_nass.transform import (
    NassReconciliationError,
    persist_replay_result,
    replay_captured_run,
    transform_release,
)
from data_ingestion_toolbox.usda_nass.silver_nass.values import NassReplayError
from tests.support import usda_nass as nass_support

pytestmark = [pytest.mark.integration, pytest.mark.database]

FIXTURE_DIR = nass_support.FIXTURE_DIR


def _fixture(product_id: str) -> dict[str, Any]:
    return nass_support.load_product_fixture(product_id)


def _capture_slice(
    connection_factory: Callable[[], connection],
    control: CaptureControl,
    *,
    run_id: UUID,
    product: NassProduct,
    item: NassSlice,
    endpoint: str,
    parameters: dict[str, Any],
    payload: bytes,
) -> UUID:
    request = control.start_request(
        run_id=run_id, endpoint=endpoint, parameters=parameters
    )
    capture = ResponseCapture(
        capture_id=uuid4(),
        request_id=request.request_id,
        run_id=run_id,
        source_code="USDA_NASS",
        endpoint=endpoint,
        request_parameters=parameters,
        retrieved_at=datetime.now(timezone.utc),
        http_status=200,
        response_headers={"content-type": "application/json"},
        media_type="application/json",
        payload=payload,
        payload_schema_version=product.parser_contract_version,
    )
    persist_response_capture(connection_factory, capture)
    control.finish_request(request.request_id, status="captured")
    return capture.capture_id


def _persist_fixture_release(
    connection_factory: Callable[[], connection],
    *,
    product: NassProduct,
    document: dict[str, Any],
    year: int | None = None,
    over_limit_levels: tuple[str, ...] = (),
) -> CapturedNassRelease:
    """Capture one reviewed sample exactly as the production capture would."""
    control = CaptureControl(connection_factory, source_code="USDA_NASS")
    run_id = control.start_run(
        watermark={"product_id": product.product_id, "slice_mode": "recent"}
    )
    sample_year = year or int(document["sample_year"])
    counts: list[NassSliceCount] = []
    slices: list[CapturedNassSlice] = []
    payloads: list[bytes] = []

    for level, envelope in document["slices"].items():
        item = NassSlice(product.product_id, level, sample_year)
        rows = envelope["data"]["data"]
        provider_count = 10**6 if level in over_limit_levels else len(rows)
        count_payload = json.dumps({"count": str(provider_count)}).encode("utf-8")
        count_capture_id = _capture_slice(
            connection_factory,
            control,
            run_id=run_id,
            product=product,
            item=item,
            endpoint=API_COUNT_PATH,
            parameters=count_parameters(product, item),
            payload=count_payload,
        )
        counts.append(
            NassSliceCount(item.slice_key, level, sample_year, provider_count)
        )
        if level in over_limit_levels:
            slices.append(
                CapturedNassSlice(
                    item.slice_key,
                    level,
                    sample_year,
                    provider_count,
                    0,
                    count_capture_id,
                    None,
                    SLICE_OVER_LIMIT,
                )
            )
            continue
        data_payload = json.dumps(envelope["data"]).encode("utf-8")
        payloads.append(data_payload)
        data_capture_id = _capture_slice(
            connection_factory,
            control,
            run_id=run_id,
            product=product,
            item=item,
            endpoint=API_DATA_PATH,
            parameters=data_parameters(product, item),
            payload=data_payload,
        )
        slices.append(
            CapturedNassSlice(
                item.slice_key,
                level,
                sample_year,
                provider_count,
                len(rows),
                count_capture_id,
                data_capture_id,
                SLICE_CAPTURED,
            )
        )

    contract = summarize_release(product, payloads=payloads, slice_counts=counts)
    complete = not over_limit_levels
    decision = (
        ReleaseDecision.INGEST if complete else ReleaseDecision.OVER_LIMIT_QUARANTINE
    )
    control.set_run_watermark(
        run_id,
        watermark={
            "product_id": product.product_id,
            "extraction_watermark": contract.extraction_watermark,
        },
    )
    control.finish_run(run_id, status="success" if complete else "partial")
    release = CapturedNassRelease(
        run_id,
        product.product_id,
        "recent",
        tuple(slices),
        contract,
        decision,
        contract.total_row_count,
        complete,
    )
    persist_release_state(connection_factory, release)
    return release


@pytest.fixture
def nass_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the geographies the reviewed fixtures resolve against."""
    return nass_support.reviewed_warehouse(postgres_connection_factory, request)


def _run_to_gold(
    connection_factory: Callable[[], connection],
    product: NassProduct,
    document: dict[str, Any],
    *,
    year: int | None = None,
) -> tuple[CapturedNassRelease, int, int]:
    release = _persist_fixture_release(
        connection_factory, product=product, document=document, year=year
    )
    watermark = release.contract.extraction_watermark
    result = replay_captured_run(
        connection_factory,
        run_id=release.run_id,
        product=product,
        release_watermark=watermark,
    )
    persist_replay_result(
        connection_factory,
        run_id=release.run_id,
        product=product,
        release_watermark=watermark,
        result=result,
    )
    transformed = transform_release(
        connection_factory,
        run_id=release.run_id,
        product=product,
        release_watermark=watermark,
    )
    published = publish_release(
        connection_factory,
        run_id=release.run_id,
        product_id=product.product_id,
        release_watermark=watermark,
    )
    return release, transformed, published


def test_registered_products_reach_gold_with_exact_source_semantics(
    nass_warehouse: Callable[[], connection],
) -> None:
    """Covers: ARC-002, DB-003 — USDA NASS releases reach gold without loss."""
    for product in enabled_products():
        document = _fixture(product.product_id)
        _release, transformed, published = _run_to_gold(
            nass_warehouse, product, document
        )
        expected = sum(
            len(envelope["data"]["data"]) for envelope in document["slices"].values()
        )
        assert transformed == expected
        assert published == expected

    reader = nass_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT product_id, COUNT(*),
                       COUNT(*) FILTER (WHERE value_status = 'valid'),
                       COUNT(*) FILTER (WHERE value_status <> 'valid'),
                       COUNT(*) FILTER (WHERE geography_status = 'resolved')
                FROM gold_nass.crop_observation
                GROUP BY product_id
                ORDER BY product_id
                """
            )
            summary = {row[0]: row[1:] for row in cursor.fetchall()}
            assert set(summary) == {
                product.product_id for product in enabled_products()
            }
            for product_id, (total, numeric, non_numeric, resolved) in summary.items():
                assert total == numeric + non_numeric, product_id
                assert non_numeric > 0, product_id
                assert resolved == total, product_id

            # Suppression is never zero, and the exact source text survives.
            cursor.execute(
                """
                SELECT value_source, value, value_status, suppression_code
                FROM gold_nass.crop_observation
                WHERE suppression_code IS NOT NULL
                """
            )
            suppressed = cursor.fetchall()
            assert suppressed
            for value_source, value, status, code in suppressed:
                assert value is None
                assert value_source == code
                assert status != "valid"

            # Survey and Census values remain separate even where labels match.
            cursor.execute(
                """
                SELECT source_desc, COUNT(DISTINCT statistic_sk)
                FROM gold_nass.crop_observation
                WHERE short_desc = 'CORN, GRAIN - ACRES HARVESTED'
                GROUP BY source_desc
                ORDER BY source_desc
                """
            )
            assert cursor.fetchall() == [("CENSUS", 1), ("SURVEY", 1)]

            # Incompatible units never share one statistic identity.
            cursor.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT statistic_sk
                    FROM gold_nass.crop_observation
                    GROUP BY statistic_sk
                    HAVING COUNT(DISTINCT unit_desc) > 1
                ) AS ambiguous
                """
            )
            assert cursor.fetchone() == (0,)

            # Rate measures are explicitly non-additive; counts are undeclared.
            cursor.execute(
                """
                SELECT DISTINCT statisticcat_desc, additive_behavior,
                       additive_behavior_known
                FROM gold_nass.crop_observation
                ORDER BY statisticcat_desc
                """
            )
            behavior = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
            assert behavior["YIELD"] == ("non_additive", True)
            assert behavior["PRODUCTION"] == ("not_established", False)

            cursor.execute(
                "SELECT COUNT(*) FROM control.publisher_ready_event "
                "WHERE source_code = 'USDA_NASS'"
            )
            assert cursor.fetchone()[0] >= 1

            cursor.execute("SELECT COUNT(*) FROM gold_nass.crop_series")
            assert cursor.fetchone()[0] > 0
            cursor.execute("SELECT COUNT(*) FROM gold_nass.measure_export")
            assert cursor.fetchone()[0] > 0
            cursor.execute(
                "SELECT COUNT(*) FROM gold_nass.metric_publisher "
                "WHERE source_code = 'USDA_NASS'"
            )
            assert cursor.fetchone()[0] > 0
    finally:
        reader.close()


def test_reruns_are_idempotent_and_revisions_are_retained(
    nass_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-003 — reruns add no duplicates and never erase a revision."""
    product = get_product("corn_survey_annual")
    document = _fixture(product.product_id)
    first, _transformed, _published = _run_to_gold(nass_warehouse, product, document)

    # Replaying and conforming the same captured run again changes nothing.
    watermark = first.contract.extraction_watermark
    replayed = replay_captured_run(
        nass_warehouse,
        run_id=first.run_id,
        product=product,
        release_watermark=watermark,
    )
    persist_replay_result(
        nass_warehouse,
        run_id=first.run_id,
        product=product,
        release_watermark=watermark,
        result=replayed,
    )
    assert (
        transform_release(
            nass_warehouse,
            run_id=first.run_id,
            product=product,
            release_watermark=watermark,
        )
        == replayed.input_count
    )
    assert (
        publish_release(
            nass_warehouse,
            run_id=first.run_id,
            product_id=product.product_id,
            release_watermark=watermark,
        )
        == replayed.input_count
    )

    revised_document = _fixture("corn_survey_annual_revised")
    revised, _transformed, _published = _run_to_gold(
        nass_warehouse, product, revised_document
    )
    assert revised.contract.extraction_watermark > watermark

    reader = nass_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT release_watermark, COUNT(*)
                FROM gold_nass.crop_observation
                WHERE product_id = %s
                GROUP BY release_watermark
                ORDER BY release_watermark
                """,
                (product.product_id,),
            )
            by_release = cursor.fetchall()
            assert len(by_release) == 2
            assert by_release[0][1] == replayed.input_count

            # The newest release is what latest_release_observation exposes,
            # and the earlier release is still queryable as released.
            cursor.execute(
                """
                SELECT DISTINCT release_watermark
                FROM gold_nass.latest_release_observation
                WHERE product_id = %s
                """,
                (product.product_id,),
            )
            assert cursor.fetchall() == [(revised.contract.extraction_watermark,)]

            # A formerly withheld county value is published in the revision
            # while the original withheld record survives untouched.
            cursor.execute(
                """
                SELECT release_watermark, value_source, value, value_status
                FROM gold_nass.crop_observation
                WHERE product_id = %s
                  AND geo_id = 'state:48|county:301'
                  AND statisticcat_desc = 'PRODUCTION'
                ORDER BY release_watermark
                """,
                (product.product_id,),
            )
            history = cursor.fetchall()
            assert len(history) == 2
            assert history[0][1] == "(D)"
            assert history[0][2] is None
            assert history[0][3] == "withheld"
            assert history[1][3] == "valid"
            assert history[1][2] is not None
    finally:
        reader.close()


def test_an_over_limit_slice_cannot_replay_transform_or_publish(
    nass_warehouse: Callable[[], connection],
) -> None:
    """Covers: RES-002 — an over-limit partition never reaches publication."""
    product = get_product("hay_survey_annual")
    release = _persist_fixture_release(
        nass_warehouse,
        product=product,
        document=_fixture(product.product_id),
        over_limit_levels=("COUNTY",),
    )
    watermark = release.contract.extraction_watermark

    assert release.complete is False
    with pytest.raises(NassReplayError, match="unusable slices"):
        replay_captured_run(
            nass_warehouse,
            run_id=release.run_id,
            product=product,
            release_watermark=watermark,
        )
    with pytest.raises(NassReconciliationError, match="absent, quarantined"):
        transform_release(
            nass_warehouse,
            run_id=release.run_id,
            product=product,
            release_watermark=watermark,
        )
    with pytest.raises(NassPublicationError, match="not reconciled"):
        publish_release(
            nass_warehouse,
            run_id=release.run_id,
            product_id=product.product_id,
            release_watermark=watermark,
        )

    reader = nass_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT status, complete, decision
                FROM control.usda_nass_release WHERE run_id = %s
                """,
                (str(release.run_id),),
            )
            assert cursor.fetchone() == (
                "quarantined",
                False,
                "over_limit_quarantine",
            )
            cursor.execute(
                "SELECT COUNT(*) FROM gold_nass.crop_observation WHERE product_id = %s",
                (product.product_id,),
            )
            assert cursor.fetchone() == (0,)
    finally:
        reader.close()


def test_a_geography_miss_is_recorded_without_blocking_publication(
    nass_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-003 — an unmapped county is explicit, not silently dropped."""
    product = get_product("corn_survey_annual")
    document = _fixture(product.product_id)
    for row in document["slices"]["COUNTY"]["data"]["data"]:
        if row["county_ansi"] == "301":
            row["county_ansi"] = "999"
            row["county_code"] = "999"

    _release, transformed, published = _run_to_gold(nass_warehouse, product, document)
    assert published == transformed

    reader = nass_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT geography_status, COUNT(*)
                FROM gold_nass.crop_observation
                WHERE product_id = %s
                GROUP BY geography_status
                ORDER BY geography_status
                """,
                (product.product_id,),
            )
            statuses = dict(cursor.fetchall())
            assert statuses.get("unmapped", 0) > 0
            assert statuses.get("resolved", 0) > 0

            cursor.execute(
                """
                SELECT source_code, status, reason_code, geo_sk
                FROM silver_ref.geography_resolution
                WHERE provider_source = 'USDA_NASS' AND status = 'unmapped'
                """
            )
            unmapped = cursor.fetchall()
            assert unmapped
            for source_code, status, reason_code, geo_sk in unmapped:
                assert status == "unmapped"
                assert reason_code == "canonical_geography_absent"
                assert geo_sk is None
                assert source_code == "48999"
    finally:
        reader.close()


def test_a_release_with_quarantined_rows_still_reconciles_exactly(
    nass_warehouse: Callable[[], connection],
) -> None:
    """Covers: RES-002 — quarantined rows reconcile instead of vanishing."""
    product = get_product("wheat_survey_annual")
    document = _fixture(product.product_id)
    boundary = json.loads(
        (FIXTURE_DIR / "boundary_records.json").read_text(encoding="utf-8")
    )["records"]
    rejected = {**boundary["unregistered_statistic"], "commodity_desc": "WHEAT"}
    document["slices"]["COUNTY"]["data"]["data"].append(rejected)
    document["slices"]["COUNTY"]["count"]["count"] = str(
        len(document["slices"]["COUNTY"]["data"]["data"])
    )

    release, transformed, published = _run_to_gold(nass_warehouse, product, document)
    assert transformed == release.contract.total_row_count - 1
    assert published == transformed

    reader = nass_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT error_code, COUNT(*)
                FROM silver_nass.observation_quarantine
                WHERE run_id = %s
                GROUP BY error_code
                """,
                (str(release.run_id),),
            )
            assert cursor.fetchall() == [("unresolvable_identity", 1)]
            cursor.execute(
                """
                SELECT source_record_count, quarantine_count, status
                FROM silver_nass.dim_dataset_release
                WHERE product_id = %s
                """,
                (product.product_id,),
            )
            record_count, quarantine_count, status = cursor.fetchone()
            assert quarantine_count == 1
            assert record_count == transformed + quarantine_count
            assert status == "published"
    finally:
        reader.close()
