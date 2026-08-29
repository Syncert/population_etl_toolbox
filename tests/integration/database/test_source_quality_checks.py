"""Real PostgreSQL source-specific coverage and validity checks (DQ-004)."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.capture import (
    CaptureControl,
    ResponseCapture,
    persist_response_capture,
)
from data_ingestion_toolbox.quality.sources import (
    SOURCE_EXECUTORS,
    acs_slice_reconciliation,
    bls_chunk_reconciliation,
    cdc_watermark_monotonicity,
    fred_slice_reconciliation,
    nass_slice_ledger,
    pep_sentinel_conformance,
    publisher_registry_reconciliation,
    reference_resolution_accounting,
)

pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_an_empty_warehouse_is_valid_emptiness_not_failure(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-004 — no source data means not_applicable, never a false alarm."""
    with postgres_connection.cursor() as cursor:
        for statement in (
            "DELETE FROM silver_ref.geography_resolution",
            "DELETE FROM control.acs_ingestion_slices",
            "DELETE FROM control.bls_ingestion_slices",
            "DELETE FROM control.fred_ingestion_slices",
            "DELETE FROM gold_glossary.publisher_registry",
        ):
            cursor.execute(statement)
        for rule_id, executor in sorted(SOURCE_EXECUTORS.items()):
            for outcome in executor(cursor, {}):
                assert outcome.result in {"not_applicable", "pass"}, (
                    f"{rule_id} produced {outcome.result} on an empty warehouse"
                )
    postgres_connection.rollback()


def test_slice_ledger_defects_fail_with_bounded_evidence(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-004 — abandoned, failed, and silently-empty slices surface."""
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.acs_ingestion_slices
                (dataset, year, geo_level, status, rows_loaded, started_at)
            VALUES
                ('acs5', 2023, 'us', 'failed', 0, NOW()),
                ('acs5', 2023, 'state', 'planned', 0, NULL),
                ('acs5', 2022, 'us', 'success', 0, NOW()),
                ('acs5', 2022, 'state', 'success', 10, NOW()),
                ('acs1', 2023, 'us', 'empty', 0, NOW())
            """
        )
        [outcome] = acs_slice_reconciliation(cursor, {})
        assert outcome.result == "fail"
        assert outcome.observed_count == 3  # failed, planned, success-with-zero
        assert len(outcome.evidence) == 3

        cursor.execute(
            """
            INSERT INTO control.bls_ingestion_slices
                (program, year_start, year_end, status, rows_loaded, started_at)
            VALUES ('la', 2023, 2024, 'success', 25, NOW())
            """
        )
        [outcome] = bls_chunk_reconciliation(cursor, {})
        assert outcome.result == "pass"

        cursor.execute(
            """
            INSERT INTO control.fred_ingestion_slices
                (domain, date_start, date_end, status, rows_loaded, started_at)
            VALUES ('labor', '2024-01-01', '2024-12-31', 'running', 0, NOW())
            """
        )
        outcomes = fred_slice_reconciliation(cursor, {})
        ledger = outcomes[0]
        assert ledger.result == "fail"
        assert ledger.evidence == ["labor:2024-01-01|running"]
    postgres_connection.rollback()


def _seed_probe_capture(
    connection_factory: Callable[[], connection], source_code: str
) -> tuple[str, str]:
    control = CaptureControl(connection_factory, source_code=source_code)
    run_id = control.start_run(watermark={})
    parameters = {"seed": str(uuid4())}
    request = control.start_request(
        run_id=run_id, endpoint="/probe", parameters=parameters
    )
    capture = ResponseCapture(
        capture_id=uuid4(),
        request_id=request.request_id,
        run_id=run_id,
        source_code=source_code,
        endpoint="/probe",
        request_parameters=parameters,
        retrieved_at=datetime.now(timezone.utc),
        http_status=200,
        response_headers={"content-type": "application/json"},
        media_type="application/json",
        payload=json.dumps({"probe": str(uuid4())}).encode(),
        payload_schema_version="probe-v1",
    )
    persist_response_capture(connection_factory, capture)
    control.finish_request(request.request_id, status="captured")
    control.finish_run(run_id, status="success")
    return str(run_id), str(capture.capture_id)


def test_cdc_backward_watermark_ingest_fails(
    postgres_connection_factory: Callable[[], connection],
    postgres_connection: connection,
) -> None:
    """Covers: DQ-004 — a later ingest at a lower watermark is a regression."""
    source = f"CDCPROBE{uuid4().hex[:10].upper()}"
    run_one, capture_one = _seed_probe_capture(postgres_connection_factory, source)
    run_two, capture_two = _seed_probe_capture(postgres_connection_factory, source)

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.cdc_dataset_release (
                run_id, asset_id, socrata_id, title, release_watermark,
                schema_contract, metadata_capture_id, decision, status,
                captured_row_count, page_count, complete, published_at,
                created_at
            ) VALUES
                (%s, 'cdi', 'abcd-1234', 'probe', 200, '{}'::JSONB, %s,
                 'ingest', 'published', 3, 1, TRUE, NOW(),
                 NOW() - INTERVAL '1 hour'),
                (%s, 'cdi', 'abcd-1234', 'probe', 100, '{}'::JSONB, %s,
                 'ingest', 'captured', 3, 1, TRUE, NULL, NOW())
            """,
            (run_one, capture_one, run_two, capture_two),
        )
        [outcome] = cdc_watermark_monotonicity(cursor, {})
        assert outcome.result == "fail"
        assert outcome.evidence == ["cdi|100"]
    postgres_connection.rollback()


def test_nass_ledger_mismatch_and_advanced_partial_slice_fail(
    postgres_connection_factory: Callable[[], connection],
    postgres_connection: connection,
) -> None:
    """Covers: DQ-004 — preflight drift and a partial slice that advanced."""
    source = f"NASSPROBE{uuid4().hex[:10].upper()}"
    run_id, capture_id = _seed_probe_capture(postgres_connection_factory, source)

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.usda_nass_release (
                run_id, product_id, slice_mode, parser_contract_version,
                extraction_watermark, total_row_count, slice_counts,
                field_signature, decision, status, captured_row_count,
                slice_count, complete
            ) VALUES (
                %s, 'corn_grain', 'recent', 'quickstats-crop-v1',
                '2024-01-01 00:00:00', 10, '{}'::JSONB, '{}'::JSONB,
                'ingest', 'captured', 10, 2, TRUE
            )
            """,
            (run_id,),
        )
        cursor.execute(
            """
            INSERT INTO control.usda_nass_slice (
                run_id, slice_key, product_id, agg_level_desc, year,
                provider_count, captured_row_count, count_capture_id,
                data_capture_id, status
            ) VALUES
                (%s, 'corn_grain|STATE|2024', 'corn_grain', 'STATE', 2024,
                 10, 7, %s, %s, 'captured'),
                (%s, 'corn_grain|COUNTY|2024', 'corn_grain', 'COUNTY', 2024,
                 99, 0, %s, NULL, 'partial')
            """,
            (run_id, capture_id, capture_id, run_id, capture_id),
        )
        [outcome] = nass_slice_ledger(cursor, {})
        assert outcome.result == "fail"
        assert any(entry.startswith("advanced:") for entry in outcome.evidence)
        assert any("corn_grain|STATE|2024" in entry for entry in outcome.evidence)
    postgres_connection.rollback()


def test_pep_sentinel_misclassification_fails(
    postgres_connection_factory: Callable[[], connection],
    postgres_connection: connection,
) -> None:
    """Covers: DQ-004 — a frozen Census sentinel must classify as sentinel."""
    source = f"PEPPROBE{uuid4().hex[:10].upper()}"
    _, capture_id = _seed_probe_capture(postgres_connection_factory, source)

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_schema = 'silver_pep'"
            " AND table_name = 'observation_revision'"
            " AND is_nullable = 'NO' AND column_default IS NULL"
        )
        required = {row[0] for row in cursor.fetchall()}
        provided = {
            "capture_id",
            "source_row_index",
            "source_column_index",
            "dataset_code",
            "release_vintage",
            "product_code",
            "metric_code",
            "observation_year",
            "value_source",
            "value_status",
            "parser_version",
            "summary_level",
            "unit",
            "source_header",
        }
        missing = required - provided
        assert not missing, f"seed does not cover NOT NULL columns: {missing}"

        cursor.execute(
            """
            INSERT INTO silver_pep.observation_revision (
                capture_id, source_row_index, source_column_index,
                dataset_code, release_vintage, product_code, metric_code,
                observation_year, value_source, value, value_status,
                parser_version, summary_level, unit, source_header
            ) VALUES (
                %s, 1, 1, 'pep_nst_alldata', 2025, 'NST-EST2025-ALLDATA',
                'POPESTIMATE', 2024, '-999999999', NULL, 'blank',
                'probe-v1', '040', 'persons', 'POPESTIMATE2024'
            )
            """,
            (capture_id,),
        )
        [outcome] = pep_sentinel_conformance(cursor, {})
        assert outcome.result == "fail"
        assert outcome.evidence == [f"{capture_id}|1|1"]
    postgres_connection.rollback()


def test_reference_and_registry_defects_fail(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-004 — incoherent resolutions and dangling publishers surface."""
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO silver_ref.geography_resolution (
                provider_source, provider_dataset, source_geo_type,
                source_code, source_vintage, geo_sk, status
            ) VALUES ('PROBE', 'probe_dataset', 'county', '01001', 2020,
                      NULL, 'resolved')
            """
        )
        [outcome] = reference_resolution_accounting(cursor, {})
        assert outcome.result == "fail"
        assert outcome.evidence == ["PROBE|probe_dataset|01001|resolved"]

        cursor.execute(
            """
            INSERT INTO gold_glossary.publisher_registry (
                source_code, publisher_schema, publisher_view,
                publisher_contract_version
            ) VALUES
                ('PROBE_MISSING', 'gold_probe', 'metric_publisher', 'v1'),
                ('CENSUS_ACS', 'gold_census', 'metric_publisher', 'v1')
            """
        )
        [outcome] = publisher_registry_reconciliation(cursor, {})
        assert outcome.result == "fail"
        assert outcome.evidence == ["PROBE_MISSING|gold_probe|metric_publisher"]
    postgres_connection.rollback()
