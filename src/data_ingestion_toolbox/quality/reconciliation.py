"""Lineage and layer reconciliation executors, and the publication gate.

DQ-003 turns the shared capture/control lineage rules into executable
measurements and provides the generic identity comparator that source-specific
reconciliations build on. ``evaluate_publication_gate`` runs blocking rules
through the evidence runner before a release is allowed to publish: a failing
BLOCK or QUARANTINE rule refuses publication, which is what keeps the prior
published partition intact.

The CDC executors here are the reference layer-reconciliation wiring
(capture -> silver -> gold for one watermark release); the remaining sources
follow the same pattern under DQ-004.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .runner import QualityRunRecord, RuleExecutor, RuleOutcome, execute_rules

#: How many offending identifiers a single outcome may carry as evidence.
EVIDENCE_LIMIT = 20

#: Default number of recent captures a bounded checksum pass verifies.
DEFAULT_CAPTURE_LIMIT = 1000


@dataclass(frozen=True, slots=True)
class IdentityComparison:
    """Counts and bounded examples from comparing two identity sets."""

    expected_count: int
    observed_count: int
    missing: tuple[tuple[Any, ...], ...]
    unexpected: tuple[tuple[Any, ...], ...]

    @property
    def reconciled(self) -> bool:
        return (
            self.expected_count == self.observed_count
            and not self.missing
            and not self.unexpected
        )


def compare_identity_sets(
    cursor: Any,
    *,
    expected_sql: str,
    observed_sql: str,
    params: Sequence[Any] = (),
    evidence_limit: int = EVIDENCE_LIMIT,
) -> IdentityComparison:
    """Compare two identity queries exactly, with bounded example evidence.

    Both queries must select the same identity columns. Counts alone cannot
    prove reconciliation — equal counts can conceal replacement — so missing
    and unexpected identities are sampled with ``EXCEPT`` in both directions.
    """
    cursor.execute(f"SELECT COUNT(*) FROM ({expected_sql}) AS expected", params)
    expected_count = cursor.fetchone()[0]
    cursor.execute(f"SELECT COUNT(*) FROM ({observed_sql}) AS observed", params)
    observed_count = cursor.fetchone()[0]

    cursor.execute(
        f"({expected_sql}) EXCEPT ({observed_sql}) LIMIT {int(evidence_limit)}",
        tuple(params) + tuple(params),
    )
    missing = tuple(tuple(row) for row in cursor.fetchall())
    cursor.execute(
        f"({observed_sql}) EXCEPT ({expected_sql}) LIMIT {int(evidence_limit)}",
        tuple(params) + tuple(params),
    )
    unexpected = tuple(tuple(row) for row in cursor.fetchall())
    return IdentityComparison(
        expected_count=expected_count,
        observed_count=observed_count,
        missing=missing,
        unexpected=unexpected,
    )


def comparison_outcome(
    object_name: str,
    comparison: IdentityComparison,
    *,
    partition_key: str = "",
    empty_is_not_applicable: bool = True,
) -> RuleOutcome:
    """Fold an identity comparison into one persistable outcome."""
    if (
        empty_is_not_applicable
        and comparison.expected_count == 0
        and comparison.observed_count == 0
    ):
        result = "not_applicable"
    elif comparison.reconciled:
        result = "pass"
    else:
        result = "fail"
    evidence: list[str] = [
        "missing:" + "|".join(str(part) for part in row) for row in comparison.missing
    ] + [
        "unexpected:" + "|".join(str(part) for part in row)
        for row in comparison.unexpected
    ]
    return RuleOutcome(
        object_name,
        result,
        partition_key=partition_key,
        observed_count=comparison.observed_count,
        expected_count=comparison.expected_count,
        evidence=evidence[:EVIDENCE_LIMIT],
    )


def _source_filter(scope: Mapping[str, Any], column: str) -> tuple[str, list[Any]]:
    source_code = scope.get("source_code")
    if source_code:
        return f" AND {column} = %s", [source_code]
    return "", []


def verify_capture_checksums(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-SHARED-001 — recompute checksums for a bounded recent capture window."""
    limit = int(scope.get("capture_limit", DEFAULT_CAPTURE_LIMIT))
    clause, params = _source_filter(scope, "capture.source_code")
    cursor.execute(
        f"""
        SELECT capture.capture_id, capture.payload_checksum, blob.payload
          FROM raw_capture.response_capture AS capture
          JOIN raw_capture.payload_blob AS blob
            ON blob.payload_checksum = capture.payload_checksum
         WHERE TRUE{clause}
         ORDER BY capture.retrieved_at DESC
         LIMIT %s
        """,
        (*params, limit),
    )
    rows = cursor.fetchall()
    mismatched = [
        str(capture_id)
        for capture_id, checksum, payload in rows
        if hashlib.sha256(bytes(payload)).hexdigest() != checksum
    ]
    if not rows:
        result = "not_applicable"
    elif mismatched:
        result = "fail"
    else:
        result = "pass"
    return [
        RuleOutcome(
            "raw_capture.response_capture",
            result,
            observed_count=len(rows) - len(mismatched),
            expected_count=len(rows),
            evidence=mismatched[:EVIDENCE_LIMIT],
        )
    ]


def verify_capture_lineage(cursor: Any, scope: Mapping[str, Any]) -> list[RuleOutcome]:
    """DQ-SHARED-002 — captured requests and captures agree in both directions."""
    clause, params = _source_filter(scope, "request.source_code")
    cursor.execute(
        f"""
        SELECT request.request_id
          FROM control.ingestion_request AS request
          LEFT JOIN raw_capture.response_capture AS capture
            ON capture.request_id = request.request_id
         WHERE request.status = 'captured'
           AND capture.capture_id IS NULL{clause}
         ORDER BY request.request_id
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
        params,
    )
    orphan_requests = [str(row[0]) for row in cursor.fetchall()]

    clause, params = _source_filter(scope, "capture.source_code")
    cursor.execute(
        f"""
        SELECT capture.capture_id
          FROM raw_capture.response_capture AS capture
          JOIN control.ingestion_request AS request
            ON request.request_id = capture.request_id
         WHERE request.status <> 'captured'{clause}
         ORDER BY capture.capture_id
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
        params,
    )
    orphan_captures = [str(row[0]) for row in cursor.fetchall()]

    return [
        RuleOutcome(
            "control.ingestion_request",
            "fail" if orphan_requests else "pass",
            observed_count=len(orphan_requests),
            expected_count=0,
            evidence=orphan_requests[:EVIDENCE_LIMIT],
        ),
        RuleOutcome(
            "raw_capture.response_capture",
            "fail" if orphan_captures else "pass",
            observed_count=len(orphan_captures),
            expected_count=0,
            evidence=orphan_captures[:EVIDENCE_LIMIT],
        ),
    ]


def reconcile_requests(cursor: Any, scope: Mapping[str, Any]) -> list[RuleOutcome]:
    """DQ-SHARED-003 — every terminal request is accounted for, never lost."""
    clause, params = _source_filter(scope, "request.source_code")
    cursor.execute(
        f"""
        SELECT request.request_id
          FROM control.ingestion_request AS request
          JOIN control.ingestion_run AS run
            ON run.run_id = request.run_id
         WHERE run.finished_at IS NOT NULL
           AND request.status IN ('planned', 'running'){clause}
         ORDER BY request.request_id
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
        params,
    )
    unfinished = [str(row[0]) for row in cursor.fetchall()]

    clause, params = _source_filter(scope, "request.source_code")
    cursor.execute(
        f"""
        SELECT request.request_id
          FROM control.ingestion_request AS request
          LEFT JOIN control.capture_quarantine AS quarantine
            ON quarantine.run_id = request.run_id
           AND quarantine.source_code = request.source_code
         WHERE request.status = 'quarantined'
           AND quarantine.quarantine_id IS NULL{clause}
         ORDER BY request.request_id
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
        params,
    )
    unaccounted = [str(row[0]) for row in cursor.fetchall()]

    return [
        RuleOutcome(
            "control.ingestion_run",
            "fail" if unfinished else "pass",
            observed_count=len(unfinished),
            expected_count=0,
            evidence=unfinished[:EVIDENCE_LIMIT],
        ),
        RuleOutcome(
            "control.capture_quarantine",
            "fail" if unaccounted else "pass",
            observed_count=len(unaccounted),
            expected_count=0,
            evidence=unaccounted[:EVIDENCE_LIMIT],
        ),
    ]


#: The shared lineage executors every gate evaluation includes.
SHARED_RECONCILIATION_EXECUTORS: Mapping[str, RuleExecutor] = {
    "DQ-SHARED-001": verify_capture_checksums,
    "DQ-SHARED-002": verify_capture_lineage,
    "DQ-SHARED-003": reconcile_requests,
}


def cdc_release_reconciliation(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-CDC-003 — one CDC release reconciles across capture, silver, and gold.

    Reference layer reconciliation: the control release's accepted row count
    must equal the retained silver revisions, the conformed facts plus
    quarantined rows, and — once published — the gold projection.
    """
    asset_id = scope["asset_id"]
    release_watermark = scope["release_watermark"]
    partition = f"{asset_id}:{release_watermark}"

    cursor.execute(
        """
        SELECT release.captured_row_count, release.complete, release.status,
               release.run_id
          FROM control.cdc_dataset_release AS release
         WHERE release.asset_id = %s
           AND release.release_watermark::TEXT = %s
         ORDER BY release.updated_at DESC
         LIMIT 1
        """,
        (asset_id, release_watermark),
    )
    release_row = cursor.fetchone()
    if release_row is None:
        return [
            RuleOutcome(
                "control.cdc_dataset_release",
                "not_applicable",
                partition_key=partition,
            )
        ]
    row_count, complete, status, run_id = release_row

    outcomes: list[RuleOutcome] = []
    if not complete and status in {"silver_ready", "published"}:
        outcomes.append(
            RuleOutcome(
                "control.cdc_dataset_release",
                "fail",
                partition_key=partition,
                evidence=[f"incomplete release advanced to {status}"],
            )
        )

    cursor.execute(
        """
        SELECT COUNT(*)
          FROM silver_cdc.fact_health_observation
         WHERE asset_id = %s AND release_watermark = %s
        """,
        (asset_id, release_watermark),
    )
    fact_count = cursor.fetchone()[0]
    cursor.execute(
        """
        SELECT COUNT(*)
          FROM silver_cdc.observation_quarantine
         WHERE asset_id = %s AND release_watermark = %s
        """,
        (asset_id, release_watermark),
    )
    quarantined_count = cursor.fetchone()[0]

    accounted = fact_count + quarantined_count
    if status in {"silver_ready", "published"}:
        outcomes.append(
            RuleOutcome(
                "silver_cdc.fact_health_observation",
                "pass" if accounted == row_count else "fail",
                partition_key=partition,
                observed_count=accounted,
                expected_count=row_count,
                evidence=(
                    []
                    if accounted == row_count
                    else [
                        f"facts={fact_count}",
                        f"quarantined={quarantined_count}",
                        f"release_row_count={row_count}",
                    ]
                ),
            )
        )

    if status == "published":
        comparison = compare_identity_sets(
            cursor,
            expected_sql=(
                "SELECT source_record_id "
                "FROM silver_cdc.fact_health_observation "
                "WHERE asset_id = %s AND release_watermark = %s"
            ),
            observed_sql=(
                "SELECT source_record_id "
                "FROM gold_cdc.health_observation "
                "WHERE asset_id = %s AND release_watermark = %s"
            ),
            params=(asset_id, release_watermark),
        )
        outcomes.append(
            comparison_outcome(
                "gold_cdc.health_observation",
                comparison,
                partition_key=partition,
                empty_is_not_applicable=False,
            )
        )
    return outcomes


def build_cdc_gate_executors(
    asset_id: str, release_watermark: str
) -> dict[str, RuleExecutor]:
    """The executor set for gating one CDC release's publication."""
    del asset_id, release_watermark  # bound through the scope at execution
    return {
        **SHARED_RECONCILIATION_EXECUTORS,
        "DQ-CDC-003": cdc_release_reconciliation,
    }


@dataclass(frozen=True, slots=True)
class GateDecision:
    """The publication gate's verdict for one release/partition."""

    publishable: bool
    record: QualityRunRecord


def evaluate_publication_gate(
    connection: Any,
    *,
    source_code: str,
    code_commit_sha: str,
    executors: Mapping[str, RuleExecutor],
    scope: Mapping[str, Any] | None = None,
    ingestion_run_id: str | None = None,
) -> GateDecision:
    """Run blocking rules before publication and decide whether it may proceed.

    Only a clean or warned run publishes. A failed or errored run refuses
    publication — the caller must leave the prior published partition in
    place — and the refusal itself is persisted evidence, not a log line.
    """
    record = execute_rules(
        connection,
        source_code=source_code,
        assessment_type="inline",
        code_commit_sha=code_commit_sha,
        executors=executors,
        scope=scope,
        ingestion_run_id=ingestion_run_id,
    )
    return GateDecision(
        publishable=record.overall_status in {"pass", "warn"},
        record=record,
    )
