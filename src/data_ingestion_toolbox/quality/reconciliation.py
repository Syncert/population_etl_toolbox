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
import re
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .runner import (
    QualityRunError,
    QualityRunRecord,
    RuleExecutor,
    RuleOutcome,
    execute_rules,
)

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


#: Clauses an offender query must not carry: the ordering is an argument
#: and the bound belongs to `_offenders`.
_FORBIDDEN_CLAUSE = re.compile(r"\b(?:ORDER\s+BY|LIMIT)\b", re.IGNORECASE)


#: A term that qualifies its column with a relation alias -- ``dataset.domain``.
_QUALIFIED_TERM = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\.")

#: The one relation in scope on the wrapping statement.
_WRAPPER_ALIAS = "offender"


def _reject_inner_qualifiers(order_by: str) -> None:
    """Refuse an ordering that names a relation the wrapper cannot see.

    The ordering is applied outside the subquery, where the only relation in
    scope is ``offender``. A term qualified with the *inner* alias --
    ``ORDER BY dataset.domain`` over ``FROM (...) AS offender`` -- is not a
    different sort order, it is a statement PostgreSQL refuses outright, and
    the rule that carries it errors instead of reporting the offenders it
    exists to find. Fourteen sites wrote positions and the fifteenth wrote
    names, which is how DQ-008 shipped a rule that raised the first time a
    FRED dataset had no series row (DQ-009).
    """
    for term in order_by.split(","):
        for match in _QUALIFIED_TERM.finditer(term):
            alias = match.group(1)
            if alias.lower() == _WRAPPER_ALIAS:
                continue
            raise QualityRunError(
                f"an offender ordering cannot name the relation '{alias}': the "
                "ordering is applied outside the subquery, where only "
                f"'{_WRAPPER_ALIAS}' is in scope. Write the positions of the "
                "wrapped select list (`1, 2`), its bare output column names, "
                f"or qualify them with '{_WRAPPER_ALIAS}'"
            )


def _shifted(order_by: str) -> str:
    """``order_by`` with positional references moved past the count column.

    The wrapper below selects the count first, so every column an offender
    query names shifts one place right. Callers keep writing the positions
    of their own select list; the shift is this helper's business, not
    fifteen rules'.
    """
    terms = []
    for term in order_by.split(","):
        parts = term.split()
        if parts and parts[0].isdigit():
            parts[0] = str(int(parts[0]) + 1)
        terms.append(" ".join(parts))
    return ", ".join(terms)


def _offenders(
    cursor: Any,
    sql: str,
    *,
    order_by: str,
    params: tuple[Any, ...] = (),
) -> tuple[list[str], int]:
    """Bounded evidence ids and the *exact* number of offenders.

    `DATA_QUALITY_OPERATIONS.md` says `control.data_quality_result` holds
    "exact counts, bounded evidence ids" -- two different things -- and its
    operator query selects `observed_count` to judge how bad a failure is.
    These rules measured both from one bounded read: the query fetched
    `EVIDENCE_LIMIT + 1`, one more than the cap so truncation could be
    detected, the helper sliced the extra row away, and the outcome recorded
    the evidence's length. Twenty bad rows and twenty thousand both persisted
    `observed_count: 20`, always understating, on the one number an operator
    is pointed at (DQ-008).

    One statement, so the count and the sample describe one reading of the
    warehouse -- the reasoning of API-084 and API-100. `COUNT(*) OVER ()` is
    evaluated over the whole offender set before `LIMIT`, and the ordering
    that decides *which* offenders are kept is applied here rather than
    inside the subquery, so the sample is deterministic by the statement's
    own contract instead of by a planner preserving a subquery's sort.

    ``sql`` therefore carries neither ``ORDER BY`` nor ``LIMIT``, and
    ``order_by`` names only what the wrapping statement can see: positions of
    the wrapped select list, its bare output column names, or names qualified
    with ``offender``. An ordering naming the subquery's own relation alias is
    refused here rather than by PostgreSQL at run time (DQ-009).
    """
    # Matched as SQL words, not substrings: a status literal named
    # `over_limit` is not a LIMIT clause, and the first version of this guard
    # refused the USDA NASS rule for containing one.
    if _FORBIDDEN_CLAUSE.search(sql):
        raise QualityRunError(
            "an offender query must carry neither ORDER BY nor LIMIT: the "
            "ordering is passed as `order_by` and the bound is this helper's"
        )
    _reject_inner_qualifiers(order_by)
    cursor.execute(
        f"SELECT COUNT(*) OVER () AS offender_total, offender.*\n"
        f"FROM (\n{sql}\n) AS offender\n"
        f"ORDER BY {_shifted(order_by)}\n"
        f"LIMIT {EVIDENCE_LIMIT}",
        params,
    )
    rows = cursor.fetchall()
    if not rows:
        return [], 0
    return (
        ["|".join(str(part) for part in row[1:]) for row in rows],
        int(rows[0][0]),
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


#: Request statuses that legitimately hold a capture.
#:
#: The capture is committed before the payload is parsed (ADR-0001), and the
#: terminal status is written from what the parse found: ``captured`` when
#: rows were loaded, ``empty`` when the provider answered nothing to load,
#: ``quarantined`` when the payload could not be parsed or the release was
#: not publishable. All three are the contract working, and all three leave
#: captured bytes behind on purpose -- quarantined most of all, since the
#: bytes are the evidence of what could not be parsed.
#:
#: A capture bound to ``planned``, ``running`` or ``failed`` is the defect
#: this rule looks for: bytes with no accounting, or accounting that never
#: reached a terminal state (DQ-010).
_CAPTURE_BEARING_STATUSES = ("captured", "empty", "quarantined")


def verify_capture_lineage(cursor: Any, scope: Mapping[str, Any]) -> list[RuleOutcome]:
    """DQ-SHARED-002 — captured requests and captures agree in both directions."""
    clause, params = _source_filter(scope, "request.source_code")
    orphan_requests, orphan_request_total = _offenders(
        cursor,
        f"""
        SELECT request.request_id
          FROM control.ingestion_request AS request
          LEFT JOIN raw_capture.response_capture AS capture
            ON capture.request_id = request.request_id
         WHERE request.status = 'captured'
           AND capture.capture_id IS NULL{clause}
        """,
        order_by="1",
        params=tuple(params),
    )

    clause, params = _source_filter(scope, "capture.source_code")
    # LEFT JOIN, because the inner join hid the worst case: a capture whose
    # request row is missing altogether disappeared from the rule instead of
    # being reported as bytes with no accounting at all.
    orphan_captures, orphan_capture_total = _offenders(
        cursor,
        f"""
        SELECT capture.capture_id
          FROM raw_capture.response_capture AS capture
          LEFT JOIN control.ingestion_request AS request
            ON request.request_id = capture.request_id
         WHERE (request.request_id IS NULL
                OR request.status <> ALL(%s)){clause}
        """,
        order_by="1",
        params=(list(_CAPTURE_BEARING_STATUSES), *params),
    )

    return [
        RuleOutcome(
            "control.ingestion_request",
            "fail" if orphan_requests else "pass",
            observed_count=orphan_request_total,
            expected_count=0,
            evidence=orphan_requests[:EVIDENCE_LIMIT],
        ),
        RuleOutcome(
            "raw_capture.response_capture",
            "fail" if orphan_captures else "pass",
            observed_count=orphan_capture_total,
            expected_count=0,
            evidence=orphan_captures[:EVIDENCE_LIMIT],
        ),
    ]


def reconcile_requests(cursor: Any, scope: Mapping[str, Any]) -> list[RuleOutcome]:
    """DQ-SHARED-003 — every terminal request is accounted for, never lost."""
    clause, params = _source_filter(scope, "request.source_code")
    unfinished, unfinished_total = _offenders(
        cursor,
        f"""
        SELECT request.request_id
          FROM control.ingestion_request AS request
          JOIN control.ingestion_run AS run
            ON run.run_id = request.run_id
         WHERE run.finished_at IS NOT NULL
           AND request.status IN ('planned', 'running'){clause}
        """,
        order_by="1",
        params=tuple(params),
    )

    clause, params = _source_filter(scope, "request.source_code")
    unaccounted, unaccounted_total = _offenders(
        cursor,
        f"""
        SELECT request.request_id
          FROM control.ingestion_request AS request
          LEFT JOIN control.capture_quarantine AS quarantine
            ON quarantine.run_id = request.run_id
           AND quarantine.source_code = request.source_code
         WHERE request.status = 'quarantined'
           AND quarantine.quarantine_id IS NULL{clause}
        """,
        order_by="1",
        params=tuple(params),
    )

    return [
        RuleOutcome(
            "control.ingestion_run",
            "fail" if unfinished else "pass",
            observed_count=unfinished_total,
            expected_count=0,
            evidence=unfinished[:EVIDENCE_LIMIT],
        ),
        RuleOutcome(
            "control.capture_quarantine",
            "fail" if unaccounted else "pass",
            observed_count=unaccounted_total,
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
