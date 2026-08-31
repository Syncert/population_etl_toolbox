"""Source-specific coverage and validity executors (DQ-004).

Each executor measures one declared rule from the inventory against the live
warehouse. All of them share three properties:

- deterministic: they compare configured scope, control ledgers, and declared
  value semantics; nothing here is statistical;
- valid emptiness aware: a warehouse with no data for a source yields
  ``not_applicable``, never a false failure — the plan's core distinction
  between valid emptiness and missing configured work; and
- bounded: evidence carries at most ``EVIDENCE_LIMIT`` identifiers, never
  payloads.

The registered scope comes from each source's own code registry (imported
here), so a registry change automatically changes what these rules expect.
"""

from __future__ import annotations

from typing import Any, Mapping

from data_ingestion_toolbox.census_pep.silver_pep.replay import (
    _CENSUS_NULL_SENTINELS,
)
from data_ingestion_toolbox.usda_nass.silver_nass.values import SYMBOL_STATUS

from .reconciliation import EVIDENCE_LIMIT
from .runner import RuleExecutor, RuleOutcome

#: Slice-ledger states that mean the slice never finished its work.
_UNFINISHED_SLICE_STATUSES = ("planned", "running")


def _count(cursor: Any, sql: str, params: tuple[Any, ...] = ()) -> int:
    cursor.execute(sql, params)
    return cursor.fetchone()[0]


def _ids(cursor: Any, sql: str, params: tuple[Any, ...] = ()) -> list[str]:
    cursor.execute(sql, params)
    return [
        "|".join(str(part) for part in row)
        for row in cursor.fetchall()[:EVIDENCE_LIMIT]
    ]


def _ledger_outcome(
    cursor: Any,
    *,
    object_name: str,
    table: str,
    label_columns: str,
) -> RuleOutcome:
    """Shared slice-ledger accounting for the ACS/BLS/FRED request ledgers.

    A finished ledger may contain only terminal work: an abandoned
    planned/running slice, a failed slice, or a "successful" slice that
    loaded nothing (distinct from an explicit ``empty`` outcome) is missing
    configured work, not valid emptiness.
    """
    total = _count(cursor, f"SELECT COUNT(*) FROM {table}")
    if total == 0:
        return RuleOutcome(object_name, "not_applicable")
    offenders = _ids(
        cursor,
        f"""
        SELECT {label_columns}, status
          FROM {table}
         WHERE status IN %s
            OR status = 'failed'
            OR (status = 'success' AND rows_loaded = 0)
         ORDER BY 1
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
        (_UNFINISHED_SLICE_STATUSES,),
    )
    return RuleOutcome(
        object_name,
        "fail" if offenders else "pass",
        observed_count=len(offenders),
        expected_count=0,
        evidence=offenders[:EVIDENCE_LIMIT],
    )


def acs_slice_reconciliation(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-ACS-002 — the configured ACS slice ledger accounts for its work."""
    del scope
    return [
        _ledger_outcome(
            cursor,
            object_name="control.acs_ingestion_slices",
            table="control.acs_ingestion_slices",
            label_columns="dataset || ':' || year || ':' || geo_level",
        )
    ]


def bls_chunk_reconciliation(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-BLS-002 — request-sized BLS chunks cannot appear complete partially."""
    del scope
    return [
        _ledger_outcome(
            cursor,
            object_name="control.bls_ingestion_slices",
            table="control.bls_ingestion_slices",
            label_columns="program || ':' || year_start || '-' || year_end",
        )
    ]


def fred_slice_reconciliation(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-FRED-002 — FRED domains, ranges, and series metadata reconcile."""
    del scope
    outcomes = [
        _ledger_outcome(
            cursor,
            object_name="control.fred_ingestion_slices",
            table="control.fred_ingestion_slices",
            label_columns="domain || ':' || date_start",
        )
    ]
    configured = _count(cursor, "SELECT COUNT(*) FROM raw_fred.fred_datasets")
    if configured == 0:
        outcomes.append(RuleOutcome("raw_fred.fred_datasets", "not_applicable"))
        return outcomes
    unmatched = _ids(
        cursor,
        f"""
        SELECT dataset.domain, dataset.series_id
          FROM raw_fred.fred_datasets AS dataset
          LEFT JOIN raw_fred.fred_series AS series
            ON series.series_id = dataset.series_id
         WHERE series.series_id IS NULL
         ORDER BY dataset.domain, dataset.series_id
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
    )
    outcomes.append(
        RuleOutcome(
            "raw_fred.fred_datasets",
            "fail" if unmatched else "pass",
            observed_count=len(unmatched),
            expected_count=0,
            evidence=unmatched[:EVIDENCE_LIMIT],
        )
    )
    return outcomes


def pep_release_completeness(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-PEP-002 — every PEP fact traces to a complete release load."""
    del scope
    total = _count(cursor, "SELECT COUNT(*) FROM silver_pep.fact_population_estimate")
    if total == 0:
        return [RuleOutcome("silver_pep.release_load", "not_applicable")]
    unverified = _ids(
        cursor,
        f"""
        SELECT DISTINCT fact.capture_id
          FROM silver_pep.fact_population_estimate AS fact
          LEFT JOIN silver_pep.release_load AS load
            ON load.capture_id = fact.capture_id
         WHERE load.capture_id IS NULL
            OR load.completeness_status <> 'complete'
         ORDER BY 1
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
    )
    return [
        RuleOutcome(
            "silver_pep.release_load",
            "fail" if unverified else "pass",
            observed_count=len(unverified),
            expected_count=0,
            evidence=unverified[:EVIDENCE_LIMIT],
        )
    ]


def pep_registry_reconciliation(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-PEP-003 — loaded PEP scope reconciles to the materialized registry."""
    del scope
    loaded = _count(cursor, "SELECT COUNT(*) FROM silver_pep.fact_population_estimate")
    if loaded == 0:
        return [RuleOutcome("silver_pep.pep_release", "not_applicable")]
    unregistered = _ids(
        cursor,
        f"""
        SELECT DISTINCT fact.dataset_code, fact.release_vintage
          FROM silver_pep.fact_population_estimate AS fact
          LEFT JOIN silver_pep.pep_release AS release
            ON release.dataset_code = fact.dataset_code
           AND release.vintage_year = fact.release_vintage
         WHERE release.dataset_code IS NULL
         ORDER BY 1, 2
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
    )
    unloaded = _ids(
        cursor,
        f"""
        SELECT release.dataset_code, release.vintage_year
          FROM silver_pep.pep_release AS release
         WHERE release.status = 'published'
           AND NOT EXISTS (
               SELECT 1
                 FROM silver_pep.release_load AS load
                WHERE load.dataset_code = release.dataset_code
                  AND load.release_vintage = release.vintage_year
                  AND load.completeness_status = 'complete'
           )
         ORDER BY 1, 2
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
    )
    offenders = unregistered + unloaded
    return [
        RuleOutcome(
            "silver_pep.pep_release",
            "fail" if offenders else "pass",
            observed_count=len(offenders),
            expected_count=0,
            evidence=(
                ["unregistered:" + entry for entry in unregistered]
                + ["unloaded:" + entry for entry in unloaded]
            )[:EVIDENCE_LIMIT],
        )
    ]


def pep_sentinel_conformance(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-PEP-004 — the frozen Census sentinel set governs value_status."""
    del scope
    total = _count(cursor, "SELECT COUNT(*) FROM silver_pep.observation_revision")
    if total == 0:
        return [RuleOutcome("silver_pep.observation_revision", "not_applicable")]
    cursor.execute(
        f"""
        SELECT capture_id, source_row_index, source_column_index
          FROM silver_pep.observation_revision
         WHERE (BTRIM(COALESCE(value_source, '')) = ANY(%s)
                AND value_status <> 'sentinel')
            OR (value_status = 'sentinel'
                AND BTRIM(COALESCE(value_source, '')) <> ALL(%s))
         ORDER BY 1, 2, 3
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
        (sorted(_CENSUS_NULL_SENTINELS), sorted(_CENSUS_NULL_SENTINELS)),
    )
    offenders = ["|".join(str(part) for part in row) for row in cursor.fetchall()]
    return [
        RuleOutcome(
            "silver_pep.observation_revision",
            "fail" if offenders else "pass",
            observed_count=len(offenders),
            expected_count=0,
            evidence=offenders[:EVIDENCE_LIMIT],
        )
    ]


def cdc_watermark_monotonicity(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-CDC-002 — an ingest decision never moves a watermark backwards."""
    del scope
    total = _count(cursor, "SELECT COUNT(*) FROM control.cdc_dataset_release")
    if total == 0:
        return [RuleOutcome("control.cdc_dataset_release", "not_applicable")]
    regressions = _ids(
        cursor,
        f"""
        SELECT release.asset_id, release.release_watermark
          FROM control.cdc_dataset_release AS release
         WHERE release.decision = 'ingest'
           AND EXISTS (
               SELECT 1
                 FROM control.cdc_dataset_release AS earlier
                WHERE earlier.asset_id = release.asset_id
                  AND earlier.decision = 'ingest'
                  AND earlier.created_at < release.created_at
                  AND earlier.release_watermark > release.release_watermark
           )
         ORDER BY 1, 2
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
    )
    return [
        RuleOutcome(
            "control.cdc_dataset_release",
            "fail" if regressions else "pass",
            observed_count=len(regressions),
            expected_count=0,
            evidence=regressions[:EVIDENCE_LIMIT],
        )
    ]


def cdc_suppression_conformance(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-CDC-004 — suppressed and missing are never numbers, never zero."""
    del scope
    total = _count(cursor, "SELECT COUNT(*) FROM silver_cdc.fact_health_observation")
    if total == 0:
        return [RuleOutcome("silver_cdc.fact_health_observation", "not_applicable")]
    offenders = _ids(
        cursor,
        f"""
        SELECT asset_id, release_watermark, source_record_id
          FROM silver_cdc.fact_health_observation
         WHERE value_status IN ('suppressed', 'missing')
           AND value IS NOT NULL
         ORDER BY 1, 2, 3
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
    )
    return [
        RuleOutcome(
            "silver_cdc.fact_health_observation",
            "fail" if offenders else "pass",
            observed_count=len(offenders),
            expected_count=0,
            evidence=offenders[:EVIDENCE_LIMIT],
        )
    ]


def fbi_participation_coverage(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-FBI-002 — no published observation without a coverage row."""
    del scope
    total = _count(cursor, "SELECT COUNT(*) FROM gold_fbi.crime_observation")
    if total == 0:
        return [RuleOutcome("gold_fbi.crime_observation", "not_applicable")]
    uncovered = _ids(
        cursor,
        f"""
        SELECT observation.product_id, observation.release_key,
               observation.source_record_id
          FROM gold_fbi.crime_observation AS observation
          LEFT JOIN gold_fbi.reporting_coverage AS coverage
            ON coverage.product_id = observation.product_id
           AND coverage.release_key = observation.release_key
           AND coverage.subject_type = observation.subject_type
           AND coverage.subject_code = observation.subject_code
           AND coverage.period = observation.period
         WHERE coverage.product_id IS NULL
         ORDER BY 1, 2, 3
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
    )
    return [
        RuleOutcome(
            "gold_fbi.crime_observation",
            "fail" if uncovered else "pass",
            observed_count=len(uncovered),
            expected_count=0,
            evidence=uncovered[:EVIDENCE_LIMIT],
        )
    ]


def fbi_reported_vs_absent(cursor: Any, scope: Mapping[str, Any]) -> list[RuleOutcome]:
    """DQ-FBI-003 — a published zero is a value; an absent month is NULL."""
    del scope
    total = _count(cursor, "SELECT COUNT(*) FROM silver_fbi.fact_crime_observation")
    if total == 0:
        return [RuleOutcome("silver_fbi.fact_crime_observation", "not_applicable")]
    offenders = _ids(
        cursor,
        f"""
        SELECT product_id, release_key, source_record_id, value_status
          FROM silver_fbi.fact_crime_observation
         WHERE (value_status = 'not_reported' AND value IS NOT NULL)
            OR (value_status = 'reported' AND value IS NULL)
         ORDER BY 1, 2, 3
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
    )
    return [
        RuleOutcome(
            "silver_fbi.fact_crime_observation",
            "fail" if offenders else "pass",
            observed_count=len(offenders),
            expected_count=0,
            evidence=offenders[:EVIDENCE_LIMIT],
        )
    ]


def fbi_aggregation_boundary(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-FBI-004 — the area filter stays at agency grain, never a total."""
    del scope
    total = _count(
        cursor, "SELECT COUNT(*) FROM gold_fbi.agency_observation_area_filter"
    )
    if total == 0:
        return [
            RuleOutcome("gold_fbi.agency_observation_area_filter", "not_applicable")
        ]
    distinct = _count(
        cursor,
        """
        SELECT COUNT(*)
          FROM (
              -- The view legitimately emits one row per associated area for
              -- a multi-area agency, so the non-fanout grain is observation
              -- x filter area; observation_sk is the per-fact surrogate the
              -- view exposes. A duplicate at this grain means overlapping
              -- effective-dated relationship rows fanned the join out.
              SELECT DISTINCT ori, observation_sk, filter_geo_id
                FROM gold_fbi.agency_observation_area_filter
          ) AS grain
        """,
    )
    return [
        RuleOutcome(
            "gold_fbi.agency_observation_area_filter",
            "pass" if distinct == total else "fail",
            observed_count=total,
            expected_count=distinct,
            evidence=(
                []
                if distinct == total
                else [f"rows={total}", f"distinct_agency_grain={distinct}"]
            ),
        )
    ]


def nass_slice_ledger(cursor: Any, scope: Mapping[str, Any]) -> list[RuleOutcome]:
    """DQ-NASS-002 — preflight counts, captures, and slice states agree."""
    del scope
    total = _count(cursor, "SELECT COUNT(*) FROM control.usda_nass_slice")
    if total == 0:
        return [RuleOutcome("control.usda_nass_slice", "not_applicable")]
    offenders = _ids(
        cursor,
        f"""
        SELECT slice.run_id, slice.slice_key, slice.status
          FROM control.usda_nass_slice AS slice
         WHERE slice.status IN ('preflighted')
            OR (slice.status = 'captured'
                AND slice.captured_row_count <> slice.provider_count)
            OR (slice.status = 'captured' AND slice.data_capture_id IS NULL)
         ORDER BY 1, 2
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
    )
    advanced = _ids(
        cursor,
        f"""
        SELECT release.run_id, release.product_id
          FROM control.usda_nass_release AS release
          JOIN control.usda_nass_slice AS slice
            ON slice.run_id = release.run_id
         WHERE release.decision = 'ingest'
           AND slice.status IN ('over_limit', 'partial')
         ORDER BY 1, 2
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
    )
    combined = offenders + ["advanced:" + entry for entry in advanced]
    return [
        RuleOutcome(
            "control.usda_nass_slice",
            "fail" if combined else "pass",
            observed_count=len(combined),
            expected_count=0,
            evidence=combined[:EVIDENCE_LIMIT],
        )
    ]


def nass_suppression_vocabulary(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-NASS-003 — every Quick Stats symbol maps to its own value status."""
    del scope
    total = _count(cursor, "SELECT COUNT(*) FROM silver_nass.fact_crop_observation")
    if total == 0:
        return [RuleOutcome("silver_nass.fact_crop_observation", "not_applicable")]
    symbols = sorted(SYMBOL_STATUS)
    statuses = [SYMBOL_STATUS[symbol] for symbol in symbols]
    offenders = _ids(
        cursor,
        f"""
        SELECT product_id, release_watermark, source_record_id,
               value_source, value_status
          FROM silver_nass.fact_crop_observation
          JOIN UNNEST(%s::TEXT[], %s::TEXT[]) AS mapping(symbol, status)
            ON BTRIM(value_source) = mapping.symbol
         WHERE value_status <> mapping.status
         ORDER BY 1, 2, 3
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
        (symbols, statuses),
    )
    return [
        RuleOutcome(
            "silver_nass.fact_crop_observation",
            "fail" if offenders else "pass",
            observed_count=len(offenders),
            expected_count=0,
            evidence=offenders[:EVIDENCE_LIMIT],
        )
    ]


def reference_resolution_accounting(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-REF-003 — every resolution row carries a coherent verdict."""
    del scope
    total = _count(cursor, "SELECT COUNT(*) FROM silver_ref.geography_resolution")
    if total == 0:
        return [RuleOutcome("silver_ref.geography_resolution", "not_applicable")]
    offenders = _ids(
        cursor,
        f"""
        SELECT provider_source, provider_dataset, source_code, status
          FROM silver_ref.geography_resolution
         WHERE (status = 'resolved' AND geo_sk IS NULL)
            OR (status <> 'resolved' AND geo_sk IS NOT NULL)
         ORDER BY 1, 2, 3
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
    )
    return [
        RuleOutcome(
            "silver_ref.geography_resolution",
            "fail" if offenders else "pass",
            observed_count=len(offenders),
            expected_count=0,
            evidence=offenders[:EVIDENCE_LIMIT],
        )
    ]


def publisher_registry_reconciliation(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-GLOSSARY-001 — every registry row resolves to a live publisher view."""
    del scope
    total = _count(cursor, "SELECT COUNT(*) FROM gold_glossary.publisher_registry")
    if total == 0:
        return [RuleOutcome("gold_glossary.publisher_registry", "not_applicable")]
    dangling = _ids(
        cursor,
        f"""
        SELECT registry.source_code, registry.publisher_schema,
               registry.publisher_view
          FROM gold_glossary.publisher_registry AS registry
          LEFT JOIN information_schema.views AS live
            ON live.table_schema = registry.publisher_schema
           AND live.table_name = registry.publisher_view
         WHERE live.table_name IS NULL
         ORDER BY 1
         LIMIT {EVIDENCE_LIMIT + 1}
        """,
    )
    return [
        RuleOutcome(
            "gold_glossary.publisher_registry",
            "fail" if dangling else "pass",
            observed_count=len(dangling),
            expected_count=0,
            evidence=dangling[:EVIDENCE_LIMIT],
        )
    ]


#: Every DQ-004 executor keyed by the rule it measures.
SOURCE_EXECUTORS: Mapping[str, RuleExecutor] = {
    "DQ-ACS-002": acs_slice_reconciliation,
    "DQ-BLS-002": bls_chunk_reconciliation,
    "DQ-FRED-002": fred_slice_reconciliation,
    "DQ-PEP-002": pep_release_completeness,
    "DQ-PEP-003": pep_registry_reconciliation,
    "DQ-PEP-004": pep_sentinel_conformance,
    "DQ-CDC-002": cdc_watermark_monotonicity,
    "DQ-CDC-004": cdc_suppression_conformance,
    "DQ-FBI-002": fbi_participation_coverage,
    "DQ-FBI-003": fbi_reported_vs_absent,
    "DQ-FBI-004": fbi_aggregation_boundary,
    "DQ-NASS-002": nass_slice_ledger,
    "DQ-NASS-003": nass_suppression_vocabulary,
    "DQ-REF-003": reference_resolution_accounting,
    "DQ-GLOSSARY-001": publisher_registry_reconciliation,
}
