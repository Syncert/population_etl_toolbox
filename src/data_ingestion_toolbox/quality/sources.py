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

from types import MappingProxyType
from typing import Any, Mapping

from data_ingestion_toolbox.census_pep.silver_pep.replay import (
    _CENSUS_NULL_SENTINELS,
)
from data_ingestion_toolbox.usda_nass.silver_nass.values import SYMBOL_STATUS

from .reconciliation import EVIDENCE_LIMIT, _offenders
from .runner import RuleExecutor, RuleOutcome

#: Slice-ledger states that mean the slice never finished its work.
_UNFINISHED_SLICE_STATUSES = ("planned", "running")


def _count(cursor: Any, sql: str, params: tuple[Any, ...] = ()) -> int:
    cursor.execute(sql, params)
    return cursor.fetchone()[0]


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
    offenders, offenders_total = _offenders(
        cursor,
        f"""
        SELECT {label_columns}, status
          FROM {table}
         WHERE status IN %s
            OR status = 'failed'
            OR (status = 'success' AND rows_loaded = 0)
        """,
        order_by="1",
        params=(_UNFINISHED_SLICE_STATUSES,),
    )
    return RuleOutcome(
        object_name,
        "fail" if offenders else "pass",
        observed_count=offenders_total,
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


def bls_geography_accountability(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-BLS-004 — every published BLS geography is stored or is accounted for.

    `silver_bls.fact_labor_statistics.geo_sk` is `NOT NULL`, so a series whose
    area the shared reference does not carry cannot be stored at all. This rule
    is the other half of that: a geography the provider published must appear
    either in the fact table or in `silver_ref.geography_resolution`, and one
    in neither has disappeared with no queryable trace.

    It was declared unimplemented because "the serving refresh joins series,
    survey and geography, so an unresolved row is dropped rather than
    reported". The drop is now recorded, so the comparison has both sides.

    The geographies are derived from `observation_revision` -- the provider's
    own rows, before any join -- rather than from the fact table, because the
    fact table is the side under test and a rule that reads only it would be
    comparing a set to itself.
    """
    del scope
    unaccounted, total = _offenders(
        cursor,
        """
        WITH published AS (
            SELECT DISTINCT series.area_code, revision.program
              FROM silver_bls.observation_revision AS revision
              JOIN raw_bls.bls_series AS series
                ON series.series_id = revision.series_id
               AND series.program = revision.program
             WHERE series.area_code IS NOT NULL
        ), stored AS (
            SELECT DISTINCT series.area_code, fact.program
              FROM silver_bls.fact_labor_statistics AS fact
              JOIN raw_bls.bls_series AS series
                ON series.series_id = fact.series_id
               AND series.program = fact.program
        ), recorded AS (
            SELECT DISTINCT series.area_code, series.program
              FROM silver_ref.geography_resolution AS ledger
              JOIN raw_bls.bls_series AS series
                ON series.program = ledger.provider_dataset
             WHERE ledger.provider_source = 'BLS'
        )
        SELECT published.program || ':' || published.area_code
          FROM published
          LEFT JOIN stored
            ON stored.area_code = published.area_code
           AND stored.program = published.program
          LEFT JOIN recorded
            ON recorded.area_code = published.area_code
           AND recorded.program = published.program
         WHERE stored.area_code IS NULL
           AND recorded.area_code IS NULL
        """,
        order_by="1",
    )
    if total == 0:
        # A warehouse that has ingested no BLS revision has nothing to account
        # for, and a pass there would be a pass over an empty set.
        published = _count(
            cursor, "SELECT COUNT(*) FROM silver_bls.observation_revision"
        )
        if published == 0:
            return [RuleOutcome("silver_bls.fact_labor_statistics", "not_applicable")]
    return [
        RuleOutcome(
            "silver_bls.fact_labor_statistics",
            "fail" if unaccounted else "pass",
            observed_count=total,
            expected_count=0,
            evidence=unaccounted[:EVIDENCE_LIMIT],
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
    unmatched, unmatched_total = _offenders(
        cursor,
        """
        SELECT dataset.domain, dataset.series_id
          FROM raw_fred.fred_datasets AS dataset
          LEFT JOIN raw_fred.fred_series AS series
            ON series.series_id = dataset.series_id
         WHERE series.series_id IS NULL
        """,
        # Positions of this select list, like every other rule: the ordering
        # is applied outside the subquery, where `dataset` is not in scope.
        order_by="1, 2",
    )
    outcomes.append(
        RuleOutcome(
            "raw_fred.fred_datasets",
            "fail" if unmatched else "pass",
            observed_count=unmatched_total,
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
    unverified, unverified_total = _offenders(
        cursor,
        """
        SELECT DISTINCT fact.capture_id
          FROM silver_pep.fact_population_estimate AS fact
          LEFT JOIN silver_pep.release_load AS load
            ON load.capture_id = fact.capture_id
         WHERE load.capture_id IS NULL
            OR load.completeness_status <> 'complete'
        """,
        order_by="1",
    )
    return [
        RuleOutcome(
            "silver_pep.release_load",
            "fail" if unverified else "pass",
            observed_count=unverified_total,
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
    unregistered, unregistered_total = _offenders(
        cursor,
        """
        SELECT DISTINCT fact.dataset_code, fact.release_vintage
          FROM silver_pep.fact_population_estimate AS fact
          LEFT JOIN silver_pep.pep_release AS release
            ON release.dataset_code = fact.dataset_code
           AND release.vintage_year = fact.release_vintage
         WHERE release.dataset_code IS NULL
        """,
        order_by="1, 2",
    )
    unloaded, unloaded_total = _offenders(
        cursor,
        """
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
        """,
        order_by="1, 2",
    )
    offenders = unregistered + unloaded
    return [
        RuleOutcome(
            "silver_pep.pep_release",
            "fail" if offenders else "pass",
            # Two offender sets, so the exact count is their sum -- not the
            # length of the bounded evidence this rule concatenates.
            observed_count=unregistered_total + unloaded_total,
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
    offenders, offenders_total = _offenders(
        cursor,
        """
        SELECT capture_id, source_row_index, source_column_index
          FROM silver_pep.observation_revision
         WHERE (BTRIM(COALESCE(value_source, '')) = ANY(%s)
                AND value_status <> 'sentinel')
            OR (value_status = 'sentinel'
                AND BTRIM(COALESCE(value_source, '')) <> ALL(%s))
        """,
        order_by="1, 2, 3",
        params=(sorted(_CENSUS_NULL_SENTINELS), sorted(_CENSUS_NULL_SENTINELS)),
    )
    return [
        RuleOutcome(
            "silver_pep.observation_revision",
            "fail" if offenders else "pass",
            observed_count=offenders_total,
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
    regressions, regressions_total = _offenders(
        cursor,
        """
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
        """,
        order_by="1, 2",
    )
    return [
        RuleOutcome(
            "control.cdc_dataset_release",
            "fail" if regressions else "pass",
            observed_count=regressions_total,
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
    offenders, offenders_total = _offenders(
        cursor,
        """
        SELECT asset_id, release_watermark, source_record_id
          FROM silver_cdc.fact_health_observation
         WHERE value_status IN ('suppressed', 'missing')
           AND value IS NOT NULL
        """,
        order_by="1, 2, 3",
    )
    return [
        RuleOutcome(
            "silver_cdc.fact_health_observation",
            "fail" if offenders else "pass",
            observed_count=offenders_total,
            expected_count=0,
            evidence=offenders[:EVIDENCE_LIMIT],
        )
    ]


def fbi_participation_coverage(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-FBI-002 — no crime observation publishes without its coverage.

    Measured at silver, because that is the only layer where the defect can
    exist. ``gold_fbi.crime_observation`` *inner joins* participation, so an
    observation that loses its coverage row does not appear in the view
    uncovered -- it disappears from the view entirely. Checking the gold view
    against itself would always pass while published rows silently vanished,
    so the rule compares the publishable silver population to what gold
    actually serves.
    """
    del scope
    total = _count(cursor, "SELECT COUNT(*) FROM silver_fbi.fact_crime_observation")
    if total == 0:
        return [
            RuleOutcome("silver_fbi.fact_crime_observation", "not_applicable"),
            RuleOutcome("gold_fbi.crime_observation", "not_applicable"),
        ]

    #: An observation on a published release, at a geography grain gold
    #: serves: exactly the population gold must carry, one for one.
    publishable = """
        FROM silver_fbi.fact_crime_observation AS fact
        JOIN silver_fbi.dim_ucr_dataset_release AS release
          ON release.product_id = fact.product_id
         AND release.release_key = fact.release_key
       WHERE release.status = 'published'
         AND fact.geography_status NOT IN ('ambiguous', 'unsupported')
    """
    uncovered, uncovered_total = _offenders(
        cursor,
        f"""
        SELECT fact.product_id, fact.release_key, fact.source_record_id
        {publishable}
           AND NOT EXISTS (
               SELECT 1
                 FROM silver_fbi.fact_reporting_participation AS coverage
                WHERE coverage.product_id = fact.product_id
                  AND coverage.release_key = fact.release_key
                  AND coverage.subject_type = fact.subject_type
                  AND coverage.subject_code = fact.subject_code
                  AND coverage.period = fact.period
           )
        """,
        order_by="1, 2, 3",
    )
    expected = _count(cursor, f"SELECT COUNT(*) {publishable}")
    served = _count(cursor, "SELECT COUNT(*) FROM gold_fbi.crime_observation")
    return [
        RuleOutcome(
            "silver_fbi.fact_crime_observation",
            "fail" if uncovered else "pass",
            observed_count=uncovered_total,
            expected_count=0,
            evidence=uncovered[:EVIDENCE_LIMIT],
        ),
        RuleOutcome(
            "gold_fbi.crime_observation",
            "pass" if served == expected else "fail",
            observed_count=served,
            expected_count=expected,
            evidence=(
                []
                if served == expected
                else [f"publishable={expected}", f"served={served}"]
            ),
        ),
    ]


def fbi_reported_vs_absent(cursor: Any, scope: Mapping[str, Any]) -> list[RuleOutcome]:
    """DQ-FBI-003 — a published zero is a value; an absent month is NULL."""
    del scope
    total = _count(cursor, "SELECT COUNT(*) FROM silver_fbi.fact_crime_observation")
    if total == 0:
        return [RuleOutcome("silver_fbi.fact_crime_observation", "not_applicable")]
    offenders, offenders_total = _offenders(
        cursor,
        """
        SELECT product_id, release_key, source_record_id, value_status
          FROM silver_fbi.fact_crime_observation
         WHERE (value_status = 'not_reported' AND value IS NOT NULL)
            OR (value_status = 'reported' AND value IS NULL)
        """,
        order_by="1, 2, 3",
    )
    return [
        RuleOutcome(
            "silver_fbi.fact_crime_observation",
            "fail" if offenders else "pass",
            observed_count=offenders_total,
            expected_count=0,
            evidence=offenders[:EVIDENCE_LIMIT],
        )
    ]


#: What each resolution method is allowed to claim about itself (ETL-050).
#:
#: Public because a second reader compares it with the warehouse: the keys
#: are the methods `silver_fbi.agency_geography_relationship.resolution_method`
#: allows and the values are a subset of the classes its `confidence_class`
#: allows, and DB-042 holds both against those CHECK constraints -- so a
#: migration that adds a method without extending this mapping fails at
#: build time rather than the next time such a row exists.
#: `exact` is the registered state-code contract, `reviewed` a crosswalk
#: carrying a reviewer, an evidence URL and a review note, and `derived` a
#: name match that is exact and uniqueness-checked and backed by no review.
#: The county path claimed `reviewed` from a name join for as long as this
#: rule's only reading was a fanout count, and a rule that declares
#: "attribution flows only through exact state codes, reviewed crosswalks, or
#: a label match published as derived" has to be able to see that.
FBI_RESOLUTION_CONFIDENCE: Mapping[str, str] = MappingProxyType(
    {
        "exact_state_code": "exact",
        "reviewed_place_crosswalk": "reviewed",
        "county_label_match": "derived",
    }
)


def _fbi_confidence_claims(cursor: Any) -> RuleOutcome:
    """Every resolved relationship claims the confidence its method earns.

    The allowed pairs are bound as a VALUES list built from
    ``FBI_RESOLUTION_CONFIDENCE``, so adding a resolution method to the
    mapping extends the rule and adding one without extending the mapping
    fails it. A resolved relationship whose method the mapping does not know
    is an offender too: an unreviewed spelling is exactly how the county path
    came to claim `reviewed`.
    """
    pairs = sorted(FBI_RESOLUTION_CONFIDENCE.items())
    values = ", ".join("(%s, %s)" for _ in pairs)
    offenders, offenders_total = _offenders(
        cursor,
        f"""
        SELECT relationship.ori, relationship.relationship_type,
               relationship.resolution_method, relationship.confidence_class
          FROM silver_fbi.agency_geography_relationship AS relationship
          LEFT JOIN (VALUES {values}) AS claim (method, confidence)
                 ON claim.method = relationship.resolution_method
         WHERE relationship.resolution_status = 'resolved'
           AND (claim.confidence IS NULL
                OR claim.confidence <> relationship.confidence_class)
        """,
        order_by="1, 2",
        params=tuple(value for pair in pairs for value in pair),
    )
    return RuleOutcome(
        "silver_fbi.agency_geography_relationship",
        "pass" if offenders_total == 0 else "fail",
        observed_count=offenders_total,
        expected_count=0,
        evidence=offenders,
    )


def fbi_aggregation_boundary(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-FBI-004 — the area filter stays at agency grain, never a total."""
    del scope
    claims = _fbi_confidence_claims(cursor)
    total = _count(
        cursor, "SELECT COUNT(*) FROM gold_fbi.agency_observation_area_filter"
    )
    if total == 0:
        return [
            claims,
            RuleOutcome("gold_fbi.agency_observation_area_filter", "not_applicable"),
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
        claims,
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
        ),
    ]


def nass_slice_ledger(cursor: Any, scope: Mapping[str, Any]) -> list[RuleOutcome]:
    """DQ-NASS-002 — preflight counts, captures, and slice states agree."""
    del scope
    total = _count(cursor, "SELECT COUNT(*) FROM control.usda_nass_slice")
    if total == 0:
        return [RuleOutcome("control.usda_nass_slice", "not_applicable")]
    offenders, offenders_total = _offenders(
        cursor,
        """
        SELECT slice.run_id, slice.slice_key, slice.status
          FROM control.usda_nass_slice AS slice
         WHERE slice.status IN ('preflighted')
            OR (slice.status = 'captured'
                AND slice.captured_row_count <> slice.provider_count)
            OR (slice.status = 'captured' AND slice.data_capture_id IS NULL)
        """,
        order_by="1, 2",
    )
    advanced, advanced_total = _offenders(
        cursor,
        """
        SELECT release.run_id, release.product_id
          FROM control.usda_nass_release AS release
          JOIN control.usda_nass_slice AS slice
            ON slice.run_id = release.run_id
         WHERE release.decision = 'ingest'
           AND slice.status IN ('over_limit', 'partial')
        """,
        order_by="1, 2",
    )
    combined = offenders + ["advanced:" + entry for entry in advanced]
    return [
        RuleOutcome(
            "control.usda_nass_slice",
            "fail" if combined else "pass",
            # Two offender sets, so the exact count is their sum.
            observed_count=offenders_total + advanced_total,
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
    offenders, offenders_total = _offenders(
        cursor,
        """
        SELECT product_id, release_watermark, source_record_id,
               value_source, value_status
          FROM silver_nass.fact_crop_observation
          JOIN UNNEST(%s::TEXT[], %s::TEXT[]) AS mapping(symbol, status)
            ON BTRIM(value_source) = mapping.symbol
         WHERE value_status <> mapping.status
        """,
        order_by="1, 2, 3",
        params=(symbols, statuses),
    )
    return [
        RuleOutcome(
            "silver_nass.fact_crop_observation",
            "fail" if offenders else "pass",
            observed_count=offenders_total,
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
    offenders, offenders_total = _offenders(
        cursor,
        """
        SELECT provider_source, provider_dataset, source_code, status
          FROM silver_ref.geography_resolution
         WHERE (status = 'resolved' AND geo_sk IS NULL)
            OR (status <> 'resolved' AND geo_sk IS NOT NULL)
        """,
        order_by="1, 2, 3",
    )
    return [
        RuleOutcome(
            "silver_ref.geography_resolution",
            "fail" if offenders else "pass",
            observed_count=offenders_total,
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
    dangling, dangling_total = _offenders(
        cursor,
        """
        SELECT registry.source_code, registry.publisher_schema,
               registry.publisher_view
          FROM gold_glossary.publisher_registry AS registry
          LEFT JOIN information_schema.views AS live
            ON live.table_schema = registry.publisher_schema
           AND live.table_name = registry.publisher_view
         WHERE live.table_name IS NULL
        """,
        order_by="1",
    )
    return [
        RuleOutcome(
            "gold_glossary.publisher_registry",
            "fail" if dangling else "pass",
            observed_count=dangling_total,
            expected_count=0,
            evidence=dangling[:EVIDENCE_LIMIT],
        )
    ]


#: Every DQ-004 executor keyed by the rule it measures.


def fred_contract_conformance(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-FRED-007 — the served views carry the published fact, unaltered.

    The conformance direction, which is the one that cannot lag: every row a
    contract view serves must trace to a fact the warehouse published, with
    the metric code derived from that fact's own series and the same value.
    An invented row, an altered value, or a metric code that names no series
    is a number the API presents and the warehouse does not hold -- the worst
    thing this warehouse can do -- and nothing measured it.

    The completeness direction is deliberately *not* measured here. The
    serving layer is rebuilt a calendar year at a time with a commit per
    chunk (DB-041), so a fact published after the last refresh is legitimately
    absent, and DQ-FRED-002 measures that ledger. For the same reason the
    value comparison exempts a fact revised after the refresh watermark:
    ETL-037 advances `ingested_at` only when a row's content changed, so a
    revision inside the window is a served value the next refresh will
    replace, not a value the serving layer invented.

    A source with no `control.serving_refresh_state` row is read strictly --
    `infinity`, so nothing is exempt -- rather than leniently. The chunked
    driver seeds that row before it refreshes anything, so its absence means
    no refresh has run, and served rows that exist anyway are the anomaly
    this rule is for. Defaulting the other way would have made the value
    comparison unreachable: every fact is ingested after `-infinity`.
    """
    del scope
    total = _count(cursor, "SELECT COUNT(*) FROM gold_fred.fact_observation")
    if total == 0:
        return [
            RuleOutcome("gold_fred.fact_observation", "not_applicable"),
            RuleOutcome("gold_fred.v_metric_latest_by_geo", "not_applicable"),
            RuleOutcome("gold_fred.metric_publisher", "not_applicable"),
        ]

    unbacked, unbacked_total = _offenders(
        cursor,
        """
        SELECT served.metric_code, served.observation_date
          FROM gold_fred.fact_observation AS served
          LEFT JOIN control.serving_refresh_state AS refreshed
            ON refreshed.source_code = 'FRED'
         WHERE NOT EXISTS (
                   SELECT 1
                     FROM silver_fred.fact_economic_indicators AS published
                    WHERE 'FRED:' || published.series_id = served.metric_code
                      AND published.observation_date = served.observation_date
                      AND published.is_missing = FALSE
                      AND (
                          published.value IS NOT DISTINCT FROM served.value
                          OR published.ingested_at > COALESCE(
                              refreshed.last_silver_ingested_at,
                              'infinity'::TIMESTAMPTZ
                          )
                      )
               )
        """,
        order_by="1, 2",
    )

    superseded, superseded_total = _offenders(
        cursor,
        """
        SELECT latest.metric_code, latest.observation_date
          FROM gold_fred.v_metric_latest_by_geo AS latest
         WHERE NOT EXISTS (
                   SELECT 1
                     FROM gold_fred.fact_observation AS released
                    WHERE released.metric_code = latest.metric_code
                      AND released.observation_date = latest.observation_date
                      AND released.value IS NOT DISTINCT FROM latest.value
               )
        """,
        order_by="1, 2",
    )

    unpublished, unpublished_total = _offenders(
        cursor,
        """
        SELECT DISTINCT served.metric_code
          FROM gold_fred.fact_observation AS served
         WHERE NOT EXISTS (
                   SELECT 1
                     FROM gold_fred.metric_publisher AS exported
                    WHERE 'FRED:' || exported.source_object_key
                          = served.metric_code
               )
        """,
        order_by="1",
    )

    return [
        RuleOutcome(
            "gold_fred.fact_observation",
            "fail" if unbacked else "pass",
            observed_count=unbacked_total,
            expected_count=0,
            evidence=unbacked[:EVIDENCE_LIMIT],
        ),
        RuleOutcome(
            "gold_fred.v_metric_latest_by_geo",
            "fail" if superseded else "pass",
            observed_count=superseded_total,
            expected_count=0,
            evidence=superseded[:EVIDENCE_LIMIT],
        ),
        RuleOutcome(
            "gold_fred.metric_publisher",
            "fail" if unpublished else "pass",
            observed_count=unpublished_total,
            expected_count=0,
            evidence=unpublished[:EVIDENCE_LIMIT],
        ),
    ]


SOURCE_EXECUTORS: Mapping[str, RuleExecutor] = {
    "DQ-ACS-002": acs_slice_reconciliation,
    "DQ-BLS-002": bls_chunk_reconciliation,
    "DQ-BLS-004": bls_geography_accountability,
    "DQ-FRED-002": fred_slice_reconciliation,
    "DQ-FRED-007": fred_contract_conformance,
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
