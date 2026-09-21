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


def fred_missing_marker_and_series_ownership(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-FRED-003 — a missing FRED value is not a zero, and a series has one owner.

    The first half is an engineering invariant this repository states in
    ``AGENTS.md``: "Never silently convert suppressed, missing, invalid, or
    non-numeric values to zero." FRED publishes its missing marker as ``"."``
    in a numeric field, which is exactly the shape that becomes ``0`` when a
    parser is careless, and a zero is a *claim* -- unemployment was zero that
    month -- rather than an absence.

    The schema refuses one direction of that already:
    ``fact_economic_indicators_published_value_check`` says a row whose
    ``value_status`` is ``valid`` carries a value. It does not refuse the
    other: a row marked ``missing`` that carries a number anyway, which is
    what a zero-filling parser produces. That is what the first arm counts,
    together with the disagreement between ``is_missing`` and
    ``value_status`` -- two columns recording one fact, which can only drift
    apart.

    The second half is about identity. A series appearing under two domains
    has no single owner, so which domain's dashboard is entitled to it is a
    question with two answers; and a series in the fact with no
    ``raw_fred.fred_series`` row is an observation whose units, frequency and
    title nobody can state.
    """
    del scope
    total = _count(cursor, "SELECT COUNT(*) FROM silver_fred.fact_economic_indicators")
    if total == 0:
        return [
            RuleOutcome("silver_fred.fact_economic_indicators", "not_applicable"),
            RuleOutcome("raw_fred.fred_series", "not_applicable"),
        ]

    zeroed, zeroed_total = _offenders(
        cursor,
        """
        SELECT series_id, observation_date::text,
               COALESCE(value::text, '<null>') AS value,
               COALESCE(value_status, '<null>') AS value_status,
               COALESCE(is_missing::text, '<null>') AS is_missing
          FROM silver_fred.fact_economic_indicators
         WHERE (value_status = 'missing' AND value IS NOT NULL)
            OR (is_missing AND value IS NOT NULL)
            OR (is_missing AND value_status = 'valid')
            OR (NOT is_missing AND value_status = 'missing')
        """,
        order_by="1, 2",
    )

    unowned, unowned_total = _offenders(
        cursor,
        """
        SELECT fact.series_id,
               CASE WHEN series.series_id IS NULL
                    THEN 'no-metadata'
                    ELSE 'domains=' || COUNT(DISTINCT fact.domain)::text
               END AS problem
          FROM silver_fred.fact_economic_indicators AS fact
          LEFT JOIN raw_fred.fred_series AS series
            ON series.series_id = fact.series_id
         GROUP BY fact.series_id, series.series_id
        HAVING series.series_id IS NULL
            OR COUNT(DISTINCT fact.domain) > 1
        """,
        order_by="1",
    )

    return [
        RuleOutcome(
            "silver_fred.fact_economic_indicators",
            "fail" if zeroed else "pass",
            observed_count=zeroed_total,
            expected_count=0,
            evidence=zeroed[:EVIDENCE_LIMIT],
        ),
        RuleOutcome(
            "raw_fred.fred_series",
            "fail" if unowned else "pass",
            observed_count=unowned_total,
            expected_count=0,
            evidence=unowned[:EVIDENCE_LIMIT],
        ),
    ]


#: FRED frequency strings whose observation dates land on a period start, and
#: the month-of-year step that period takes. FRED dates a monthly observation
#: on the 1st, a quarterly one on the 1st of January, April, July or October,
#: and so on -- so alignment is checkable for these without inventing a
#: convention.
#:
#: `Daily`, `Weekly` and `Biweekly` are deliberately absent: a weekly series is
#: dated by its own week-ending day, which varies per series, and a daily one
#: by whichever days the provider published. Asserting a rule there would
#: refuse dates FRED legitimately publishes.
_FRED_PERIOD_START_MONTHS: Mapping[str, int] = {
    "Monthly": 1,
    "Quarterly": 3,
    "Semiannual": 6,
    "Annual": 12,
}

#: Frequencies the alignment arm knowingly does not constrain. Listed rather
#: than defaulted, so a frequency string that is in neither map is reported as
#: unrecognised instead of quietly skipped -- a check that silently covered
#: nothing would pass forever.
_FRED_UNCONSTRAINED_FREQUENCIES: frozenset[str] = frozenset(
    {
        "Daily",
        "Weekly",
        "Biweekly",
        "Weekly, Ending Friday",
        "Weekly, Ending Saturday",
        "Weekly, Ending Sunday",
        "Weekly, Ending Monday",
        "Weekly, Ending Tuesday",
        "Weekly, Ending Wednesday",
        "Weekly, Ending Thursday",
    }
)


def fred_observation_dates_within_the_published_range(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-FRED-004 — dates the provider never published, and dates off their grid.

    Two halves, because the rule's summary has two: "observation dates
    validate against each series' frequency and source observation range".

    **The range.** ``raw_fred.fred_series`` carries ``observation_start`` and
    ``observation_end`` -- FRED's own statement of the window a series covers.
    Nothing compared the facts against it, so a date outside that window was
    served exactly like a date inside it, and a reader charting the series
    sees a point the provider does not have. Both bounds are nullable, because
    FRED does not always state them, and a null bound narrows nothing: an
    unstated start cannot make a date too early. Each side is therefore tested
    only where the provider said something.

    **The frequency.** A monthly series is dated on the 1st, a quarterly one
    on the 1st of January, April, July or October, and so on, so alignment is
    checkable for those without inventing a convention.
    ``_FRED_UNCONSTRAINED_FREQUENCIES`` says which it deliberately leaves
    alone and why -- a weekly series is dated by its own week-ending day,
    which varies per series.

    A frequency in neither map is **reported**, not skipped. That is the
    difference between a check that covers what it says and one that quietly
    covers nothing: if FRED renames ``Monthly`` tomorrow, this says so instead
    of passing forever.
    """
    del scope
    total = _count(cursor, "SELECT COUNT(*) FROM silver_fred.fact_economic_indicators")
    if total == 0:
        return [
            RuleOutcome("silver_fred.fact_economic_indicators", "not_applicable"),
            RuleOutcome("gold_fred.dim_fred_series", "not_applicable"),
        ]

    outside, outside_total = _offenders(
        cursor,
        """
        SELECT fact.series_id, fact.observation_date::text,
               COALESCE(series.observation_start::text, '<unstated>') AS starts,
               COALESCE(series.observation_end::text, '<unstated>') AS ends
          FROM silver_fred.fact_economic_indicators AS fact
          JOIN raw_fred.fred_series AS series
            ON series.series_id = fact.series_id
         WHERE (series.observation_start IS NOT NULL
                AND fact.observation_date < series.observation_start)
            OR (series.observation_end IS NOT NULL
                AND fact.observation_date > series.observation_end)
        """,
        order_by="1, 2",
    )

    aligned_frequencies = sorted(_FRED_PERIOD_START_MONTHS)
    misaligned, misaligned_total = _offenders(
        cursor,
        """
        SELECT series.frequency, fact.series_id, fact.observation_date::text,
               CASE
                   WHEN series.frequency = ANY(%s) THEN 'off-period-start'
                   ELSE 'unrecognised-frequency'
               END AS problem
          FROM silver_fred.fact_economic_indicators AS fact
          JOIN gold_fred.dim_fred_series AS series
            ON series.series_id = fact.series_id
         WHERE (
                 series.frequency = ANY(%s)
                 AND (
                   EXTRACT(DAY FROM fact.observation_date) <> 1
                   OR MOD(
                        (EXTRACT(MONTH FROM fact.observation_date)::int - 1),
                        CASE series.frequency
                            WHEN 'Monthly' THEN 1
                            WHEN 'Quarterly' THEN 3
                            WHEN 'Semiannual' THEN 6
                            ELSE 12
                        END
                      ) <> 0
                 )
               )
            OR (
                 series.frequency IS NOT NULL
                 AND NOT (series.frequency = ANY(%s))
                 AND NOT (series.frequency = ANY(%s))
               )
        """,
        order_by="1, 2, 3",
        params=(
            aligned_frequencies,
            aligned_frequencies,
            aligned_frequencies,
            sorted(_FRED_UNCONSTRAINED_FREQUENCIES),
        ),
    )

    return [
        RuleOutcome(
            "silver_fred.fact_economic_indicators",
            "fail" if outside else "pass",
            observed_count=outside_total,
            expected_count=0,
            evidence=outside[:EVIDENCE_LIMIT],
        ),
        RuleOutcome(
            "gold_fred.dim_fred_series",
            "fail" if misaligned else "pass",
            observed_count=misaligned_total,
            expected_count=0,
            evidence=misaligned[:EVIDENCE_LIMIT],
        ),
    ]


def acs_published_row_resolution(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-ACS-004 — every published ACS observation resolves what it names.

    ``gold_census.fact_acs_observation`` is a view:
    ``silver_census.fact_demographics`` **inner joined** to
    ``gold_census.dim_acs_variable`` on ``(dataset, estimate_year,
    variable_code)``. Two consequences follow, and they pull in opposite
    directions.

    **A published row always resolves its variable, and that is the problem.**
    The join *is* the resolution, so ``acs_variable_sk`` can never be
    orphaned -- and a silver row whose variable the dimension does not carry
    is not published at all. It was captured, parsed, stored, and then
    silently declined. Nothing counts it.

    ``DQ-ACS-007`` cannot see it either, which is the reason this rule is
    separate rather than folded into that one. Its *published* side applies
    the same inner join, so such a row is absent from both sides of its
    comparison and its groups agree perfectly while the observation is gone.

    **The geography is published unresolved.** ``fact_demographics.geo_sk`` is
    ``NOT NULL`` with a foreign key into ``silver_ref.dim_geo_entity``, so the
    database guarantees the row resolved *at silver*. The view then publishes
    ``s.geo_id`` -- a different, nullable column with no constraint tying it
    to ``geo_sk``. A published observation can therefore carry no geography,
    or one that disagrees with the entity it actually resolved to, and reach a
    reader as a number nobody can place.

    Both arms are bounded work against small dimensions: the first
    anti-joins silver to the variable dimension, the second reads only rows
    whose ``geo_id`` fails to match an entity.
    """
    del scope
    silver_rows = _count(cursor, "SELECT COUNT(*) FROM silver_census.fact_demographics")
    if silver_rows == 0:
        return [
            RuleOutcome("silver_census.fact_demographics", "not_applicable"),
            RuleOutcome("gold_census.fact_acs_observation", "not_applicable"),
        ]

    # A row the serving view drops: usable variable code, no dimension row.
    dropped, dropped_total = _offenders(
        cursor,
        """
        SELECT s.dataset, s.estimate_year, s.variable_code, COUNT(*) AS rows
          FROM silver_census.fact_demographics AS s
          LEFT JOIN gold_census.dim_acs_variable AS av
            ON av.dataset_code = s.dataset
           AND av.vintage_year = s.estimate_year
           AND av.variable_code = s.variable_code
         WHERE s.variable_code IS NOT NULL
           AND s.variable_code <> ''
           AND av.acs_variable_sk IS NULL
         GROUP BY 1, 2, 3
        """,
        order_by="1, 2, 3",
    )

    # A row the serving view publishes without a geography anybody can place.
    unplaceable, unplaceable_total = _offenders(
        cursor,
        """
        SELECT s.dataset, s.estimate_year,
               COALESCE(s.geo_id, '<null>') AS geo_id, COUNT(*) AS rows
          FROM silver_census.fact_demographics AS s
          JOIN gold_census.dim_acs_variable AS av
            ON av.dataset_code = s.dataset
           AND av.vintage_year = s.estimate_year
           AND av.variable_code = s.variable_code
          LEFT JOIN silver_ref.dim_geo_entity AS entity
            ON entity.geo_id = s.geo_id
         WHERE s.variable_code IS NOT NULL
           AND s.variable_code <> ''
           AND entity.geo_id IS NULL
         GROUP BY 1, 2, 3
        """,
        order_by="1, 2, 3",
    )

    return [
        RuleOutcome(
            "silver_census.fact_demographics",
            "fail" if dropped else "pass",
            observed_count=dropped_total,
            expected_count=0,
            evidence=dropped[:EVIDENCE_LIMIT],
        ),
        RuleOutcome(
            "gold_census.fact_acs_observation",
            "fail" if unplaceable else "pass",
            observed_count=unplaceable_total,
            expected_count=0,
            evidence=unplaceable[:EVIDENCE_LIMIT],
        ),
    ]


def current_geography_projection(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-REF-005 — one current version per entity, and no entity lost.

    Two directions, and they are not equally likely, which is worth stating
    rather than leaving a reader to assume the rule found something.

    **An entity lost is reachable today.** ``dim_geo_current`` reaches its
    attribute choice through an inner join to ``dim_geo_entity_version``, so
    an entity carrying no version row leaves the projection with no trace:
    every consumer of ``dim_geo`` simply never sees that geography, and no
    count anywhere goes red. This is the half that earns the rule.

    **A duplicated entity is not reachable today, and the reason is not the
    one the rule's note gave.** That note credited ``DISTINCT ON`` with making
    the projection one row per entity. It does that for the attribute and
    geometry choices; the third join -- the state lookup, on
    ``state_entity.geo_type = 'state' AND state_entity.state_fips =
    entity.state_fips`` -- is not covered by it, and ``dim_geo_entity``
    declares no uniqueness on that pair. What actually prevents the fan-out is
    two constraints acting together: ``dim_geo_entity_check1`` forces
    ``geo_id = 'state:' || state_fips`` for a state-typed row, and ``geo_id``
    is ``UNIQUE``. So ``state_fips`` is unique among states as a consequence,
    and a second state sharing one cannot be inserted at all.

    That makes the duplicate count a guard on a constraint rather than a live
    defect hunt, and it is kept deliberately: the day somebody relaxes that
    CHECK -- to admit a geography whose id is not derived from its fips, say --
    the fan-out becomes reachable, every geography in the affected state is
    served twice, and this is what notices.

    ``silver_ref.dim_geo`` is a bare projection of ``dim_geo_current`` today,
    so its row count cannot differ -- which is the point of checking it. The
    day someone adds a predicate to one and not the other, two names that
    consumers use interchangeably stop meaning the same thing, and nothing
    else in this repository would notice.
    """
    del scope
    entities = _count(cursor, "SELECT COUNT(*) FROM silver_ref.dim_geo_entity")
    if entities == 0:
        return [
            RuleOutcome("silver_ref.dim_geo_current", "not_applicable"),
            RuleOutcome("silver_ref.dim_geo", "not_applicable"),
        ]

    duplicated, duplicated_total = _offenders(
        cursor,
        """
        SELECT geo_sk, COUNT(*) AS current_rows
          FROM silver_ref.dim_geo_current
         GROUP BY geo_sk
        HAVING COUNT(*) > 1
        """,
        order_by="1",
    )
    dropped, dropped_total = _offenders(
        cursor,
        """
        SELECT entity.geo_sk, entity.geo_id, entity.geo_type
          FROM silver_ref.dim_geo_entity AS entity
          LEFT JOIN silver_ref.dim_geo_current AS current
            ON current.geo_sk = entity.geo_sk
         WHERE current.geo_sk IS NULL
        """,
        order_by="1",
    )

    offenders = ["duplicated:" + str(entry) for entry in duplicated] + [
        "dropped:" + str(entry) for entry in dropped
    ]
    projection = RuleOutcome(
        "silver_ref.dim_geo_current",
        "fail" if offenders else "pass",
        observed_count=duplicated_total + dropped_total,
        expected_count=0,
        evidence=offenders[:EVIDENCE_LIMIT],
    )

    current_rows = _count(cursor, "SELECT COUNT(*) FROM silver_ref.dim_geo_current")
    legacy_rows = _count(cursor, "SELECT COUNT(*) FROM silver_ref.dim_geo")
    compatibility = RuleOutcome(
        "silver_ref.dim_geo",
        "pass" if legacy_rows == current_rows else "fail",
        observed_count=legacy_rows,
        expected_count=current_rows,
        evidence=()
        if legacy_rows == current_rows
        else [f"dim_geo={legacy_rows} dim_geo_current={current_rows}"],
    )
    return [projection, compatibility]


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


#: A served group and a published group are compared by row count and by a
#: sum of value hashes. That detects an invented row, a dropped row, and an
#: altered value -- including a value altered to or from NULL, because the
#: hash is taken over a rendering that distinguishes NULL from every number.
#: It does not name the offending row, only its (metric, vintage) group, and
#: that is the trade being made: the row-level form of this question is a join
#: between two hundred-million-row relations, which is what the first attempt
#: at these rules was and why it timed out (DQ-014 note).
_GROUP_DIGEST = "SUM(hashtext(COALESCE({column}::TEXT, '<null>'))::BIGINT)"


def _conformance_offenders(
    cursor: Any,
    *,
    served_sql: str,
    published_sql: str,
) -> tuple[list[str], int]:
    """Groups the serving layer holds that the warehouse does not back.

    Only the served side is required to be backed. A published group with no
    served group is the *completeness* direction, which these rules
    deliberately do not measure: the serving layer is rebuilt a year at a time
    with a commit per chunk, so a fact published since the last refresh is
    legitimately absent and the source's reconciliation rule measures that
    ledger.
    """
    cursor.execute(
        f"""
        WITH served AS ({served_sql}),
             published AS ({published_sql})
        SELECT served.metric_code, served.vintage
          FROM served
          LEFT JOIN published USING (metric_code, vintage)
         WHERE published.metric_code IS NULL
            OR published.rows <> served.rows
            OR published.digest IS DISTINCT FROM served.digest
         ORDER BY 1, 2
        """
    )
    rows = cursor.fetchall()
    return [f"{code}|{vintage}" for code, vintage in rows], len(rows)


def acs_contract_conformance(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-ACS-007 -- the served views carry the published fact, unaltered.

    Grouped by `(metric_code, vintage)` rather than compared row by row. The
    first implementation of this rule asked, per served row, whether a
    published fact existed with the same value, matching on a composed metric
    code and a computed date. Those are expressions on the silver side, so no
    index served them and every probe scanned the fact table: it passed on the
    fixture warehouse and timed out after fifty minutes against 99,783,997
    real rows.

    Two grouped scans answer the same question. A row the warehouse does not
    hold changes its group's count; a value the serving layer altered changes
    its group's digest; a metric code derived wrongly produces a served group
    with no published counterpart. What is given up is the identity of the
    offending row -- evidence names the group -- and for a BLOCK rule whose
    job is to refuse certification, the group is enough to act on.

    A group whose silver rows moved after the last refresh is exempt, for the
    reason the FRED rule states: ETL-037 advances `ingested_at` only when a
    row's content changed, so a revision inside that window is a served value
    the next refresh will replace rather than one the serving layer invented.
    A source with no `control.serving_refresh_state` row is read strictly.
    """
    del scope
    total = _count(cursor, "SELECT COUNT(*) FROM gold_census.rpt_acs_observations")
    if total == 0:
        return [
            RuleOutcome("gold_census.fact_observation", "not_applicable"),
            RuleOutcome("gold_census.v_metric_latest_by_geo", "not_applicable"),
            RuleOutcome("gold_census.metric_publisher", "not_applicable"),
        ]

    served_sql = f"""
        SELECT metric_code,
               vintage_year AS vintage,
               COUNT(*) AS rows,
               {_GROUP_DIGEST.format(column="estimate_value")} AS digest
          FROM gold_census.rpt_acs_observations
         GROUP BY 1, 2
    """
    published_sql = f"""
        SELECT 'CENSUS_ACS:' || s.dataset || ':' || s.variable_code AS metric_code,
               s.estimate_year AS vintage,
               COUNT(*) AS rows,
               {_GROUP_DIGEST.format(column="s.estimate_value")} AS digest
          FROM silver_census.fact_demographics s
          JOIN gold_census.dim_acs_variable av
            ON av.dataset_code = s.dataset
           AND av.vintage_year = s.estimate_year
           AND av.variable_code = s.variable_code
          LEFT JOIN control.serving_refresh_state r ON r.source_code = 'CENSUS_ACS'
         WHERE s.variable_code IS NOT NULL
           AND s.variable_code <> ''
         GROUP BY 1, 2
        HAVING MAX(s.ingested_at) <= COALESCE(
                   MIN(r.last_silver_ingested_at), 'infinity'::TIMESTAMPTZ
               )
    """
    unbacked, unbacked_total = _conformance_offenders(
        cursor, served_sql=served_sql, published_sql=published_sql
    )

    superseded, superseded_total = _offenders(
        cursor,
        """
        SELECT latest.metric_code, latest.vintage_year
          FROM gold_census.mv_acs_latest AS latest
         WHERE NOT EXISTS (
                   SELECT 1
                     FROM gold_census.rpt_acs_observations AS released
                    WHERE released.geo_id = latest.geo_id
                      AND released.metric_code = latest.metric_code
                      AND released.observation_date = latest.observation_date
                      AND released.estimate_value
                          IS NOT DISTINCT FROM latest.estimate_value
               )
        """,
        order_by="1, 2",
    )

    unpublished, unpublished_total = _offenders(
        cursor,
        """
        SELECT served.metric_code
          FROM (
                SELECT DISTINCT metric_code
                  FROM gold_census.rpt_acs_observations
               ) AS served
         WHERE NOT EXISTS (
                   SELECT 1
                     FROM gold_census.metric_publisher AS exported
                    WHERE 'CENSUS_ACS:' || exported.source_object_key
                          = served.metric_code
               )
        """,
        order_by="1",
    )

    return [
        RuleOutcome(
            "gold_census.fact_observation",
            "fail" if unbacked else "pass",
            observed_count=unbacked_total,
            expected_count=0,
            evidence=unbacked[:EVIDENCE_LIMIT],
        ),
        RuleOutcome(
            "gold_census.v_metric_latest_by_geo",
            "fail" if superseded else "pass",
            observed_count=superseded_total,
            expected_count=0,
            evidence=superseded[:EVIDENCE_LIMIT],
        ),
        RuleOutcome(
            "gold_census.metric_publisher",
            "fail" if unpublished else "pass",
            observed_count=unpublished_total,
            expected_count=0,
            evidence=unpublished[:EVIDENCE_LIMIT],
        ),
    ]


def bls_contract_conformance(
    cursor: Any, scope: Mapping[str, Any]
) -> list[RuleOutcome]:
    """DQ-BLS-007 -- the served views carry the published fact, unaltered.

    The ACS rule's shape against BLS's identity. A BLS metric code is the
    series except where a measure-identified programme publishes one metric
    across every geography it covers, so the published side reads
    `gold_bls.dim_bls_measure` -- the mapping the refresh itself uses rather
    than a restatement of its rule -- and groups by the code that mapping
    produces.
    """
    del scope
    total = _count(cursor, "SELECT COUNT(*) FROM gold_bls.rpt_bls_observations")
    if total == 0:
        return [
            RuleOutcome("gold_bls.fact_observation", "not_applicable"),
            RuleOutcome("gold_bls.v_metric_latest_by_geo", "not_applicable"),
            RuleOutcome("gold_bls.metric_publisher", "not_applicable"),
        ]

    served_sql = f"""
        SELECT metric_code,
               EXTRACT(YEAR FROM observation_date)::INT AS vintage,
               COUNT(*) AS rows,
               {_GROUP_DIGEST.format(column="value")} AS digest
          FROM gold_bls.rpt_bls_observations
         GROUP BY 1, 2
    """
    published_sql = f"""
        SELECT COALESCE('BLS:' || m.metric_key, 'BLS:' || s.series_id) AS metric_code,
               s.year AS vintage,
               COUNT(*) AS rows,
               {_GROUP_DIGEST.format(column="s.value")} AS digest
          FROM silver_bls.fact_labor_statistics s
          LEFT JOIN gold_bls.dim_bls_measure m
                 ON m.program_code = UPPER(s.program)
                AND m.measure_code = s.measure_code
          LEFT JOIN control.serving_refresh_state r ON r.source_code = 'BLS'
         WHERE s.series_id IS NOT NULL
           AND s.series_id <> ''
         GROUP BY 1, 2
        HAVING MAX(s.ingested_at) <= COALESCE(
                   MIN(r.last_silver_ingested_at), 'infinity'::TIMESTAMPTZ
               )
    """
    unbacked, unbacked_total = _conformance_offenders(
        cursor, served_sql=served_sql, published_sql=published_sql
    )

    superseded, superseded_total = _offenders(
        cursor,
        """
        SELECT latest.metric_code, latest.observation_date
          FROM gold_bls.mv_bls_latest AS latest
         WHERE NOT EXISTS (
                   SELECT 1
                     FROM gold_bls.rpt_bls_observations AS released
                    WHERE released.geo_id = latest.geo_id
                      AND released.metric_code = latest.metric_code
                      AND released.observation_date = latest.observation_date
                      AND released.value IS NOT DISTINCT FROM latest.value
               )
        """,
        order_by="1, 2",
    )

    unpublished, unpublished_total = _offenders(
        cursor,
        """
        SELECT served.metric_code
          FROM (
                SELECT DISTINCT metric_code
                  FROM gold_bls.rpt_bls_observations
               ) AS served
         WHERE NOT EXISTS (
                   SELECT 1
                     FROM gold_bls.metric_publisher AS exported
                    WHERE 'BLS:' || exported.source_object_key
                          = served.metric_code
               )
        """,
        order_by="1",
    )

    return [
        RuleOutcome(
            "gold_bls.fact_observation",
            "fail" if unbacked else "pass",
            observed_count=unbacked_total,
            expected_count=0,
            evidence=unbacked[:EVIDENCE_LIMIT],
        ),
        RuleOutcome(
            "gold_bls.v_metric_latest_by_geo",
            "fail" if superseded else "pass",
            observed_count=superseded_total,
            expected_count=0,
            evidence=superseded[:EVIDENCE_LIMIT],
        ),
        RuleOutcome(
            "gold_bls.metric_publisher",
            "fail" if unpublished else "pass",
            observed_count=unpublished_total,
            expected_count=0,
            evidence=unpublished[:EVIDENCE_LIMIT],
        ),
    ]


SOURCE_EXECUTORS: Mapping[str, RuleExecutor] = {
    "DQ-ACS-002": acs_slice_reconciliation,
    "DQ-ACS-004": acs_published_row_resolution,
    "DQ-ACS-007": acs_contract_conformance,
    "DQ-BLS-002": bls_chunk_reconciliation,
    "DQ-BLS-004": bls_geography_accountability,
    "DQ-BLS-007": bls_contract_conformance,
    "DQ-FRED-002": fred_slice_reconciliation,
    "DQ-FRED-003": fred_missing_marker_and_series_ownership,
    "DQ-FRED-004": fred_observation_dates_within_the_published_range,
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
    "DQ-REF-005": current_geography_projection,
    "DQ-GLOSSARY-001": publisher_registry_reconciliation,
}
