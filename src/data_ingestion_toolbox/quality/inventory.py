"""The warehouse quality inventory: every object, its contract, and its rules.

DQ-001 makes the quality scope executable rather than prose. Each relation the
warehouse manifest creates is declared here with its layer, owning source,
grain, upstream lineage, expected-scope method, cadence, and valid empty
behavior. Each quality rule carries a stable id, one severity, one dimension,
and the objects it evaluates. ``validate_inventory`` enforces the acceptance
criterion: every published object has an owner, a declared grain, an
expected-scope method, and at least one deterministic integrity rule.

The catalog is deliberately data, not reflection: reading the live database
would make the expected scope depend on what happens to exist, which is the
failure mode this plan exists to detect.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Mapping

#: Storage layers, ordered from provider payload to public serving surface.
LAYERS: tuple[str, ...] = (
    "raw",
    "control",
    "silver",
    "reference",
    "gold",
    "publisher",
    "serving",
    "glossary",
)

#: Layers whose objects are published contracts: they feed the glossary, the
#: API, or downstream consumers, so each must carry a deterministic rule.
PUBLISHED_LAYERS: frozenset[str] = frozenset(
    {"gold", "publisher", "serving", "glossary"}
)

#: Rule severities and their publication behavior (see the plan's table).
SEVERITIES: tuple[str, ...] = ("BLOCK", "QUARANTINE", "WARN", "INFO")

#: Quality dimensions from the plan; "plausibility" is the only
#: non-deterministic dimension and never satisfies the deterministic-rule
#: requirement on its own.
DIMENSIONS: tuple[str, ...] = (
    "capture_integrity",
    "completeness",
    "conformance",
    "uniqueness",
    "referential_integrity",
    "temporal_integrity",
    "freshness",
    "revision_integrity",
    "reconciliation",
    "plausibility",
)

#: Source owners. ``SHARED`` owns cross-source foundation and glossary objects.
SOURCES: tuple[str, ...] = (
    "SHARED",
    "CENSUS_ACS",
    "BLS",
    "FRED",
    "CENSUS_PEP",
    "CDC",
    "FBI_UCR",
    "USDA_NASS",
)

RULE_ID_PATTERN = re.compile(r"\ADQ-[A-Z]+-\d{3}\Z")

OBJECT_NAME_PATTERN = re.compile(r"\A[a-z_]+\.[a-z_0-9]+\Z")


class QualityInventoryError(ValueError):
    """Raised when the inventory contradicts itself and cannot be trusted."""


@dataclass(frozen=True, slots=True)
class WarehouseObject:
    """One warehouse relation and its declared quality contract."""

    name: str
    layer: str
    source: str
    grain: tuple[str, ...]
    lineage: tuple[str, ...]
    scope_method: str
    cadence: str
    empty_behavior: str

    def __post_init__(self) -> None:
        if not OBJECT_NAME_PATTERN.match(self.name):
            raise QualityInventoryError(
                f"Object name '{self.name}' must be schema-qualified lowercase."
            )
        if self.layer not in LAYERS:
            raise QualityInventoryError(f"{self.name}: unknown layer '{self.layer}'.")
        if self.source not in SOURCES:
            raise QualityInventoryError(f"{self.name}: unknown source '{self.source}'.")
        if not self.grain:
            raise QualityInventoryError(
                f"{self.name}: an object without a declared grain cannot be "
                "checked for uniqueness or reconciled across layers."
            )
        if not self.scope_method:
            raise QualityInventoryError(
                f"{self.name}: expected-scope method must be declared."
            )
        if not self.cadence:
            raise QualityInventoryError(f"{self.name}: cadence must be declared.")
        if not self.empty_behavior:
            raise QualityInventoryError(
                f"{self.name}: valid empty behavior must be declared."
            )


@dataclass(frozen=True, slots=True)
class QualityRule:
    """One declared rule: a stable id, one severity, one dimension."""

    rule_id: str
    severity: str
    dimension: str
    summary: str
    objects: tuple[str, ...]

    def __post_init__(self) -> None:
        if not RULE_ID_PATTERN.match(self.rule_id):
            raise QualityInventoryError(
                f"Rule id '{self.rule_id}' must match DQ-<GROUP>-<NNN>."
            )
        if self.severity not in SEVERITIES:
            raise QualityInventoryError(
                f"{self.rule_id}: unknown severity '{self.severity}'."
            )
        if self.dimension not in DIMENSIONS:
            raise QualityInventoryError(
                f"{self.rule_id}: unknown dimension '{self.dimension}'."
            )
        if not self.summary:
            raise QualityInventoryError(f"{self.rule_id}: summary is required.")
        if not self.objects:
            raise QualityInventoryError(
                f"{self.rule_id}: a rule must evaluate at least one object."
            )

    @property
    def is_deterministic(self) -> bool:
        """Plausibility rules warn on judgement; every other dimension is exact."""
        return self.dimension != "plausibility"


def _obj(
    name: str,
    layer: str,
    source: str,
    grain: str,
    lineage: str = "",
    scope_method: str = "",
    cadence: str = "",
    empty_behavior: str = "",
) -> WarehouseObject:
    """Compact constructor: comma-separated grain and lineage strings."""
    return WarehouseObject(
        name=name,
        layer=layer,
        source=source,
        grain=tuple(part.strip() for part in grain.split(",") if part.strip()),
        lineage=tuple(part.strip() for part in lineage.split(",") if part.strip()),
        scope_method=scope_method,
        cadence=cadence,
        empty_behavior=empty_behavior,
    )


# ---------------------------------------------------------------------------
# Shared capture/control foundation.
# ---------------------------------------------------------------------------

_FOUNDATION = "sql/migrations/001_raw_capture_control_foundation.sql"

_SHARED_OBJECTS: tuple[WarehouseObject, ...] = (
    _obj(
        "raw_capture.payload_blob",
        "raw",
        "SHARED",
        grain="payload_checksum",
        scope_method="content-addressed store; one row per distinct payload",
        cadence="per ingestion run",
        empty_behavior="empty only before the first capture of any source",
    ),
    _obj(
        "raw_capture.response_capture",
        "raw",
        "SHARED",
        grain="capture_id",
        lineage="raw_capture.payload_blob, control.ingestion_request",
        scope_method="one row per accepted provider response",
        cadence="per ingestion run",
        empty_behavior="empty only before the first capture of any source",
    ),
    _obj(
        "control.ingestion_run",
        "control",
        "SHARED",
        grain="run_id",
        scope_method="one row per orchestrated run",
        cadence="per ingestion run",
        empty_behavior="empty only on a fresh bootstrap",
    ),
    _obj(
        "control.ingestion_request",
        "control",
        "SHARED",
        grain="request_id",
        lineage="control.ingestion_run",
        scope_method="one row per planned provider request",
        cadence="per ingestion run",
        empty_behavior="empty only on a fresh bootstrap",
    ),
    _obj(
        "control.capture_quarantine",
        "control",
        "SHARED",
        grain="quarantine_id",
        lineage="control.ingestion_request",
        scope_method="one row per rejected capture; populated only on failure",
        cadence="per ingestion run",
        empty_behavior="empty when every capture was accepted",
    ),
    _obj(
        "control.schema_migration_state",
        "control",
        "SHARED",
        grain="component_name",
        scope_method="one row per manifest component",
        cadence="per bootstrap or migration",
        empty_behavior="never empty after bootstrap",
    ),
    _obj(
        "control.serving_refresh_state",
        "control",
        "SHARED",
        grain="source_code",
        scope_method="one row per source with serving refresh",
        cadence="per publication",
        empty_behavior="empty before the first serving refresh",
    ),
    _obj(
        "control.serving_refresh_chunk_state",
        "control",
        "SHARED",
        grain="source_code, chunk_start, chunk_end",
        lineage="control.serving_refresh_state",
        scope_method="one row per refresh chunk",
        cadence="per publication",
        empty_behavior="empty before the first chunked refresh",
    ),
    _obj(
        "control.publisher_ready_event",
        "control",
        "SHARED",
        grain="event_id",
        scope_method="one row per source publication event; unique per "
        "(source_code, publisher_contract_version, source_watermark)",
        cadence="per publication",
        empty_behavior="empty before the first publication",
    ),
    _obj(
        "control.data_quality_run",
        "control",
        "SHARED",
        grain="quality_run_id",
        lineage="control.ingestion_run, control.publisher_ready_event",
        scope_method="one row per quality assessment execution",
        cadence="per inline, scheduled, release, or manual assessment",
        empty_behavior="empty only before the first assessment",
    ),
    _obj(
        "control.data_quality_result",
        "control",
        "SHARED",
        grain="result_id; unique per (quality_run_id, rule_id, object_name, "
        "partition_key)",
        lineage="control.data_quality_run, raw_capture.response_capture",
        scope_method="one row per rule and evaluated object/partition",
        cadence="per quality assessment",
        empty_behavior="empty only before the first assessment",
    ),
)

# ---------------------------------------------------------------------------
# Shared geography reference and time dimension.
# ---------------------------------------------------------------------------

_REFERENCE_OBJECTS: tuple[WarehouseObject, ...] = (
    _obj(
        "silver_ref.dim_geo_type",
        "reference",
        "SHARED",
        grain="geo_type",
        scope_method="reviewed geography-type vocabulary",
        cadence="per geography reference load",
        empty_behavior="never empty after bootstrap",
    ),
    _obj(
        "silver_ref.dim_geo_entity",
        "reference",
        "SHARED",
        grain="geo_sk",
        lineage="silver_ref.dim_geo_type",
        scope_method="TIGER/reference load of configured geography levels",
        cadence="per geography vintage",
        empty_behavior="empty only before the first reference load",
    ),
    _obj(
        "silver_ref.dim_geo_entity_version",
        "reference",
        "SHARED",
        grain="geo_sk, geography_vintage, attribute_checksum",
        lineage="silver_ref.dim_geo_entity",
        scope_method="one row per entity attribute version",
        cadence="per geography vintage",
        empty_behavior="empty only before the first reference load",
    ),
    _obj(
        "silver_ref.dim_geo_geometry_version",
        "reference",
        "SHARED",
        grain="geo_sk, boundary_vintage, geometry_source, resolution, geometry_checksum",
        lineage="silver_ref.dim_geo_entity",
        scope_method="one row per boundary geometry version",
        cadence="per geography vintage",
        empty_behavior="empty only before the first reference load",
    ),
    _obj(
        "silver_ref.bridge_geo_relationship_version",
        "reference",
        "SHARED",
        grain="parent_geo_sk, related_geo_sk, relationship_type, geography_vintage",
        lineage="silver_ref.dim_geo_entity",
        scope_method="derived hierarchy and overlap relationships per vintage",
        cadence="per geography vintage",
        empty_behavior="empty only before the first reference load",
    ),
    _obj(
        "silver_ref.geography_resolution",
        "reference",
        "SHARED",
        grain="provider_source, provider_dataset, source_geo_type, source_code, source_vintage",
        lineage="silver_ref.dim_geo_entity",
        scope_method="one row per provider geography mapping decision",
        cadence="per source ingestion",
        empty_behavior="empty before the first source resolution",
    ),
    _obj(
        "silver_ref.dim_geo",
        "reference",
        "SHARED",
        grain="geo_sk (projection of dim_geo_entity and versions)",
        lineage="silver_ref.dim_geo_entity, silver_ref.dim_geo_entity_version",
        scope_method="view over the entity and version tables",
        cadence="per geography vintage",
        empty_behavior="empty only before the first reference load",
    ),
    _obj(
        "silver_ref.dim_geo_current",
        "reference",
        "SHARED",
        grain="geo_sk (current version per entity)",
        lineage="silver_ref.dim_geo",
        scope_method="view selecting the current version per entity",
        cadence="per geography vintage",
        empty_behavior="empty only before the first reference load",
    ),
    _obj(
        "silver_ref.dim_time",
        "reference",
        "SHARED",
        grain="time_sk; unique per calendar date",
        scope_method="generated calendar covering configured observation range",
        cadence="static after bootstrap",
        empty_behavior="never empty after bootstrap",
    ),
)

# ---------------------------------------------------------------------------
# Glossary and cross-source serving compatibility.
# ---------------------------------------------------------------------------

_GLOSSARY_OBJECTS: tuple[WarehouseObject, ...] = (
    _obj(
        "gold_glossary.publisher_registry",
        "glossary",
        "SHARED",
        grain="source_code",
        scope_method="one row per harvested source publisher",
        cadence="per glossary harvest",
        empty_behavior="empty only before the first harvest",
    ),
    _obj(
        "gold_glossary.publisher_harvest_state",
        "glossary",
        "SHARED",
        grain="source_code",
        lineage="gold_glossary.publisher_registry",
        scope_method="one row per source harvest watermark",
        cadence="per glossary harvest",
        empty_behavior="empty only before the first harvest",
    ),
    _obj(
        "gold_glossary.dim_source_system",
        "glossary",
        "SHARED",
        grain="source_system_sk; unique per source_code",
        lineage="gold_glossary.publisher_registry",
        scope_method="one row per registered source",
        cadence="per glossary harvest",
        empty_behavior="empty only before the first harvest",
    ),
    _obj(
        "gold_glossary.dim_metric_catalog",
        "glossary",
        "SHARED",
        grain="metric_catalog_sk; unique per metric_code and per "
        "(source_code, source_object_type, source_object_key)",
        lineage="gold_glossary.dim_source_system",
        scope_method="harvested from each source's measure_export",
        cadence="per glossary harvest",
        empty_behavior="empty only before the first harvest",
    ),
    _obj(
        "gold_glossary.dim_geo_latest",
        "glossary",
        "SHARED",
        grain="geo_id",
        lineage="silver_ref.dim_geo_current",
        scope_method="latest resolved geography per geo_id",
        cadence="per geography vintage",
        empty_behavior="empty only before the first reference load",
    ),
    _obj(
        "gold_glossary.dim_metric",
        "glossary",
        "SHARED",
        grain="metric_code (projection of dim_metric_catalog)",
        lineage="gold_glossary.dim_metric_catalog",
        scope_method="view over the metric catalog",
        cadence="per glossary harvest",
        empty_behavior="empty only before the first harvest",
    ),
    _obj(
        "gold_glossary.dim_geography",
        "glossary",
        "SHARED",
        grain="geo_id (projection of dim_geo_latest)",
        lineage="gold_glossary.dim_geo_latest",
        scope_method="view over the latest geography dimension",
        cadence="per geography vintage",
        empty_behavior="empty only before the first reference load",
    ),
)

_LEGACY_SERVING_OBJECTS: tuple[WarehouseObject, ...] = tuple(
    _obj(
        f"gold.{view}",
        "serving",
        "SHARED",
        grain=grain,
        lineage=lineage,
        scope_method="legacy compatibility view over the glossary contract",
        cadence="follows its underlying contract objects",
        empty_behavior="empty only when the underlying contract is empty",
    )
    for view, grain, lineage in (
        (
            "dim_source_system",
            "source_system_sk (projection)",
            "gold_glossary.dim_source_system",
        ),
        (
            "dim_metric_catalog",
            "metric_catalog_sk (projection)",
            "gold_glossary.dim_metric_catalog",
        ),
        ("dim_metric", "metric_code (projection)", "gold_glossary.dim_metric"),
        ("dim_geo_latest", "geo_id (projection)", "gold_glossary.dim_geo_latest"),
        ("dim_geography", "geo_id (projection)", "gold_glossary.dim_geography"),
        (
            "fact_observation",
            "metric_code, geo_id, period (union of source facts)",
            "gold_census.fact_observation, gold_bls.fact_observation, "
            "gold_fred.fact_observation",
        ),
        (
            "v_metric_latest_by_geo",
            "metric_code, geo_id (union of source latest views)",
            "gold_census.v_metric_latest_by_geo, gold_bls.v_metric_latest_by_geo, "
            "gold_fred.v_metric_latest_by_geo",
        ),
        (
            "v_metric_timeseries_by_geo",
            "metric_code, geo_id, period (union of source timeseries views)",
            "gold_census.v_metric_timeseries_by_geo, "
            "gold_bls.v_metric_timeseries_by_geo, "
            "gold_fred.v_metric_timeseries_by_geo",
        ),
        (
            "rpt_observation_dashboard",
            "metric_code, geo_id, period (projection)",
            "gold.fact_observation",
        ),
        (
            "mv_latest_dashboard",
            "metric_code, geo_id (projection)",
            "gold.v_metric_latest_by_geo",
        ),
    )
)

# ---------------------------------------------------------------------------
# Census ACS.
# ---------------------------------------------------------------------------

_ACS_OBJECTS: tuple[WarehouseObject, ...] = (
    _obj(
        "control.acs_ingestion_slices",
        "control",
        "CENSUS_ACS",
        grain="dataset, year, table_id, geo_level, state_fips (NULL-distinct)",
        lineage="control.ingestion_run",
        scope_method="configured dataset-years and geography slices",
        cadence="per ACS ingestion run",
        empty_behavior="empty only before the first ACS run",
    ),
    _obj(
        "raw_census.acs_datasets",
        "raw",
        "CENSUS_ACS",
        grain="dataset, year",
        scope_method="synchronized provider dataset metadata",
        cadence="per metadata sync",
        empty_behavior="empty only before the first metadata sync",
    ),
    _obj(
        "raw_census.acs_tables",
        "raw",
        "CENSUS_ACS",
        grain="dataset, table_id",
        lineage="raw_census.acs_datasets",
        scope_method="synchronized provider table metadata",
        cadence="per metadata sync",
        empty_behavior="empty only before the first metadata sync",
    ),
    _obj(
        "raw_census.acs_variables",
        "raw",
        "CENSUS_ACS",
        grain="dataset, year, variable_name",
        lineage="raw_census.acs_tables",
        scope_method="synchronized provider variable metadata",
        cadence="per metadata sync",
        empty_behavior="empty only before the first metadata sync",
    ),
    _obj(
        "silver_census.observation_revision",
        "silver",
        "CENSUS_ACS",
        grain="capture_id, source_row_index, source_column_index",
        lineage="raw_capture.response_capture",
        scope_method="deterministic replay of accepted captures",
        cadence="per ACS ingestion run",
        empty_behavior="empty only before the first ACS capture",
    ),
    _obj(
        "silver_census.fact_demographics",
        "silver",
        "CENSUS_ACS",
        grain="dataset, table_id, variable_code, geo_id, estimate_year",
        lineage="silver_census.observation_revision, silver_ref.geography_resolution",
        scope_method="configured dataset-years, variables, and geography slices",
        cadence="per ACS ingestion run",
        empty_behavior="source-confirmed ACS1 geographic absence is valid emptiness",
    ),
    _obj(
        "gold_census.dim_acs_table",
        "gold",
        "CENSUS_ACS",
        grain="acs_table_sk; unique per (dataset_code, vintage_year, table_id)",
        lineage="raw_census.acs_tables",
        scope_method="synchronized provider table metadata",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_census.dim_acs_variable",
        "gold",
        "CENSUS_ACS",
        grain="acs_variable_sk; unique per (dataset_code, vintage_year, variable_code)",
        lineage="raw_census.acs_variables",
        scope_method="synchronized provider variable metadata",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_census.fact_acs_observation",
        "gold",
        "CENSUS_ACS",
        grain="dataset_code, vintage_year, variable_code, geo_id",
        lineage="silver_census.fact_demographics, gold_census.dim_acs_variable",
        scope_method="published subset of the configured silver scope",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_census.rpt_acs_observations",
        "serving",
        "CENSUS_ACS",
        grain="dataset_code, vintage_year, variable_code, geo_id (unique index)",
        lineage="gold_census.fact_acs_observation",
        scope_method="projection of the published fact",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_census.mv_acs_latest",
        "serving",
        "CENSUS_ACS",
        grain="variable_code, geo_id (latest vintage; unique index)",
        lineage="gold_census.rpt_acs_observations",
        scope_method="latest-vintage projection",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_census.fact_observation",
        "serving",
        "CENSUS_ACS",
        grain="metric_code, geo_id, period (contract projection)",
        lineage="gold_census.fact_acs_observation",
        scope_method="provider-neutral contract view",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_census.v_metric_latest_by_geo",
        "serving",
        "CENSUS_ACS",
        grain="metric_code, geo_id (contract projection)",
        lineage="gold_census.mv_acs_latest",
        scope_method="provider-neutral contract view",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_census.v_metric_timeseries_by_geo",
        "serving",
        "CENSUS_ACS",
        grain="metric_code, geo_id, period (contract projection)",
        lineage="gold_census.fact_observation",
        scope_method="provider-neutral contract view",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_census.metric_publisher",
        "publisher",
        "CENSUS_ACS",
        grain="metric_code (one row per published ACS metric)",
        lineage="gold_census.dim_acs_variable",
        scope_method="publisher view harvested into the glossary",
        cadence="per glossary harvest",
        empty_behavior="empty only before the first publication",
    ),
)

# ---------------------------------------------------------------------------
# BLS.
# ---------------------------------------------------------------------------

_BLS_OBJECTS: tuple[WarehouseObject, ...] = (
    _obj(
        "control.bls_ingestion_slices",
        "control",
        "BLS",
        grain="program, series_group, year_start, year_end (unique index)",
        lineage="control.ingestion_run",
        scope_method="configured programs and request-sized series/year chunks",
        cadence="per BLS ingestion run",
        empty_behavior="empty only before the first BLS run",
    ),
    _obj(
        "raw_bls.bls_datasets",
        "raw",
        "BLS",
        grain="program, year",
        scope_method="synchronized provider program metadata",
        cadence="per metadata sync",
        empty_behavior="empty only before the first metadata sync",
    ),
    _obj(
        "raw_bls.bls_series",
        "raw",
        "BLS",
        grain="program, series_id",
        lineage="raw_bls.bls_datasets",
        scope_method="synchronized and curated series metadata",
        cadence="per metadata sync",
        empty_behavior="empty only before the first metadata sync",
    ),
    _obj(
        "silver_bls.observation_revision",
        "silver",
        "BLS",
        grain="capture_id, observation_index",
        lineage="raw_capture.response_capture",
        scope_method="deterministic replay of accepted captures",
        cadence="per BLS ingestion run",
        empty_behavior="provider 'No Data Available' tied to the exact request "
        "is valid emptiness",
    ),
    _obj(
        "silver_bls.fact_labor_statistics",
        "silver",
        "BLS",
        grain="series_id, period_date",
        lineage="silver_bls.observation_revision, silver_ref.geography_resolution",
        scope_method="curated series and published observation ranges",
        cadence="per BLS ingestion run",
        empty_behavior="series-specific published range bounds expected periods",
    ),
    _obj(
        "gold_bls.dim_bls_survey",
        "gold",
        "BLS",
        grain="bls_survey_sk; unique per program_code",
        lineage="raw_bls.bls_datasets",
        scope_method="configured program list",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_bls.dim_bls_series",
        "gold",
        "BLS",
        grain="bls_series_sk; unique per series_id",
        lineage="raw_bls.bls_series",
        scope_method="curated series metadata",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_bls.fact_bls_observation",
        "gold",
        "BLS",
        grain="series_id, period_date",
        lineage="silver_bls.fact_labor_statistics, gold_bls.dim_bls_series",
        scope_method="published subset of the curated silver scope",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_bls.rpt_bls_observations",
        "serving",
        "BLS",
        grain="series_id, period_date (unique index)",
        lineage="gold_bls.fact_bls_observation",
        scope_method="projection of the published fact",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_bls.mv_bls_latest",
        "serving",
        "BLS",
        grain="series_id (latest period; unique index)",
        lineage="gold_bls.rpt_bls_observations",
        scope_method="latest-period projection",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_bls.fact_observation",
        "serving",
        "BLS",
        grain="metric_code, geo_id, period (contract projection)",
        lineage="gold_bls.fact_bls_observation",
        scope_method="provider-neutral contract view",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_bls.v_metric_latest_by_geo",
        "serving",
        "BLS",
        grain="metric_code, geo_id (contract projection)",
        lineage="gold_bls.mv_bls_latest",
        scope_method="provider-neutral contract view",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_bls.v_metric_timeseries_by_geo",
        "serving",
        "BLS",
        grain="metric_code, geo_id, period (contract projection)",
        lineage="gold_bls.fact_observation",
        scope_method="provider-neutral contract view",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_bls.metric_publisher",
        "publisher",
        "BLS",
        grain="metric_code (one row per published BLS metric)",
        lineage="gold_bls.dim_bls_series",
        scope_method="publisher view harvested into the glossary",
        cadence="per glossary harvest",
        empty_behavior="empty only before the first publication",
    ),
)

# ---------------------------------------------------------------------------
# FRED.
# ---------------------------------------------------------------------------

_FRED_OBJECTS: tuple[WarehouseObject, ...] = (
    _obj(
        "control.fred_ingestion_slices",
        "control",
        "FRED",
        grain="domain, date_start, date_end",
        lineage="control.ingestion_run",
        scope_method="configured domains and requested date ranges",
        cadence="per FRED ingestion run",
        empty_behavior="empty only before the first FRED run",
    ),
    _obj(
        "raw_fred.fred_datasets",
        "raw",
        "FRED",
        grain="domain, series_id",
        scope_method="configured series per domain",
        cadence="per metadata sync",
        empty_behavior="empty only before the first metadata sync",
    ),
    _obj(
        "raw_fred.fred_series",
        "raw",
        "FRED",
        grain="series_id",
        lineage="raw_fred.fred_datasets",
        scope_method="synchronized provider series metadata",
        cadence="per metadata sync",
        empty_behavior="empty only before the first metadata sync",
    ),
    _obj(
        "silver_fred.observation_revision",
        "silver",
        "FRED",
        grain="capture_id, observation_index",
        lineage="raw_capture.response_capture",
        scope_method="deterministic replay of accepted captures",
        cadence="per FRED ingestion run",
        empty_behavior="the FRED missing marker is data, never numeric zero",
    ),
    _obj(
        "silver_fred.fact_economic_indicators",
        "silver",
        "FRED",
        grain="series_id, observation_date",
        lineage="silver_fred.observation_revision",
        scope_method="configured series and requested date ranges",
        cadence="per FRED ingestion run",
        empty_behavior="series frequency bounds expected observation dates",
    ),
    _obj(
        "gold_fred.dim_fred_series",
        "gold",
        "FRED",
        grain="fred_series_sk; unique per series_id",
        lineage="raw_fred.fred_series",
        scope_method="configured series list",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_fred.fact_fred_observation",
        "gold",
        "FRED",
        grain="series_id, observation_date",
        lineage="silver_fred.fact_economic_indicators, gold_fred.dim_fred_series",
        scope_method="published subset of the configured silver scope",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_fred.rpt_fred_observations",
        "serving",
        "FRED",
        grain="series_id, observation_date (unique index)",
        lineage="gold_fred.fact_fred_observation",
        scope_method="projection of the published fact",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_fred.mv_fred_latest",
        "serving",
        "FRED",
        grain="series_id (latest observation; unique index)",
        lineage="gold_fred.rpt_fred_observations",
        scope_method="latest-observation projection",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_fred.fact_observation",
        "serving",
        "FRED",
        grain="metric_code, geo_id, period (contract projection)",
        lineage="gold_fred.fact_fred_observation",
        scope_method="provider-neutral contract view",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_fred.v_metric_latest_by_geo",
        "serving",
        "FRED",
        grain="metric_code, geo_id (contract projection)",
        lineage="gold_fred.mv_fred_latest",
        scope_method="provider-neutral contract view",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_fred.v_metric_timeseries_by_geo",
        "serving",
        "FRED",
        grain="metric_code, geo_id, period (contract projection)",
        lineage="gold_fred.fact_observation",
        scope_method="provider-neutral contract view",
        cadence="per publication",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_fred.metric_publisher",
        "publisher",
        "FRED",
        grain="metric_code (one row per published FRED metric)",
        lineage="gold_fred.dim_fred_series",
        scope_method="publisher view harvested into the glossary",
        cadence="per glossary harvest",
        empty_behavior="empty only before the first publication",
    ),
)

# ---------------------------------------------------------------------------
# Census PEP.
# ---------------------------------------------------------------------------

_PEP_OBJECTS: tuple[WarehouseObject, ...] = (
    _obj(
        "silver_pep.pep_dataset",
        "silver",
        "CENSUS_PEP",
        grain="dataset_code",
        scope_method="code registry materialized by migration 009",
        cadence="per registry change",
        empty_behavior="never empty after bootstrap",
    ),
    _obj(
        "silver_pep.pep_release",
        "silver",
        "CENSUS_PEP",
        grain="dataset_code, vintage_year; unique per product_code",
        lineage="silver_pep.pep_dataset",
        scope_method="registered vintages in the code registry",
        cadence="per registry change",
        empty_behavior="never empty after bootstrap",
    ),
    _obj(
        "silver_pep.release_load",
        "silver",
        "CENSUS_PEP",
        grain="capture_id",
        lineage="raw_capture.response_capture, silver_pep.pep_release",
        scope_method="one completeness verdict per capture",
        cadence="per PEP ingestion run",
        empty_behavior="empty only before the first PEP capture",
    ),
    _obj(
        "silver_pep.observation_revision",
        "silver",
        "CENSUS_PEP",
        grain="capture_id, source_row_index, source_column_index",
        lineage="raw_capture.response_capture",
        scope_method="deterministic replay of accepted captures",
        cadence="per PEP ingestion run",
        empty_behavior="empty only before the first PEP capture",
    ),
    _obj(
        "silver_pep.dim_measure",
        "silver",
        "CENSUS_PEP",
        grain="metric_code",
        scope_method="registered variable families",
        cadence="per registry change",
        empty_behavior="never empty after bootstrap",
    ),
    _obj(
        "silver_pep.fact_population_estimate",
        "silver",
        "CENSUS_PEP",
        grain="capture_id, source_row_index, source_column_index; natural key "
        "(dataset_code, release_vintage, metric_code, geo_id, observation_year)",
        lineage="silver_pep.observation_revision, silver_pep.release_load, "
        "silver_ref.geography_resolution",
        scope_method="registered datasets, vintages, and geography levels",
        cadence="per PEP ingestion run",
        empty_behavior="registry-excluded geography levels are valid emptiness",
    ),
    _obj(
        "gold_pep.population_estimate_revision",
        "gold",
        "CENSUS_PEP",
        grain="dataset_code, release_vintage, metric_code, geo_id, "
        "observation_year (current revision per vintage)",
        lineage="silver_pep.fact_population_estimate",
        scope_method="complete, resolved loads of registered vintages",
        cadence="per publication",
        empty_behavior="empty only before the first complete load",
    ),
    _obj(
        "gold_pep.population_estimate_latest",
        "gold",
        "CENSUS_PEP",
        grain="dataset_code, metric_code, geo_id, observation_year (latest vintage)",
        lineage="gold_pep.population_estimate_revision",
        scope_method="latest-vintage projection",
        cadence="per publication",
        empty_behavior="empty only before the first complete load",
    ),
    _obj(
        "gold_pep.population_change",
        "gold",
        "CENSUS_PEP",
        grain="dataset_code, metric_code, geo_id, observation_year "
        "(components of change only)",
        lineage="gold_pep.population_estimate_latest",
        scope_method="component metrics NPOPCHG, NATURALCHG, NETMIG",
        cadence="per publication",
        empty_behavior="empty only before the first complete load",
    ),
    _obj(
        "gold_pep.rpt_pep_observations",
        "serving",
        "CENSUS_PEP",
        grain="dataset_code, metric_code, geo_id, observation_year (projection)",
        lineage="gold_pep.population_estimate_latest",
        scope_method="projection of the latest publication",
        cadence="per publication",
        empty_behavior="empty only before the first complete load",
    ),
    _obj(
        "gold_pep.mv_pep_latest",
        "serving",
        "CENSUS_PEP",
        grain="metric_code, geo_id (latest observation year; unique index)",
        lineage="gold_pep.rpt_pep_observations",
        scope_method="latest-year projection",
        cadence="per publication",
        empty_behavior="empty only before the first complete load",
    ),
    _obj(
        "gold_pep.measure_export",
        "publisher",
        "CENSUS_PEP",
        grain="metric_code (one row per published PEP metric)",
        lineage="silver_pep.dim_measure",
        scope_method="publisher export harvested into the glossary",
        cadence="per glossary harvest",
        empty_behavior="empty only before the first publication",
    ),
    _obj(
        "gold_pep.metric_publisher",
        "publisher",
        "CENSUS_PEP",
        grain="metric_code (one row per published PEP metric)",
        lineage="gold_pep.measure_export",
        scope_method="publisher view harvested into the glossary",
        cadence="per glossary harvest",
        empty_behavior="empty only before the first publication",
    ),
)

# ---------------------------------------------------------------------------
# CDC.
# ---------------------------------------------------------------------------

_CDC_OBJECTS: tuple[WarehouseObject, ...] = (
    _obj(
        "control.cdc_dataset_release",
        "control",
        "CDC",
        grain="run_id; unique per (asset_id, release_watermark, run_id)",
        lineage="control.ingestion_run",
        scope_method="registered Socrata assets (cdi, places_county)",
        cadence="weekly metadata check",
        empty_behavior="empty only before the first CDC run",
    ),
    _obj(
        "silver_cdc.observation_revision",
        "silver",
        "CDC",
        grain="capture_id, source_row_index",
        lineage="raw_capture.response_capture",
        scope_method="deterministic replay of accepted captures",
        cadence="per CDC ingestion run",
        empty_behavior="empty only before the first CDC capture",
    ),
    _obj(
        "silver_cdc.observation_quarantine",
        "silver",
        "CDC",
        grain="run_id, asset_id, release_watermark, source_row_index, error_code",
        lineage="control.cdc_dataset_release",
        scope_method="one row per rejected source row; populated only on failure",
        cadence="per CDC ingestion run",
        empty_behavior="empty when every row conformed",
    ),
    _obj(
        "silver_cdc.dim_dataset_release",
        "silver",
        "CDC",
        grain="asset_id, release_watermark",
        lineage="control.cdc_dataset_release",
        scope_method="every retained release watermark per asset",
        cadence="per CDC ingestion run",
        empty_behavior="empty only before the first CDC release",
    ),
    _obj(
        "silver_cdc.dim_measure",
        "silver",
        "CDC",
        grain="asset_id, measure_id, value_type_id",
        scope_method="measures observed in retained releases",
        cadence="per CDC ingestion run",
        empty_behavior="empty only before the first CDC release",
    ),
    _obj(
        "silver_cdc.dim_stratum",
        "silver",
        "CDC",
        grain="stratum_id (sha256 of stratification pairs)",
        scope_method="strata observed in retained releases",
        cadence="per CDC ingestion run",
        empty_behavior="empty only before the first CDC release",
    ),
    _obj(
        "silver_cdc.fact_health_observation",
        "silver",
        "CDC",
        grain="asset_id, release_watermark, source_record_id",
        lineage="silver_cdc.observation_revision, silver_cdc.dim_dataset_release, "
        "silver_cdc.dim_measure, silver_cdc.dim_stratum, "
        "silver_ref.geography_resolution",
        scope_method="registered assets and their declared geography levels",
        cadence="per CDC ingestion run",
        empty_behavior="suppressed and missing rows are retained, never dropped",
    ),
    _obj(
        "gold_cdc.health_observation",
        "gold",
        "CDC",
        grain="asset_id, release_watermark, source_record_id (published releases only)",
        lineage="silver_cdc.fact_health_observation",
        scope_method="releases with status published",
        cadence="per publication",
        empty_behavior="empty only before the first published release",
    ),
    _obj(
        "gold_cdc.latest_release_observation",
        "gold",
        "CDC",
        grain="asset_id, source_record_id (latest watermark per asset)",
        lineage="gold_cdc.health_observation",
        scope_method="latest-watermark projection, never a replacement",
        cadence="per publication",
        empty_behavior="empty only before the first published release",
    ),
    _obj(
        "gold_cdc.measure_export",
        "publisher",
        "CDC",
        grain="metric_code (one row per published CDC measure)",
        lineage="silver_cdc.dim_measure",
        scope_method="publisher export harvested into the glossary",
        cadence="per glossary harvest",
        empty_behavior="empty only before the first published release",
    ),
    _obj(
        "gold_cdc.metric_publisher",
        "publisher",
        "CDC",
        grain="metric_code (one row per published CDC measure)",
        lineage="gold_cdc.measure_export",
        scope_method="publisher view harvested into the glossary",
        cadence="per glossary harvest",
        empty_behavior="empty only before the first published release",
    ),
)

# ---------------------------------------------------------------------------
# FBI UCR.
# ---------------------------------------------------------------------------

_FBI_OBJECTS: tuple[WarehouseObject, ...] = (
    _obj(
        "control.fbi_ucr_release",
        "control",
        "FBI_UCR",
        grain="run_id; unique per (product_id, refresh_date, run_id)",
        lineage="control.ingestion_run",
        scope_method="registered products in fbi_ucr/registry.py",
        cadence="weekly release check",
        empty_behavior="empty only before the first FBI run",
    ),
    _obj(
        "silver_fbi.agency_revision",
        "silver",
        "FBI_UCR",
        grain="capture_id, source_row_index",
        lineage="raw_capture.response_capture",
        scope_method="deterministic replay of agency directory captures",
        cadence="per FBI ingestion run",
        empty_behavior="empty only before the first FBI capture",
    ),
    _obj(
        "silver_fbi.observation_revision",
        "silver",
        "FBI_UCR",
        grain="capture_id, source_row_index",
        lineage="raw_capture.response_capture",
        scope_method="deterministic replay of observation captures",
        cadence="per FBI ingestion run",
        empty_behavior="empty only before the first FBI capture",
    ),
    _obj(
        "silver_fbi.participation_revision",
        "silver",
        "FBI_UCR",
        grain="capture_id, source_row_index",
        lineage="raw_capture.response_capture",
        scope_method="deterministic replay of participation captures",
        cadence="per FBI ingestion run",
        empty_behavior="empty only before the first FBI capture",
    ),
    _obj(
        "silver_fbi.slice_quarantine",
        "silver",
        "FBI_UCR",
        grain="run_id, product_id, release_key, slice_key, source_row_index, error_code",
        lineage="control.fbi_ucr_release",
        scope_method="one row per rejected slice row; populated only on failure",
        cadence="per FBI ingestion run",
        empty_behavior="empty when every slice conformed",
    ),
    _obj(
        "silver_fbi.dim_ucr_dataset_release",
        "silver",
        "FBI_UCR",
        grain="product_id, release_key",
        lineage="control.fbi_ucr_release",
        scope_method="every retained provider refresh per product",
        cadence="per FBI ingestion run",
        empty_behavior="empty only before the first FBI release",
    ),
    _obj(
        "silver_fbi.dim_offense_measure",
        "silver",
        "FBI_UCR",
        grain="product_id, measure_id",
        scope_method="registered offenses and measure forms",
        cadence="per registry change",
        empty_behavior="never empty after the first release",
    ),
    _obj(
        "silver_fbi.dim_agency",
        "silver",
        "FBI_UCR",
        grain="ori",
        lineage="silver_fbi.agency_revision",
        scope_method="agency directory of registered reference states",
        cadence="per FBI ingestion run",
        empty_behavior="empty only before the first directory capture",
    ),
    _obj(
        "silver_fbi.dim_agency_version",
        "silver",
        "FBI_UCR",
        grain="ori, release_key",
        lineage="silver_fbi.dim_agency, silver_fbi.dim_ucr_dataset_release",
        scope_method="one agency version per retained release",
        cadence="per FBI ingestion run",
        empty_behavior="empty only before the first directory capture",
    ),
    _obj(
        "silver_fbi.agency_geography_relationship",
        "silver",
        "FBI_UCR",
        grain="ori, relationship_type, source_label, geography_vintage, effective_start",
        lineage="silver_fbi.dim_agency, silver_ref.geography_resolution",
        scope_method="name evidence plus reviewed crosswalks only",
        cadence="per FBI ingestion run",
        empty_behavior="unresolved agencies carry no relationship rows",
    ),
    _obj(
        "silver_fbi.dim_state_code",
        "silver",
        "FBI_UCR",
        grain="state_code",
        scope_method="registered state-code contract",
        cadence="per registry change",
        empty_behavior="never empty after bootstrap",
    ),
    _obj(
        "silver_fbi.reviewed_place_crosswalk",
        "silver",
        "FBI_UCR",
        grain="ori, place_geo_id, effective_start",
        lineage="silver_fbi.dim_agency",
        scope_method="human-reviewed crosswalk entries only",
        cadence="per review",
        empty_behavior="empty until a crosswalk entry is reviewed",
    ),
    _obj(
        "silver_fbi.fact_reporting_participation",
        "silver",
        "FBI_UCR",
        grain="product_id, release_key, subject_type, subject_code, period",
        lineage="silver_fbi.participation_revision, silver_fbi.dim_ucr_dataset_release",
        scope_method="registered products and expected periods",
        cadence="per FBI ingestion run",
        empty_behavior="coverage absence is recorded, never imputed",
    ),
    _obj(
        "silver_fbi.fact_crime_observation",
        "silver",
        "FBI_UCR",
        grain="product_id, release_key, source_record_id",
        lineage="silver_fbi.observation_revision, "
        "silver_fbi.fact_reporting_participation, silver_fbi.dim_offense_measure",
        scope_method="registered products, offenses, states, and agencies",
        cadence="per FBI ingestion run",
        empty_behavior="an absent month is NULL and never a zero",
    ),
    _obj(
        "gold_fbi.crime_observation",
        "gold",
        "FBI_UCR",
        grain="product_id, release_key, source_record_id "
        "(published, unambiguous geography only)",
        lineage="silver_fbi.fact_crime_observation",
        scope_method="published releases excluding ambiguous/unsupported geography",
        cadence="per publication",
        empty_behavior="withheld ambiguous evidence stays queryable in silver",
    ),
    _obj(
        "gold_fbi.reporting_coverage",
        "gold",
        "FBI_UCR",
        grain="product_id, release_key, subject_type, subject_code, period",
        lineage="silver_fbi.fact_reporting_participation",
        scope_method="published releases",
        cadence="per publication",
        empty_behavior="agency subjects publish no coverage percent by design",
    ),
    _obj(
        "gold_fbi.agency_geography",
        "gold",
        "FBI_UCR",
        grain="ori, relationship_type, geography_vintage (published projection)",
        lineage="silver_fbi.agency_geography_relationship",
        scope_method="resolved and reviewed relationships only",
        cadence="per publication",
        empty_behavior="unresolved agencies are absent by design",
    ),
    _obj(
        "gold_fbi.agency_observation_area_filter",
        "gold",
        "FBI_UCR",
        grain="ori, source_record_id (agency grain; never a county/city total)",
        lineage="gold_fbi.crime_observation, gold_fbi.agency_geography",
        scope_method="effective-dated area filter at agency grain",
        cadence="per publication",
        empty_behavior="empty when no agency resolves to the filtered area",
    ),
    _obj(
        "gold_fbi.latest_release_observation",
        "gold",
        "FBI_UCR",
        grain="product_id, source_record_id (latest refresh per product)",
        lineage="gold_fbi.crime_observation",
        scope_method="latest-refresh projection, never a replacement",
        cadence="per publication",
        empty_behavior="empty only before the first published release",
    ),
    _obj(
        "gold_fbi.measure_export",
        "publisher",
        "FBI_UCR",
        grain="metric_code (one row per published FBI measure)",
        lineage="silver_fbi.dim_offense_measure",
        scope_method="publisher export harvested into the glossary",
        cadence="per glossary harvest",
        empty_behavior="empty only before the first published release",
    ),
    _obj(
        "gold_fbi.metric_publisher",
        "publisher",
        "FBI_UCR",
        grain="metric_code (one row per published FBI measure)",
        lineage="gold_fbi.measure_export",
        scope_method="publisher view harvested into the glossary",
        cadence="per glossary harvest",
        empty_behavior="empty only before the first published release",
    ),
)

# ---------------------------------------------------------------------------
# USDA NASS.
# ---------------------------------------------------------------------------

_NASS_OBJECTS: tuple[WarehouseObject, ...] = (
    _obj(
        "control.usda_nass_release",
        "control",
        "USDA_NASS",
        grain="run_id; unique per (product_id, extraction_watermark, run_id)",
        lineage="control.ingestion_run",
        scope_method="registered products in usda_nass/registry.py",
        cadence="business-daily recent window; monthly full sweep",
        empty_behavior="empty only before the first NASS run",
    ),
    _obj(
        "control.usda_nass_slice",
        "control",
        "USDA_NASS",
        grain="run_id, slice_key",
        lineage="control.usda_nass_release",
        scope_method="registered products x aggregation levels x years",
        cadence="per NASS ingestion run",
        empty_behavior="a preflighted empty slice is recorded as status empty",
    ),
    _obj(
        "silver_nass.observation_revision",
        "silver",
        "USDA_NASS",
        grain="capture_id, source_row_index",
        lineage="raw_capture.response_capture",
        scope_method="deterministic replay of accepted captures",
        cadence="per NASS ingestion run",
        empty_behavior="empty only before the first NASS capture",
    ),
    _obj(
        "silver_nass.observation_quarantine",
        "silver",
        "USDA_NASS",
        grain="run_id, product_id, release_watermark, slice_key, "
        "source_row_index, error_code",
        lineage="control.usda_nass_release",
        scope_method="one row per rejected source row; populated only on failure",
        cadence="per NASS ingestion run",
        empty_behavior="empty when every row conformed",
    ),
    _obj(
        "silver_nass.dim_dataset_release",
        "silver",
        "USDA_NASS",
        grain="product_id, release_watermark",
        lineage="control.usda_nass_release",
        scope_method="every retained release watermark per product",
        cadence="per NASS ingestion run",
        empty_behavior="empty only before the first NASS release",
    ),
    _obj(
        "silver_nass.dim_commodity",
        "silver",
        "USDA_NASS",
        grain="commodity_sk; unique per (sector_desc, group_desc, commodity_desc, "
        "class_desc, prodn_practice_desc, util_practice_desc)",
        scope_method="commodities observed in retained releases",
        cadence="per NASS ingestion run",
        empty_behavior="empty only before the first NASS release",
    ),
    _obj(
        "silver_nass.dim_statistic",
        "silver",
        "USDA_NASS",
        grain="statistic_sk; unique per (source_desc, statisticcat_desc, "
        "short_desc, unit_desc, freq_desc)",
        scope_method="statistics observed in retained releases",
        cadence="per NASS ingestion run",
        empty_behavior="empty only before the first NASS release",
    ),
    _obj(
        "silver_nass.dim_domain",
        "silver",
        "USDA_NASS",
        grain="domain_sk; unique per (domain_desc, domaincat_desc)",
        scope_method="domains observed in retained releases",
        cadence="per NASS ingestion run",
        empty_behavior="empty only before the first NASS release",
    ),
    _obj(
        "silver_nass.fact_crop_observation",
        "silver",
        "USDA_NASS",
        grain="product_id, release_watermark, source_record_id "
        "(the complete Quick Stats grain)",
        lineage="silver_nass.observation_revision, silver_nass.dim_dataset_release, "
        "silver_nass.dim_commodity, silver_nass.dim_statistic, "
        "silver_nass.dim_domain, silver_ref.geography_resolution",
        scope_method="registered products, year window, and aggregation levels",
        cadence="per NASS ingestion run",
        empty_behavior="suppressed values are retained with their own status, "
        "never dropped",
    ),
    _obj(
        "gold_nass.crop_observation",
        "gold",
        "USDA_NASS",
        grain="product_id, release_watermark, source_record_id "
        "(published releases only)",
        lineage="silver_nass.fact_crop_observation",
        scope_method="releases with status published",
        cadence="per publication",
        empty_behavior="empty only before the first published release",
    ),
    _obj(
        "gold_nass.crop_series",
        "gold",
        "USDA_NASS",
        grain="series_id (md5 of product, commodity, statistic, domain, "
        "geography, frequency)",
        lineage="gold_nass.crop_observation",
        scope_method="derived series over published observations",
        cadence="per publication",
        empty_behavior="empty only before the first published release",
    ),
    _obj(
        "gold_nass.latest_release_observation",
        "gold",
        "USDA_NASS",
        grain="product_id, source_record_id (latest watermark per product)",
        lineage="gold_nass.crop_observation",
        scope_method="latest-watermark projection, never a replacement",
        cadence="per publication",
        empty_behavior="empty only before the first published release",
    ),
    _obj(
        "gold_nass.measure_export",
        "publisher",
        "USDA_NASS",
        grain="metric_code (one row per published NASS measure)",
        lineage="silver_nass.dim_statistic",
        scope_method="publisher export harvested into the glossary",
        cadence="per glossary harvest",
        empty_behavior="empty only before the first published release",
    ),
    _obj(
        "gold_nass.metric_publisher",
        "publisher",
        "USDA_NASS",
        grain="metric_code (one row per published NASS measure)",
        lineage="gold_nass.measure_export",
        scope_method="publisher view harvested into the glossary",
        cadence="per glossary harvest",
        empty_behavior="empty only before the first published release",
    ),
)

ALL_OBJECTS: tuple[WarehouseObject, ...] = (
    _SHARED_OBJECTS
    + _REFERENCE_OBJECTS
    + _GLOSSARY_OBJECTS
    + _LEGACY_SERVING_OBJECTS
    + _ACS_OBJECTS
    + _BLS_OBJECTS
    + _FRED_OBJECTS
    + _PEP_OBJECTS
    + _CDC_OBJECTS
    + _FBI_OBJECTS
    + _NASS_OBJECTS
)


def _rule(
    rule_id: str,
    severity: str,
    dimension: str,
    summary: str,
    objects: Iterable[str],
) -> QualityRule:
    return QualityRule(
        rule_id=rule_id,
        severity=severity,
        dimension=dimension,
        summary=summary,
        objects=tuple(objects),
    )


def _names(*groups: tuple[WarehouseObject, ...]) -> tuple[str, ...]:
    return tuple(entry.name for group in groups for entry in group)


def _layer_names(layer: str, *groups: tuple[WarehouseObject, ...]) -> tuple[str, ...]:
    return tuple(
        entry.name for group in groups for entry in group if entry.layer == layer
    )


#: Declared rule registry. DQ-001 assigns identities and severities; the
#: implementations land under DQ-002 through DQ-004 and must not renumber ids.
ALL_RULES: tuple[QualityRule, ...] = (
    # -- shared capture/lineage --------------------------------------------
    _rule(
        "DQ-SHARED-001",
        "BLOCK",
        "capture_integrity",
        "Every response_capture payload_checksum verifies against its "
        "immutable payload blob.",
        ("raw_capture.response_capture", "raw_capture.payload_blob"),
    ),
    _rule(
        "DQ-SHARED-002",
        "BLOCK",
        "capture_integrity",
        "Every capture has exactly one valid request/run lineage chain.",
        (
            "raw_capture.response_capture",
            "control.ingestion_request",
            "control.ingestion_run",
        ),
    ),
    _rule(
        "DQ-SHARED-003",
        "QUARANTINE",
        "reconciliation",
        "Successful requests reconcile to captures; empty requests reconcile "
        "to explicit empty outcomes; the remainder is quarantined, never lost.",
        (
            "control.ingestion_run",
            "control.ingestion_request",
            "raw_capture.response_capture",
            "control.capture_quarantine",
        ),
    ),
    _rule(
        "DQ-SHARED-004",
        "BLOCK",
        "completeness",
        "The manifest's schema components are all recorded as applied before "
        "any publication is certified.",
        ("control.schema_migration_state",),
    ),
    _rule(
        "DQ-SHARED-005",
        "BLOCK",
        "revision_integrity",
        "Publisher-ready events are unique per (source, contract version, "
        "watermark) and serving refresh state never precedes its event.",
        (
            "control.publisher_ready_event",
            "control.serving_refresh_state",
            "control.serving_refresh_chunk_state",
        ),
    ),
    _rule(
        "DQ-SHARED-006",
        "BLOCK",
        "revision_integrity",
        "Quality evidence is append-only: results never mutate beyond a "
        "warning's review status, every terminal run records its finish, and "
        "each run's results are unique per rule, object, and partition.",
        ("control.data_quality_run", "control.data_quality_result"),
    ),
    # -- shared geography reference ----------------------------------------
    _rule(
        "DQ-REF-001",
        "BLOCK",
        "uniqueness",
        "Geography reference tables are unique at their declared grains.",
        _names(_REFERENCE_OBJECTS),
    ),
    _rule(
        "DQ-REF-002",
        "BLOCK",
        "referential_integrity",
        "Every entity version, geometry version, and relationship resolves to "
        "a live geography entity of a known type.",
        (
            "silver_ref.dim_geo_entity",
            "silver_ref.dim_geo_entity_version",
            "silver_ref.dim_geo_geometry_version",
            "silver_ref.bridge_geo_relationship_version",
            "silver_ref.dim_geo_type",
        ),
    ),
    _rule(
        "DQ-REF-003",
        "QUARANTINE",
        "referential_integrity",
        "Every provider geography either resolves through geography_resolution "
        "or is explicitly recorded as unresolved/unsupported.",
        ("silver_ref.geography_resolution",),
    ),
    _rule(
        "DQ-REF-004",
        "WARN",
        "conformance",
        "Relationship overlap weights and hierarchy shapes stay within "
        "reviewed bounds after a geography reload.",
        ("silver_ref.bridge_geo_relationship_version",),
    ),
    _rule(
        "DQ-REF-005",
        "BLOCK",
        "conformance",
        "The current-geography projections expose exactly one current version "
        "per entity.",
        ("silver_ref.dim_geo", "silver_ref.dim_geo_current"),
    ),
    _rule(
        "DQ-REF-006",
        "INFO",
        "completeness",
        "The time dimension covers the full configured observation range.",
        ("silver_ref.dim_time",),
    ),
    # -- glossary and cross-source serving ---------------------------------
    _rule(
        "DQ-GLOSSARY-001",
        "BLOCK",
        "referential_integrity",
        "Every publisher registry row resolves to a live metric_publisher "
        "view, and every published source has exactly one registry row.",
        _names(_GLOSSARY_OBJECTS)
        + (
            "gold_census.metric_publisher",
            "gold_bls.metric_publisher",
            "gold_fred.metric_publisher",
            "gold_pep.metric_publisher",
            "gold_cdc.metric_publisher",
            "gold_fbi.metric_publisher",
            "gold_nass.metric_publisher",
        ),
    ),
    _rule(
        "DQ-GLOSSARY-002",
        "BLOCK",
        "uniqueness",
        "Metric catalog identity is unique per metric_code and per source "
        "object key; no harvest may fork a metric identity.",
        ("gold_glossary.dim_metric_catalog", "gold_glossary.dim_source_system"),
    ),
    _rule(
        "DQ-GLOSSARY-003",
        "BLOCK",
        "conformance",
        "Legacy gold.* compatibility views stay column- and row-consistent "
        "with the glossary contract objects they project.",
        _names(_LEGACY_SERVING_OBJECTS),
    ),
    _rule(
        "DQ-GLOSSARY-004",
        "INFO",
        "freshness",
        "Publisher harvest watermarks advance with each publication event.",
        ("gold_glossary.publisher_harvest_state", "control.publisher_ready_event"),
    ),
    # -- per-source uniqueness at declared grain ---------------------------
    _rule(
        "DQ-ACS-001",
        "BLOCK",
        "uniqueness",
        "ACS silver and gold facts are unique at their declared grains.",
        (
            "silver_census.fact_demographics",
            "gold_census.fact_acs_observation",
            "gold_census.rpt_acs_observations",
            "gold_census.mv_acs_latest",
        ),
    ),
    _rule(
        "DQ-ACS-002",
        "QUARANTINE",
        "reconciliation",
        "Configured acs1/acs5 dataset-years reconcile to metadata, slice "
        "ledgers, captures, and observations; ACS1 source-confirmed absence "
        "is valid emptiness.",
        (
            "control.acs_ingestion_slices",
            "raw_census.acs_datasets",
            "silver_census.fact_demographics",
        ),
    ),
    _rule(
        "DQ-ACS-003",
        "BLOCK",
        "conformance",
        "Census sentinel and null interpretation is preserved with exact "
        "source text; estimate/MOE pairing is checked without fabrication.",
        ("silver_census.fact_demographics", "gold_census.fact_acs_observation"),
    ),
    _rule(
        "DQ-ACS-004",
        "BLOCK",
        "referential_integrity",
        "Every published ACS observation resolves its variable metadata and geography.",
        (
            "gold_census.fact_acs_observation",
            "gold_census.dim_acs_variable",
            "silver_ref.geography_resolution",
        ),
    ),
    _rule(
        "DQ-ACS-005",
        "WARN",
        "plausibility",
        "Year-over-year ACS changes are monitored stratified by variable, "
        "dataset, and geography level; overlapping vintages are never "
        "compared as independent samples.",
        ("gold_census.fact_acs_observation",),
    ),
    _rule(
        "DQ-ACS-006",
        "INFO",
        "freshness",
        "ACS vintage freshness is evaluated against annual release "
        "availability per dataset.",
        ("gold_census.mv_acs_latest",),
    ),
    _rule(
        "DQ-ACS-007",
        "BLOCK",
        "conformance",
        "ACS serving contract views preserve the published fact's identity, "
        "values, and metric codes.",
        (
            "gold_census.fact_observation",
            "gold_census.v_metric_latest_by_geo",
            "gold_census.v_metric_timeseries_by_geo",
            "gold_census.metric_publisher",
            "gold_census.dim_acs_table",
        ),
    ),
    _rule(
        "DQ-BLS-001",
        "BLOCK",
        "uniqueness",
        "BLS silver and gold facts are unique at (series_id, period).",
        (
            "silver_bls.fact_labor_statistics",
            "gold_bls.fact_bls_observation",
            "gold_bls.rpt_bls_observations",
            "gold_bls.mv_bls_latest",
        ),
    ),
    _rule(
        "DQ-BLS-002",
        "QUARANTINE",
        "reconciliation",
        "Request-sized series/year chunks reconcile so a partial backfill "
        "cannot appear complete; provider 'No Data Available' counts as "
        "emptiness only for the exact request.",
        (
            "control.bls_ingestion_slices",
            "silver_bls.observation_revision",
            "silver_bls.fact_labor_statistics",
        ),
    ),
    _rule(
        "DQ-BLS-003",
        "BLOCK",
        "conformance",
        "Series-ID grammar, program ownership, period codes, annual-average "
        "handling, and footnotes conform to synchronized metadata.",
        (
            "raw_bls.bls_datasets",
            "raw_bls.bls_series",
            "silver_bls.fact_labor_statistics",
        ),
    ),
    _rule(
        "DQ-BLS-004",
        "BLOCK",
        "referential_integrity",
        "Every published BLS observation resolves its series and survey "
        "dimensions and its geography.",
        (
            "gold_bls.fact_bls_observation",
            "gold_bls.dim_bls_series",
            "gold_bls.dim_bls_survey",
        ),
    ),
    _rule(
        "DQ-BLS-005",
        "INFO",
        "temporal_integrity",
        "Expected frequency and published observation range are evaluated "
        "per series, never as one universal monthly requirement.",
        ("silver_bls.fact_labor_statistics",),
    ),
    _rule(
        "DQ-BLS-006",
        "WARN",
        "revision_integrity",
        "Revision and benchmark changes are tracked without overwriting "
        "prior captured revisions.",
        ("silver_bls.observation_revision", "gold_bls.fact_bls_observation"),
    ),
    _rule(
        "DQ-BLS-007",
        "BLOCK",
        "conformance",
        "BLS serving contract views preserve the published fact's identity, "
        "values, and metric codes.",
        (
            "gold_bls.fact_observation",
            "gold_bls.v_metric_latest_by_geo",
            "gold_bls.v_metric_timeseries_by_geo",
            "gold_bls.metric_publisher",
        ),
    ),
    _rule(
        "DQ-FRED-001",
        "BLOCK",
        "uniqueness",
        "FRED silver and gold facts are unique at (series_id, observation_date).",
        (
            "silver_fred.fact_economic_indicators",
            "gold_fred.fact_fred_observation",
            "gold_fred.rpt_fred_observations",
            "gold_fred.mv_fred_latest",
        ),
    ),
    _rule(
        "DQ-FRED-002",
        "QUARANTINE",
        "reconciliation",
        "Configured series, requested date ranges, captures, silver revisions, "
        "and published current observations reconcile per domain.",
        (
            "control.fred_ingestion_slices",
            "raw_fred.fred_datasets",
            "silver_fred.fact_economic_indicators",
        ),
    ),
    _rule(
        "DQ-FRED-003",
        "BLOCK",
        "conformance",
        "The FRED missing marker stays distinct from numeric zero, and every "
        "configured series has metadata and exactly one domain owner.",
        (
            "raw_fred.fred_series",
            "silver_fred.observation_revision",
            "silver_fred.fact_economic_indicators",
        ),
    ),
    _rule(
        "DQ-FRED-004",
        "BLOCK",
        "temporal_integrity",
        "Observation dates validate against each series' frequency and "
        "source observation range.",
        ("silver_fred.fact_economic_indicators", "gold_fred.dim_fred_series"),
    ),
    _rule(
        "DQ-FRED-005",
        "INFO",
        "freshness",
        "Freshness windows apply per frequency; daily through annual series "
        "never share one threshold.",
        ("gold_fred.mv_fred_latest",),
    ),
    _rule(
        "DQ-FRED-006",
        "WARN",
        "plausibility",
        "Unusually large FRED changes warn and are never automatic invalidation.",
        ("gold_fred.fact_fred_observation",),
    ),
    _rule(
        "DQ-FRED-007",
        "BLOCK",
        "conformance",
        "FRED serving contract views preserve the published fact's identity, "
        "values, and metric codes.",
        (
            "gold_fred.fact_observation",
            "gold_fred.v_metric_latest_by_geo",
            "gold_fred.v_metric_timeseries_by_geo",
            "gold_fred.metric_publisher",
        ),
    ),
    # -- Census PEP --------------------------------------------------------
    _rule(
        "DQ-PEP-001",
        "BLOCK",
        "uniqueness",
        "PEP facts are unique at the capture grain and at the natural key "
        "(dataset, vintage, metric, geography, year).",
        (
            "silver_pep.fact_population_estimate",
            "gold_pep.population_estimate_revision",
            "gold_pep.population_estimate_latest",
            "gold_pep.rpt_pep_observations",
            "gold_pep.mv_pep_latest",
        ),
    ),
    _rule(
        "DQ-PEP-002",
        "BLOCK",
        "completeness",
        "Only captures with a complete release_load verdict publish; an "
        "incomplete load carries its reason and never feeds gold.",
        (
            "silver_pep.release_load",
            "gold_pep.population_estimate_revision",
        ),
    ),
    _rule(
        "DQ-PEP-003",
        "QUARANTINE",
        "reconciliation",
        "Registered datasets and vintages reconcile to captures, loads, and "
        "published observations; registry-excluded levels are valid emptiness.",
        (
            "silver_pep.pep_dataset",
            "silver_pep.pep_release",
            "silver_pep.fact_population_estimate",
        ),
    ),
    _rule(
        "DQ-PEP-004",
        "BLOCK",
        "conformance",
        "The frozen Census null-sentinel set and value_status semantics hold; "
        "only valid rows carry a number; component sign rules hold.",
        (
            "silver_pep.observation_revision",
            "silver_pep.fact_population_estimate",
            "silver_pep.dim_measure",
        ),
    ),
    _rule(
        "DQ-PEP-005",
        "BLOCK",
        "revision_integrity",
        "Prior vintages are retained; current-revision selection within a "
        "vintage is by capture recency; latest-vintage selection is a "
        "separate projection.",
        (
            "gold_pep.population_estimate_revision",
            "gold_pep.population_estimate_latest",
            "gold_pep.population_change",
        ),
    ),
    _rule(
        "DQ-PEP-006",
        "WARN",
        "plausibility",
        "Vintage-over-vintage revisions to the same (metric, geography, year) "
        "warn and never automatically invalidate.",
        ("gold_pep.population_estimate_revision",),
    ),
    _rule(
        "DQ-PEP-007",
        "BLOCK",
        "conformance",
        "PEP publisher exports preserve metric identity and the annual time grain.",
        ("gold_pep.measure_export", "gold_pep.metric_publisher"),
    ),
    # -- CDC ---------------------------------------------------------------
    _rule(
        "DQ-CDC-001",
        "BLOCK",
        "uniqueness",
        "CDC facts are unique at (asset, release watermark, source record).",
        (
            "silver_cdc.fact_health_observation",
            "gold_cdc.health_observation",
            "gold_cdc.latest_release_observation",
        ),
    ),
    _rule(
        "DQ-CDC-002",
        "QUARANTINE",
        "revision_integrity",
        "Release watermarks advance monotonically per asset; backward "
        "watermarks, schema changes, and dataset replacements quarantine "
        "rather than overwrite.",
        ("control.cdc_dataset_release", "silver_cdc.dim_dataset_release"),
    ),
    _rule(
        "DQ-CDC-003",
        "BLOCK",
        "completeness",
        "A CDC release publishes only when complete; partial pages cannot "
        "advance the published watermark.",
        (
            "control.cdc_dataset_release",
            "silver_cdc.fact_health_observation",
            "gold_cdc.health_observation",
        ),
    ),
    _rule(
        "DQ-CDC-004",
        "BLOCK",
        "conformance",
        "Suppression semantics are exact: CDI suppression is absent datavalue "
        "with footnotes retained; PLACES suppression requires the suppression "
        "footnote; suppressed and missing are never zero.",
        ("silver_cdc.fact_health_observation", "silver_cdc.observation_quarantine"),
    ),
    _rule(
        "DQ-CDC-005",
        "BLOCK",
        "referential_integrity",
        "Every CDC observation resolves its release, measure, stratum, and "
        "geography status; confidence intervals stay ordered.",
        (
            "silver_cdc.fact_health_observation",
            "silver_cdc.dim_measure",
            "silver_cdc.dim_stratum",
        ),
    ),
    _rule(
        "DQ-CDC-006",
        "INFO",
        "freshness",
        "Freshness is evaluated per asset cadence: CDI irregular, PLACES "
        "annual, both checked weekly.",
        ("silver_cdc.dim_dataset_release",),
    ),
    _rule(
        "DQ-CDC-007",
        "BLOCK",
        "conformance",
        "CDC publisher exports preserve measure identity and the annual time grain.",
        ("gold_cdc.measure_export", "gold_cdc.metric_publisher"),
    ),
    # -- FBI UCR -----------------------------------------------------------
    _rule(
        "DQ-FBI-001",
        "BLOCK",
        "uniqueness",
        "FBI facts are unique at (product, release, source record) and "
        "participation at (product, release, subject, period).",
        (
            "silver_fbi.fact_crime_observation",
            "silver_fbi.fact_reporting_participation",
            "gold_fbi.crime_observation",
            "gold_fbi.latest_release_observation",
        ),
    ),
    _rule(
        "DQ-FBI-002",
        "BLOCK",
        "referential_integrity",
        "No crime observation publishes without a reporting-participation "
        "coverage interpretation; coverage basis is recorded, never imputed.",
        (
            "silver_fbi.fact_crime_observation",
            "silver_fbi.fact_reporting_participation",
            "gold_fbi.crime_observation",
            "gold_fbi.reporting_coverage",
        ),
    ),
    _rule(
        "DQ-FBI-003",
        "BLOCK",
        "conformance",
        "Reported and not_reported stay distinct: a published zero is a "
        "value, an absent month is NULL, and no rule conflates them.",
        ("silver_fbi.fact_crime_observation", "gold_fbi.crime_observation"),
    ),
    _rule(
        "DQ-FBI-004",
        "BLOCK",
        "conformance",
        "The agency aggregation boundary holds: agency-grain observations "
        "are never summed into county or city totals; attribution flows only "
        "through exact state codes or reviewed crosswalks.",
        (
            "gold_fbi.agency_observation_area_filter",
            "gold_fbi.agency_geography",
            "silver_fbi.agency_geography_relationship",
            "silver_fbi.reviewed_place_crosswalk",
            "silver_fbi.dim_state_code",
        ),
    ),
    _rule(
        "DQ-FBI-005",
        "QUARANTINE",
        "reconciliation",
        "Every release reconciles directory and observation slice counts to "
        "silver revisions and quarantines before publication; ambiguous "
        "geography is withheld from gold but stays queryable.",
        (
            "control.fbi_ucr_release",
            "silver_fbi.agency_revision",
            "silver_fbi.observation_revision",
            "silver_fbi.participation_revision",
            "silver_fbi.slice_quarantine",
        ),
    ),
    _rule(
        "DQ-FBI-006",
        "BLOCK",
        "revision_integrity",
        "Release identity comes from provider refresh_date; /LATEST is "
        "capture input, a backward refresh quarantines, and prior releases "
        "are retained.",
        (
            "silver_fbi.dim_ucr_dataset_release",
            "silver_fbi.dim_agency",
            "silver_fbi.dim_agency_version",
        ),
    ),
    _rule(
        "DQ-FBI-007",
        "BLOCK",
        "conformance",
        "Measure identity separation holds: counted-entity bases never share "
        "a measure, and only absolute totals carry the additive-within-"
        "subject characteristic.",
        (
            "silver_fbi.dim_offense_measure",
            "gold_fbi.measure_export",
            "gold_fbi.metric_publisher",
        ),
    ),
    # -- USDA NASS ---------------------------------------------------------
    _rule(
        "DQ-NASS-001",
        "BLOCK",
        "uniqueness",
        "NASS facts are unique at the complete Quick Stats grain "
        "(product, release watermark, source record), never commodity/year.",
        (
            "silver_nass.fact_crop_observation",
            "gold_nass.crop_observation",
            "gold_nass.latest_release_observation",
        ),
    ),
    _rule(
        "DQ-NASS-002",
        "QUARANTINE",
        "completeness",
        "The slice ledger reconciles preflight counts to captured rows; "
        "over-limit and partial slices never advance the published watermark.",
        ("control.usda_nass_release", "control.usda_nass_slice"),
    ),
    _rule(
        "DQ-NASS-003",
        "BLOCK",
        "conformance",
        "The full Quick Stats suppression vocabulary maps each symbol to its "
        "own value status, applies independently to CV values, and retains "
        "exact provider text.",
        ("silver_nass.fact_crop_observation", "silver_nass.observation_quarantine"),
    ),
    _rule(
        "DQ-NASS-004",
        "BLOCK",
        "referential_integrity",
        "Every NASS observation resolves its release, commodity, statistic, "
        "and domain dimensions; unsupported geography is explicit.",
        (
            "silver_nass.fact_crop_observation",
            "silver_nass.dim_dataset_release",
            "silver_nass.dim_commodity",
            "silver_nass.dim_statistic",
            "silver_nass.dim_domain",
            "silver_nass.observation_revision",
        ),
    ),
    _rule(
        "DQ-NASS-005",
        "BLOCK",
        "conformance",
        "additive_behavior propagates into series and publisher "
        "characteristics; not_established stays visibly unknown.",
        (
            "gold_nass.crop_series",
            "gold_nass.measure_export",
            "gold_nass.metric_publisher",
        ),
    ),
    _rule(
        "DQ-NASS-006",
        "INFO",
        "freshness",
        "Survey products follow revised-until-final expectations and the "
        "census product periodic-final; recent-window and full sweeps "
        "reconcile to the same retained releases.",
        ("silver_nass.dim_dataset_release",),
    ),
)


def objects_by_name(
    objects: Iterable[WarehouseObject] = ALL_OBJECTS,
) -> dict[str, WarehouseObject]:
    """Index objects by name, rejecting duplicates."""
    catalog: dict[str, WarehouseObject] = {}
    for entry in objects:
        if entry.name in catalog:
            raise QualityInventoryError(f"Duplicate object '{entry.name}'.")
        catalog[entry.name] = entry
    return catalog


def rules_by_object(
    rules: Iterable[QualityRule] = ALL_RULES,
) -> dict[str, tuple[QualityRule, ...]]:
    """Index rules by each object they evaluate."""
    index: dict[str, list[QualityRule]] = {}
    for rule in rules:
        for name in rule.objects:
            index.setdefault(name, []).append(rule)
    return {name: tuple(entries) for name, entries in index.items()}


def validate_inventory(
    objects: Iterable[WarehouseObject] = ALL_OBJECTS,
    rules: Iterable[QualityRule] = ALL_RULES,
) -> Mapping[str, WarehouseObject]:
    """Validate the whole inventory and return the object catalog.

    Enforces the DQ-001 acceptance criterion: every published object has an
    owner, a declared grain, an expected-scope method, and at least one
    deterministic integrity rule.
    """
    catalog = objects_by_name(objects)

    for entry in catalog.values():
        unknown = sorted(set(entry.lineage) - set(catalog))
        if unknown:
            raise QualityInventoryError(
                f"{entry.name}: lineage names unknown object(s): {', '.join(unknown)}."
            )

    seen_rule_ids: set[str] = set()
    coverage = rules_by_object(rules)
    for rule in rules:
        if rule.rule_id in seen_rule_ids:
            raise QualityInventoryError(f"Duplicate rule id '{rule.rule_id}'.")
        seen_rule_ids.add(rule.rule_id)
        unknown = sorted(set(rule.objects) - set(catalog))
        if unknown:
            raise QualityInventoryError(
                f"{rule.rule_id}: evaluates unknown object(s): {', '.join(unknown)}."
            )

    for entry in catalog.values():
        if entry.layer not in PUBLISHED_LAYERS:
            continue
        deterministic = [
            rule for rule in coverage.get(entry.name, ()) if rule.is_deterministic
        ]
        if not deterministic:
            raise QualityInventoryError(
                f"{entry.name}: published object has no deterministic integrity rule."
            )

    return catalog
