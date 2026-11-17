"""Response bodies for the admin warehouse routes (``/admin/warehouse/*``).

These describe the warehouse to its operators: what each source is, what it
holds, and every table and view with its keys. Nothing here is observation
data, and nothing is cacheable by a shared cache -- every route answers
``private, no-store``.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class GrainMark(BaseModel):
    grain: str = Field(
        description="Page column: nation, state, county, metro, place, tract or other."
    )
    mark: str = Field(
        description="n = the publisher's own figure; d = built by the warehouse; "
        "p = some programs or areas only."
    )


class CatalogSummary(BaseModel):
    """What ``gold_glossary.dim_metric_catalog`` says about the source now."""

    metrics: int
    current_metrics: int
    stale_metrics: int
    geo_grains: list[str]
    time_grains: list[str]
    newest_watermark: str | None
    newest_publication: str | None
    last_harvested: str | None


class WarehouseSourceEntry(BaseModel):
    code: str
    name: str
    publisher: str
    topic: str
    module: str
    dag: str
    cadence: str
    release: str
    grains: list[GrainMark]
    declared_years: list[list[int]] = Field(
        description="Inclusive observation-year ranges the loaders are configured for."
    )
    years_note: str
    datasets: list[str]
    measures: str
    raw: list[str]
    control: list[str]
    silver: list[str]
    gold: list[str]
    credential: str
    catalog_source_code: str | None
    catalog: CatalogSummary | None


class WarehouseSourcesResponse(BaseModel):
    grains: list[list[str]] = Field(description="(code, label) in display order.")
    topics: list[str]
    sources: list[WarehouseSourceEntry]
    catalog_read_at: str


class GrainCoverage(BaseModel):
    grain: str | None = Field(
        description="The served grain word, as the API serves it."
    )
    column: str = Field(description="Page column the grain folds onto.")
    first_year: int | None
    last_year: int | None
    years: list[int]
    row_count: int | None


class SourceCoverageResponse(BaseModel):
    source_code: str
    catalog_source_code: str | None
    status: str = Field(
        description="ok, shared (measured under another source), not_served, "
        "timed_out or unavailable."
    )
    relation: str | None
    method: str | None = Field(description="scan or partition_catalog.")
    note: str | None
    grains: list[GrainCoverage]
    row_count: int | None
    row_count_is_estimate: bool
    measured_at: str
    elapsed_ms: int
    cached: bool


class AtlasStats(BaseModel):
    schemas: int
    tables: int
    views: int
    partitions: int
    foreign_keys: int


class AtlasBox(BaseModel):
    name: str
    layer: str
    kind: str
    shared: bool
    partitions: int
    x: int
    y: int
    width: int
    height: int
    columns: list[list[str]] = Field(
        description="(name, type, badge) per drawn column."
    )
    more_columns: int


class AtlasEdge(BaseModel):
    from_: str = Field(alias="from")
    to: str
    kind: str = Field(
        description="fk (a declared foreign key) or reads (a view reads it)."
    )
    columns: list[str]

    model_config = {"populate_by_name": True}


class AtlasDomain(BaseModel):
    id: str
    title: str
    caption: str
    source_code: str | None
    tables: int
    views: int
    width: int
    height: int
    boxes: list[AtlasBox]
    edges: list[AtlasEdge]


class AtlasForeignKey(BaseModel):
    columns: list[str]
    references: str


class AtlasRelation(BaseModel):
    name: str
    layer: str
    kind: str
    domain: str | None
    columns: list[list[Any]] = Field(description="(name, type, not_null) per column.")
    primary_key: list[str]
    uniques: list[str]
    foreign_keys: list[AtlasForeignKey]
    reads_from: list[str]
    partition_key: str | None
    partitions: int
    estimated_rows: int | None
    index_count: int


class WarehouseSchemaResponse(BaseModel):
    stats: AtlasStats
    domains: list[AtlasDomain]
    relations: list[AtlasRelation]
    read_at: str
