"""The plain label every response that names a metric carries (A-2).

``metric_display_name`` stays the provider's own label, untouched, so a
consumer that reproduces a provider's wording still can. ``metric_label`` is
what a reader should see: the reviewed (or draft) plain label from
``docs/semantics/metric_definitions.json``, or the provider's label with the
ACS ``Estimate!!`` path set as words. ``metric_label_status`` says which.
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, model_validator

from data_ingestion_toolbox.semantics.definitions import definition_for, plain_label

LabelStatus = Literal["approved", "draft", "needs_review", "deprecated", "provider"]


class MetricDefinitionSummary(BaseModel):
    """A metric's plain-language definition, as its registry entry states it."""

    label: str
    definition: str
    unit: Optional[str] = None
    universe: Optional[str] = None
    caveat: Optional[str] = None
    status: LabelStatus
    version: int
    citations: list[str]


class PlainLabelled(BaseModel):
    """Mixin: fills ``metric_label`` from ``metric_code`` and the provider label."""

    metric_label: Optional[str] = None
    metric_label_status: Optional[LabelStatus] = None

    @model_validator(mode="after")
    def _label(self):  # type: ignore[no-untyped-def]
        code = getattr(self, "metric_code", None)
        if code and self.metric_label is None:
            label, status = plain_label(
                code, getattr(self, "metric_display_name", None)
            )
            self.metric_label = label
            self.metric_label_status = status if label is not None else None
        return self


def definition_summary(metric_code: str | None) -> MetricDefinitionSummary | None:
    definition = definition_for(metric_code)
    if definition is None:
        return None
    return MetricDefinitionSummary(
        label=definition.label,
        definition=definition.definition,
        unit=definition.unit,
        universe=definition.universe,
        caveat=definition.caveat,
        status=definition.status,
        version=definition.version,
        citations=list(definition.citations),
    )
