"""Conform parsed flows to the shared geography (ADR-0008).

A flow has two geography keys, origin and destination. A row is admitted to
the flow fact only when every county it names resolves to the shared
reference: the subject always, and for a county-to-county flow the other
county too. A row naming a county the reference does not hold is refused
with the side that failed, never loaded with a dangling key and never
folded into a category. SOI's own categories carry no second county, so
they need only their subject.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from .parse import FlowRow


@dataclass(frozen=True)
class ConformedFlow:
    flow: FlowRow
    subject_geo_sk: int
    origin_geo_sk: int | None
    destination_geo_sk: int | None


@dataclass(frozen=True)
class RefusedFlow:
    flow: FlowRow
    error_code: str
    error_summary: str


def conform_flows(
    flows: Iterable[FlowRow], geo_sk_by_id: Mapping[str, int]
) -> tuple[list[ConformedFlow], list[RefusedFlow]]:
    """Admit flows whose counties all resolve; refuse the rest by side."""
    admitted: list[ConformedFlow] = []
    refused: list[RefusedFlow] = []
    for flow in flows:
        subject = geo_sk_by_id.get(flow.subject_geo_id)
        if subject is None:
            refused.append(
                RefusedFlow(
                    flow,
                    "subject_unresolved",
                    f"{flow.subject_geo_id} is not in the shared reference",
                )
            )
            continue
        origin = destination = None
        if flow.origin_geo_id is not None:
            origin = geo_sk_by_id.get(flow.origin_geo_id)
            if origin is None:
                refused.append(
                    RefusedFlow(
                        flow,
                        "origin_unresolved",
                        f"{flow.origin_geo_id} is not in the shared reference",
                    )
                )
                continue
        if flow.destination_geo_id is not None:
            destination = geo_sk_by_id.get(flow.destination_geo_id)
            if destination is None:
                refused.append(
                    RefusedFlow(
                        flow,
                        "destination_unresolved",
                        f"{flow.destination_geo_id} is not in the shared reference",
                    )
                )
                continue
        admitted.append(ConformedFlow(flow, subject, origin, destination))
    return admitted, refused
