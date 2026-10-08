"""The reviewed per-metric time-aggregation methods (ADR-0007).

The registry is ``docs/semantics/time_aggregation_methods.json``: one entry
per served sub-annual metric, binding its stable ``metric_code`` to ``sum``,
``mean``, ``end_of_period``, ``recompute_ratio`` or ``not_aggregable``, with
its owner, reviewer, status and citations. Only an ``approved`` entry
authorizes a derived value; a ``draft`` is a proposal, and a metric with no
entry, a non-approved entry or ``not_aggregable`` has no derived value.

Reading the registry is the only thing here; nothing is inferred from units
or source.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

METHODS = frozenset(
    {"sum", "mean", "end_of_period", "recompute_ratio", "not_aggregable"}
)
STATUSES = frozenset({"draft", "approved", "deprecated", "needs_review"})

REGISTRY_PATH = (
    Path(__file__).resolve().parents[3]
    / "docs"
    / "semantics"
    / "time_aggregation_methods.json"
)


class RegistryError(ValueError):
    """The registry breaks its own contract."""


@dataclass(frozen=True)
class TimeMethod:
    metric_code: str
    method: str
    status: str
    reviewer: str
    version: int
    numerator: str | None = None
    denominator: str | None = None

    @property
    def authorizes_derivation(self) -> bool:
        return self.status == "approved" and self.method != "not_aggregable"


def parse_registry(document: dict) -> dict[str, TimeMethod]:
    """Each entry by metric code, or RegistryError naming the first broken rule."""
    entries: dict[str, TimeMethod] = {}
    for entry in document.get("entries", []):
        code = entry["metric_code"]
        if code in entries:
            raise RegistryError(f"{code} has two entries")
        if entry["method"] not in METHODS or entry["status"] not in STATUSES:
            raise RegistryError(f"{code} names an unknown method or status")
        ratio = entry["method"] == "recompute_ratio"
        if ratio != bool(entry.get("numerator") and entry.get("denominator")):
            raise RegistryError(
                f"{code}: recompute_ratio needs a numerator and denominator, and only it may name them"
            )
        if entry["status"] == "approved" and "pending" in entry["reviewer"].lower():
            raise RegistryError(f"{code} is approved without a reviewer")
        if not entry.get("citations"):
            raise RegistryError(f"{code} cites nothing")
        entries[code] = TimeMethod(
            code,
            entry["method"],
            entry["status"],
            entry["reviewer"],
            int(entry["version"]),
            entry.get("numerator"),
            entry.get("denominator"),
        )
    return entries


@lru_cache(maxsize=1)
def load_registry(path: Path = REGISTRY_PATH) -> dict[str, TimeMethod]:
    return parse_registry(json.loads(path.read_text(encoding="utf-8")))


def authorized_method(
    metric_code: str, registry: dict[str, TimeMethod] | None = None
) -> TimeMethod | None:
    """The method that may derive a value for this metric, or None."""
    entry = (registry if registry is not None else load_registry()).get(metric_code)
    return entry if entry is not None and entry.authorizes_derivation else None
