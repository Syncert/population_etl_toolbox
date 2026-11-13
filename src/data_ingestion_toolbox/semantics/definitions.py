"""Plain-language metric definitions, and the label a reader sees (A-2).

The registry is ``docs/semantics/metric_definitions.json`` (schema
``definition.schema.json``): one entry per measure, naming the metric codes
it defines, a short label, a one-sentence definition, the unit, the universe
and a caveat, with its owner, reviewer, status and citations.

``plain_label`` is the label every API route that names a metric serves. A
metric with a registry entry is served its entry's label, with the entry's
status beside it, so a draft is never passed off as reviewed. A metric with
no entry is served its provider's own label: for the American Community
Survey, the variable's label path with the Bureau's ``Estimate!!`` prefix and
``!!`` separators set as words, which is the provider's text rearranged, never
a guessed name.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

STATUSES = frozenset({"draft", "approved", "deprecated", "needs_review"})
#: What a value measures (ADR-0010 rule 2). A count grows with the place; the
#: others describe its population whatever its size, so they compare across
#: geographic levels.
KINDS = frozenset({"count", "median", "mean", "rate", "share", "index"})
#: The kind an entry without one is taken to be: the conservative default.
DEFAULT_KIND = "count"
REQUIRED = (
    "metric_id",
    "metric_codes",
    "name",
    "definition",
    "intended_use",
    "limitations",
    "status",
    "owner",
    "reviewer",
    "version",
    "effective_date",
    "last_reviewed_date",
    "citations",
)

REGISTRY_PATH = (
    Path(__file__).resolve().parents[3]
    / "docs"
    / "semantics"
    / "metric_definitions.json"
)

#: The ACS label path's measure prefixes, as the Bureau writes them.
_ACS_PREFIX = re.compile(r"^(Estimate|Margin of Error|Annotation of Estimate)!!")


#: Set to ``1`` to serve only reviewed (``approved``) labels: a draft entry's
#: label then gives way to the provider's, while its definition is still
#: served with its status. Off by default so the drafts can be reviewed on the
#: running site; listed in ``infra/docker/stack.env.example``.
REVIEWED_ONLY_ENV = "METRIC_LABELS_REVIEWED_ONLY"


def reviewed_only() -> bool:
    return os.environ.get(REVIEWED_ONLY_ENV, "").strip().lower() in {"1", "true", "yes"}


class DefinitionError(ValueError):
    """The registry breaks its own contract."""


@dataclass(frozen=True)
class MetricDefinition:
    metric_id: str
    label: str
    definition: str
    unit: str | None
    universe: str | None
    caveat: str | None
    status: str
    version: int
    citations: tuple[str, ...]
    #: ``None`` when the entry declares none; read it through ``measure_kind``.
    kind: str | None = None


def parse_registry(document: dict) -> dict[str, MetricDefinition]:
    """Each entry by every metric code it names, or DefinitionError."""
    by_code: dict[str, MetricDefinition] = {}
    ids: set[str] = set()
    for entry in document.get("entries", []):
        missing = [field for field in REQUIRED if field not in entry]
        if missing:
            raise DefinitionError(
                f"{entry.get('metric_id')} lacks {', '.join(missing)}"
            )
        if entry["metric_id"] in ids:
            raise DefinitionError(f"{entry['metric_id']} has two entries")
        ids.add(entry["metric_id"])
        if entry["status"] not in STATUSES:
            raise DefinitionError(
                f"{entry['metric_id']} has status {entry['status']!r}"
            )
        if entry.get("kind") is not None and entry["kind"] not in KINDS:
            raise DefinitionError(f"{entry['metric_id']} has kind {entry['kind']!r}")
        if not entry["citations"] or not entry["metric_codes"]:
            raise DefinitionError(f"{entry['metric_id']} names no code or no citation")
        definition = MetricDefinition(
            metric_id=entry["metric_id"],
            label=entry["name"],
            definition=entry["definition"],
            unit=entry.get("unit"),
            universe=entry.get("universe"),
            caveat=entry.get("caveat"),
            status=entry["status"],
            version=int(entry["version"]),
            citations=tuple(entry["citations"]),
            kind=entry.get("kind"),
        )
        for code in entry["metric_codes"]:
            if code in by_code:
                raise DefinitionError(f"{code} is defined twice")
            by_code[code] = definition
    return by_code


@lru_cache(maxsize=1)
def registry() -> dict[str, MetricDefinition]:
    return parse_registry(json.loads(REGISTRY_PATH.read_text(encoding="utf-8")))


def definition_for(metric_code: str | None) -> MetricDefinition | None:
    if not metric_code:
        return None
    return registry().get(metric_code)


def measure_kind(metric_code: str | None) -> tuple[str, str]:
    """What a metric measures and how sure the registry is: ``(kind, status)``.

    ``status`` is the entry's own status, or ``undeclared`` for a metric with
    no entry or an entry without a kind, which is then a count (ADR-0010 rule
    2). With ``METRIC_LABELS_REVIEWED_ONLY=1`` a draft's kind gives way to the
    default exactly as its label gives way to the provider's.
    """
    definition = definition_for(metric_code)
    if (
        definition is None
        or definition.kind is None
        or (reviewed_only() and definition.status != "approved")
    ):
        return DEFAULT_KIND, "undeclared"
    return definition.kind, definition.status


def provider_label(metric_code: str | None, display_name: str | None) -> str | None:
    """The provider's own label, with the ACS label path set as words."""
    if not display_name:
        return display_name
    if (metric_code or "").startswith("CENSUS_ACS:") and "!!" in display_name:
        parts = [
            part.strip().rstrip(":")
            for part in _ACS_PREFIX.sub("", display_name).split("!!")
        ]
        return ": ".join(part for part in parts if part)
    return display_name


def plain_label(
    metric_code: str | None, display_name: str | None
) -> tuple[str | None, str]:
    """The label to show and where it came from: the entry's status, or ``provider``."""
    definition = definition_for(metric_code)
    if definition is not None and not (
        reviewed_only() and definition.status != "approved"
    ):
        return definition.label, definition.status
    return provider_label(metric_code, display_name), "provider"
