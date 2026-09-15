"""Regenerate the reviewed web-visualization coverage snapshot.

Run deliberately, as part of a change that intends to move which sources can
be drawn in which presentation:

    python -m tests.support.regenerate_viz_coverage

Then read the resulting diff. A source moving out of a surface's
``served_sources`` is a screen going blank for a reader; regenerating the
snapshot to silence a failing test is how that ships unnoticed.

The snapshot is also what the frontend tier reads. Python builds the served
capability payload and the matrix over it; the web suite runs its own
discovery and request-building modules against the same payload and must agree
about every cell. Two languages cannot import one declaration, so they share
one reviewed file instead of each keeping a copy.
"""

from __future__ import annotations

import json

from pathlib import Path
from typing import Any

from apps.api.main import app
from apps.api.services.catalog_service import list_source_capabilities
from tests.support.viz_coverage import (
    VERSIONED_ROOT,
    coverage_matrix,
    render_matrix,
)

SNAPSHOT_PATH = (
    Path(__file__).resolve().parents[1] / "fixtures" / "api" / "viz_coverage.json"
)


def application_query_parameters(paths: dict[str, Any]) -> dict[str, list[str]]:
    """Version-relative GET path -> declared query parameter names."""
    parameters: dict[str, list[str]] = {}
    for path, item in paths.items():
        operation = (item or {}).get("get")
        if operation is None:
            continue
        relative = (
            path[len(VERSIONED_ROOT) :] if path.startswith(VERSIONED_ROOT) else path
        )
        parameters[relative] = sorted(
            entry["name"]
            for entry in operation.get("parameters") or []
            if entry.get("in") == "query"
        )
    return parameters


def build_snapshot() -> dict[str, Any]:
    """The capability payload and the matrix computed over it."""
    paths = app.openapi()["paths"]
    capabilities = [item.model_dump() for item in list_source_capabilities(paths).items]
    verdicts = coverage_matrix(
        capabilities, application_parameters=application_query_parameters(paths)
    )
    return {
        "capabilities": capabilities,
        **render_matrix(verdicts),
    }


def main() -> None:
    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    snapshot = build_snapshot()
    SNAPSHOT_PATH.write_text(
        json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    served = sum(len(surface["served_sources"]) for surface in snapshot["surfaces"])
    declined = sum(len(surface["declined_sources"]) for surface in snapshot["surfaces"])
    print(
        f"wrote {SNAPSHOT_PATH} ({len(snapshot['surfaces'])} surfaces, "
        f"{len(snapshot['capabilities'])} sources, {served} served, "
        f"{declined} declined)"
    )


if __name__ == "__main__":
    main()
