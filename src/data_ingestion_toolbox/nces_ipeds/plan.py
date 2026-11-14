"""Which IPEDS files a run reads.

For each component, every registered year from its first year to its last
registered year, and also the year after the newest one published (so a new
release is found without a code change; a component with a last registered
year stops there). A file published, recorded ``unchanged`` or set aside
whole is answered and not read again; one NCES has not published (404) is
asked again. ``{"history": true}`` reads every registered year again.
Directory files come first within a year: they place that year's
institutions.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from .registry import COMPONENTS, Component

ANSWERED_SQL = """
    SELECT component_id, collection_year, status
    FROM control.nces_ipeds_file
    WHERE status = ANY(%s)
"""

ANSWERED_STATUSES = ["published", "unchanged", "quarantined"]


def answered(connection: Any) -> dict[tuple[str, int], set[str]]:
    with connection.cursor() as cursor:
        cursor.execute(ANSWERED_SQL, (ANSWERED_STATUSES,))
        found: dict[tuple[str, int], set[str]] = {}
        for component_id, year, status in cursor.fetchall():
            found.setdefault((str(component_id), int(year)), set()).add(str(status))
        return found


def _years(
    component: Component, answered_keys: Mapping[tuple[str, int], set[str]], newest: int
) -> list[int]:
    published = [
        year
        for (component_id, year), statuses in answered_keys.items()
        if component_id == component.component_id and "published" in statuses
    ]
    last = component.last_year
    if last is None:
        last = max(newest, max(published) + 1) if published else newest
    return list(range(component.first_year, last + 1))


def files_to_capture(
    answered_keys: Mapping[tuple[str, int], set[str]],
    conf: Mapping[str, Any] | None = None,
    *,
    newest_registered_year: int = 2024,
    components: Iterable[Component] = COMPONENTS,
) -> list[tuple[Component, int]]:
    """(component, year) pairs a run reads, by year, directory first."""
    history = bool((conf or {}).get("history"))
    order = {
        component.component_id: index for index, component in enumerate(COMPONENTS)
    }
    chosen = []
    for component in components:
        for year in _years(component, answered_keys, newest_registered_year):
            if history or (component.component_id, year) not in answered_keys:
                chosen.append((component, year))
    return sorted(chosen, key=lambda item: (item[1], order[item[0].component_id]))
