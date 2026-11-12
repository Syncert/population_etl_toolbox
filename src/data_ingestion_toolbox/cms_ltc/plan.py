"""Which catalog releases a run reads.

A release already published from the same download URL is not read again;
CMS posts a corrected file under a new URL, which is read (and recorded
``unchanged`` if its bytes repeat the last publication). ``{"history": true}``
reads every release the catalog lists.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from .catalog import Release

ANSWERED_SQL = """
    SELECT series_id, period_start, download_url
    FROM control.cms_ltc_file
    WHERE status = ANY(%s)
"""

ANSWERED_STATUSES = ["published", "unchanged"]


def answered(connection: Any) -> set[tuple[str, str, str]]:
    with connection.cursor() as cursor:
        cursor.execute(ANSWERED_SQL, (ANSWERED_STATUSES,))
        return {
            (str(series_id), str(period_start), str(url))
            for series_id, period_start, url in cursor.fetchall()
        }


def releases_to_capture(
    found: Iterable[Release],
    answered_keys: set[tuple[str, str, str]],
    conf: Mapping[str, Any] | None = None,
) -> list[Release]:
    """The releases a run reads, cost reports first (they place the facilities)."""
    history = bool((conf or {}).get("history"))
    chosen = [
        release
        for release in found
        if history
        or (release.series_id, release.release_key, release.download_url)
        not in answered_keys
    ]
    order = {"cost_report": 0, "characteristics": 1}
    return sorted(
        chosen, key=lambda item: (order.get(item.series_id, 9), item.period_start)
    )
