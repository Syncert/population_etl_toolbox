"""Which catalog releases a run reads.

A release already published from the same download URL under the series'
current parser contract is not read again; CMS posts a corrected file under
a new URL, which is read (and recorded ``unchanged`` if its bytes repeat the
last publication). A release published under an earlier parser contract is
read again once, so a contract change replays every release and rebuilds
what the new contract derives (placements, encodings). ``{"history": true}``
reads every release the catalog lists.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from .catalog import Release
from .registry import get_series

ANSWERED_SQL = """
    SELECT file.series_id, file.period_start, file.download_url,
           capture.payload_schema_version
    FROM control.cms_ltc_file AS file
    JOIN raw_capture.response_capture AS capture
      ON capture.capture_id = file.capture_id
    WHERE file.status = ANY(%s)
"""

ANSWERED_STATUSES = ["published", "unchanged"]


def answered(connection: Any) -> set[tuple[str, str, str]]:
    """Releases read under their series' current parser contract."""
    with connection.cursor() as cursor:
        cursor.execute(ANSWERED_SQL, (ANSWERED_STATUSES,))
        rows = cursor.fetchall()
    return {
        (str(series_id), str(period_start), str(url))
        for series_id, period_start, url, contract in rows
        if contract == _current_contract(str(series_id))
    }


def _current_contract(series_id: str) -> str | None:
    try:
        return get_series(series_id).parser_contract_version
    except KeyError:
        return None


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
