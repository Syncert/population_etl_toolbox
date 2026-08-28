"""Pure FBI CDE release identification and revision decisions.

``/LATEST`` is a mutable provider alias, so a release identity has to come from
the payload rather than the path. Every summarized response carries a
``cde_properties`` block with the UCR refresh date and the latest published
data month; those two values are the release key and the completeness signal.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from typing import Any

from .registry import FbiUcrProduct

_REFRESH_DATE = re.compile(r"^(0[1-9]|1[0-2])/(0[1-9]|[12][0-9]|3[01])/([0-9]{4})$")
_DATA_MONTH = re.compile(r"^(0[1-9]|1[0-2])/([0-9]{4})$")

#: Provider program key inside ``cde_properties``.
PROGRAM_KEY = "UCR"


class FbiReleaseError(ValueError):
    """A payload cannot support a safe release decision."""


class ReleaseDecision(StrEnum):
    """Typed outcome of comparing provider release state with accepted state."""

    UNCHANGED = "unchanged"
    INGEST = "ingest"
    MISSING_RELEASE_QUARANTINE = "missing_release_quarantine"
    BACKWARD_REFRESH_QUARANTINE = "backward_refresh_quarantine"
    PERIOD_UNAVAILABLE_QUARANTINE = "period_unavailable_quarantine"


@dataclass(frozen=True)
class FbiRelease:
    """Allowlisted provider freshness fields identifying one UCR release."""

    refresh_date: date
    max_data_month: str

    @property
    def release_key(self) -> str:
        """Return the stable warehouse release identity for this refresh."""
        return self.refresh_date.isoformat()

    @property
    def max_data_period(self) -> str:
        """Return the latest published data month as ``mm-yyyy``."""
        month, year = self.max_data_month.split("/")
        return f"{month}-{year}"


def parse_release(payload: bytes | dict[str, Any]) -> FbiRelease:
    """Parse only the fields required for release identity and completeness."""
    if isinstance(payload, (bytes, bytearray)):
        try:
            document = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FbiReleaseError("FBI CDE payload is not valid JSON") from exc
    else:
        document = payload
    if not isinstance(document, dict):
        raise FbiReleaseError("FBI CDE payload must be a JSON object")
    properties = document.get("cde_properties")
    if not isinstance(properties, dict):
        raise FbiReleaseError("FBI CDE payload has no cde_properties block")
    refresh = properties.get("last_refresh_date")
    latest = properties.get("max_data_date")
    if not isinstance(refresh, dict) or not isinstance(latest, dict):
        raise FbiReleaseError("FBI CDE freshness fields are missing")
    refresh_value = refresh.get(PROGRAM_KEY)
    latest_value = latest.get(PROGRAM_KEY)
    if not isinstance(refresh_value, str) or not _REFRESH_DATE.fullmatch(refresh_value):
        raise FbiReleaseError("FBI CDE last_refresh_date.UCR is invalid")
    if not isinstance(latest_value, str) or not _DATA_MONTH.fullmatch(latest_value):
        raise FbiReleaseError("FBI CDE max_data_date.UCR is invalid")
    month, day, year = refresh_value.split("/")
    return FbiRelease(date(int(year), int(month), int(day)), latest_value)


def _period_ordinal(period: str) -> int:
    month, year = period.split("-")
    return int(year) * 12 + int(month) - 1


def decide_release(
    product: FbiUcrProduct,
    current: FbiRelease | None,
    previous: FbiRelease | None,
) -> ReleaseDecision:
    """Return the safe next action without performing I/O."""
    if current is None:
        return ReleaseDecision.MISSING_RELEASE_QUARANTINE
    if _period_ordinal(current.max_data_period) < _period_ordinal(product.period_end):
        # The registered window is not fully published yet. Capturing it would
        # produce months that look non-reporting purely because the provider
        # has not released them.
        return ReleaseDecision.PERIOD_UNAVAILABLE_QUARANTINE
    if previous is None:
        return ReleaseDecision.INGEST
    if current.refresh_date < previous.refresh_date:
        return ReleaseDecision.BACKWARD_REFRESH_QUARANTINE
    if current.refresh_date == previous.refresh_date:
        return ReleaseDecision.UNCHANGED
    return ReleaseDecision.INGEST


def load_latest_accepted_release(
    connection_factory: Callable[[], Any],
    product: FbiUcrProduct,
) -> FbiRelease | None:
    """Load the latest safe release identity for revision comparison."""
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT refresh_date, max_data_month
                FROM control.fbi_ucr_release
                WHERE product_id = %s
                  AND decision IN ('ingest', 'unchanged')
                  AND status IN ('captured', 'silver_ready', 'published')
                ORDER BY refresh_date DESC, created_at DESC
                LIMIT 1
                """,
                (product.product_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return FbiRelease(row[0], row[1])
    finally:
        database_connection.close()
