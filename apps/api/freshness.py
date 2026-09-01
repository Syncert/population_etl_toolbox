"""Publication-epoch provider for the response cache (API-006).

The cache key includes an epoch derived from the warehouse's published
harvest state, so a republication rotates the key and a stale body cannot be
served for the whole TTL. The epoch reads
``gold_glossary.publisher_harvest_state`` — the serving-side, one-row-per-
source mirror of the publication lifecycle that the read-only API role is
granted. The API must not (and cannot) read ``control.publisher_ready_event``;
the glossary mirror exists precisely so consumers never need to.

The lookup is memoized for ``freshness_seconds``, which is therefore the
declared staleness bound after a publication: tighter than the response TTL,
and cheap enough that a cache hit stays a cache hit rather than becoming a
database round trip. A failed lookup keeps the last known epoch (or a
constant before any succeeds) and is retried only after the window — Redis
and the epoch are optimizations, and neither may take availability down.
"""

from __future__ import annotations

import logging
import time

from sqlalchemy import text
from starlette.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)

#: The published harvest-state contract the epoch reads. One row per source;
#: created unconditionally by the bootstrap manifest (migration 002).
PUBLICATION_STATE_RELATION = "gold_glossary.publisher_harvest_state"

#: Served before any successful lookup: requests are cacheable immediately and
#: converge to the real epoch once the warehouse answers.
UNKNOWN_EPOCH = "epoch-unknown"

_EPOCH_QUERY = text(
    f"""
    SELECT COALESCE(
        TO_CHAR(MAX(last_publication_time), 'YYYYMMDDHH24MISSUS'),
        'never-published'
    )
    FROM {PUBLICATION_STATE_RELATION}
    """
)


class PublicationEpochProvider:
    """Memoized epoch reads over the API engine, safe to call per request."""

    def __init__(
        self,
        freshness_seconds: int,
        clock=time.monotonic,
    ) -> None:
        self._freshness_seconds = max(0, freshness_seconds)
        self._clock = clock
        self._epoch = UNKNOWN_EPOCH
        self._read_at: float | None = None

    def _read_epoch(self) -> str:
        from apps.api.database import get_db_session

        for session in get_db_session():
            return str(session.execute(_EPOCH_QUERY).scalar() or "never-published")
        return UNKNOWN_EPOCH  # pragma: no cover - generator always yields

    def _refresh(self) -> str:
        try:
            self._epoch = self._read_epoch()
        except Exception:
            # Keep the last known epoch: an unreachable warehouse must not
            # take cache hits (or the request itself) down, and the window
            # prevents hammering a struggling database with epoch probes.
            logger.warning("publication epoch refresh failed; keeping %r", self._epoch)
        self._read_at = self._clock()
        return self._epoch

    async def __call__(self) -> str:
        now = self._clock()
        if (
            self._read_at is not None
            and (now - self._read_at) < self._freshness_seconds
        ):
            return self._epoch
        return await run_in_threadpool(self._refresh)
