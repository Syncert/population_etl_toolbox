"""Publication-epoch provider for the response cache (API-006).

The cache key includes an epoch derived from the warehouse's published
harvest state, so a republication rotates the key and a stale body cannot be
served for the whole TTL. The epoch reads
``gold_glossary.publisher_harvest_state`` — the serving-side, one-row-per-
source mirror of the publication lifecycle that the read-only API role is
granted. The API must not (and cannot) read ``control.publisher_ready_event``;
the glossary mirror exists precisely so consumers never need to.

The epoch is a digest of the whole recorded state rather than the newest
publication time in it. ``last_publication_time`` is the publisher's own
declared time, carried through from the ready event, and migration 016 wrote
down why it is not enough on its own: a change to what a publisher *says* --
a metric's identity, units, grains, lineage, the set of keys it emits --
moves no publication time, which is why the harvest guard gained a content
fingerprint. An epoch reading only the first input rotated nothing in exactly
the case that migration exists for. A maximum also assumes the seven
publishers share one clock, so a source republishing behind another left the
key where it was.

The lookup is memoized for ``freshness_seconds``, which is therefore the
declared staleness bound after a publication: tighter than the response TTL,
and cheap enough that a cache hit stays a cache hit rather than becoming a
database round trip. A failed lookup keeps the last known epoch (or a
constant before any succeeds) and is retried only after the window — Redis
and the epoch are optimizations, and neither may take availability down.
"""

from __future__ import annotations

import hashlib
import json
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

#: Answered when the harvest state records nothing at all. Nothing published
#: is a state like any other, and a cacheable one.
NEVER_PUBLISHED = "never-published"

#: The recorded publication state, projected whole. The epoch rule lives in
#: Python rather than in SQL so it is provable without a database: a digest
#: is a rule about what counts as a change, and that is the part worth
#: testing. Seven rows once per freshness window is not a cost worth trading
#: it for.
_STATE_QUERY = text(
    f"""
    SELECT source_code,
           last_publication_time::TEXT AS last_publication_time,
           last_content_fingerprint,
           last_source_watermark
    FROM {PUBLICATION_STATE_RELATION}
    ORDER BY source_code
    """
)

#: How much of the digest travels in a cache key. Sixteen hex characters is
#: the same width the served-contract fingerprint uses, and a collision costs
#: one stale body for at most one TTL -- not a security boundary.
_EPOCH_WIDTH = 16


def publication_epoch(rows) -> str:
    """A stable, opaque token for one reading of the publication state.

    Changes when any source's recorded state changes -- its publication time,
    its content fingerprint, or its source watermark -- and stays put when
    none of them do, which is the one property a cache key needs. Rows are
    sorted here as well as in the query: the order they arrive in is not part
    of the state.

    The state is serialized as JSON before hashing so that field boundaries
    are unambiguous: a separator-joined string lets two different states --
    one field ending where the next begins -- digest the same.
    """
    state = sorted(
        [
            str(row["source_code"]),
            _text_or_empty(row["last_publication_time"]),
            _text_or_empty(row["last_content_fingerprint"]),
            _text_or_empty(row["last_source_watermark"]),
        ]
        for row in rows
    )
    if not state:
        return NEVER_PUBLISHED
    payload = json.dumps(state, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:_EPOCH_WIDTH]


def _text_or_empty(value) -> str:
    """A recorded field as text; an unrecorded one as the empty string."""
    return "" if value is None else str(value)


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
            rows = session.execute(_STATE_QUERY).mappings().all()
            return publication_epoch(rows)
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
