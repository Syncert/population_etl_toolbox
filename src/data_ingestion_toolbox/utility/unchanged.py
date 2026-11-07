"""Whether a fresh read only repeats the last published one.

Several loaders read the same file again on every scheduled run: the whole
registry (County Business Patterns, IRS migration, PEP) or a revision window
(SAIPE/SAHIE years, QCEW quarters, permit months). Their silver facts are
keyed by capture, so replaying identical bytes stored a second full copy of
every row each month -- the pattern that left 80 percent of the BEA silver
rows as repeats (PR #296). A read is ``unchanged`` when its bytes and parser
contract equal the newest published capture of the same file or slice and
no geography that capture names entered ``silver_ref.dim_geo_entity`` since
that publication (a replay then would resolve a row the earlier one left
``unmapped``). An unchanged read is recorded and replays nothing; the payload
store keeps one copy of the bytes either way. A file never published is
always replayed.

A geography added for another source does not count: until 2026-11-07 any
new ``dim_geo_entity`` row (an FBI agency ORI, say) made every QCEW, CBP,
permits, SAIPE/SAHIE and SOI read replay in full and store another copy.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

#: A geography added after the earlier publication could resolve a row that
#: publication left unmapped, so the read is replayed after all. This one
#: index-free probe answers "nothing was added" for most reads.
_GEOGRAPHY_SINCE_SQL = """
    SELECT EXISTS (
        SELECT 1 FROM silver_ref.dim_geo_entity WHERE created_at > %s
    )
"""

#: Whether a geography added since the publication is one its capture names.
#: The facts resolve ``revision.geo_id`` against ``dim_geo_entity`` at replay,
#: so a geo_id created after the publication was unmapped then. The revision
#: table's primary key leads with capture_id. ``{revision_table}`` is a module
#: constant of the caller, never user input.
_CAPTURE_GEOGRAPHY_SINCE_SQL = """
    SELECT EXISTS (
        SELECT 1
        FROM {revision_table} AS revision
        JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
        WHERE revision.capture_id = %s
          AND entity.created_at > %s
    )
"""


def repeats_last_publication(
    connection_factory: Callable[[], Any],
    latest_published_sql: str,
    parameters: Sequence[Any],
    *,
    payload_checksum: str,
    parser_contract: str | None,
    revision_table: str | None = None,
) -> bool:
    """Whether replaying these bytes would only repeat the last publication.

    ``latest_published_sql`` returns, for the same file or slice, the newest
    published capture's ``payload_checksum``, ``payload_schema_version`` and
    ``published_at`` (one row, or none when nothing was published). With
    ``revision_table`` it also returns that capture's ``capture_id``, and
    only a new geography the capture's revision rows name forces a replay;
    without it any new geography does.
    """
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(latest_published_sql, tuple(parameters))
            previous = cursor.fetchone()
            if previous is None:
                return False
            checksum, contract, published_at = previous[:3]
            if checksum != payload_checksum or contract != parser_contract:
                return False
            cursor.execute(_GEOGRAPHY_SINCE_SQL, (published_at,))
            if not bool(cursor.fetchone()[0]):
                return True
            if revision_table is None:
                return False
            cursor.execute(
                _CAPTURE_GEOGRAPHY_SINCE_SQL.format(revision_table=revision_table),
                (previous[3], published_at),
            )
            return not bool(cursor.fetchone()[0])
    finally:
        connection.close()


#: The newest earlier capture of the same request that was replayed (it has
#: revision rows). ``{revision_table}`` is a module constant of the caller,
#: never user input. The request-identity index on ``response_capture``
#: (source_code, request_fingerprint, ...) and the revision table's primary
#: key (capture_id first) make this two index probes.
_STORED_COPY_SQL = """
    SELECT capture.capture_id, capture.payload_checksum,
           capture.payload_schema_version
    FROM raw_capture.response_capture AS capture
    WHERE capture.source_code = %s
      AND capture.request_fingerprint = %s
      AND capture.capture_id <> %s
      AND EXISTS (
          SELECT 1 FROM {revision_table} AS revision
          WHERE revision.capture_id = capture.capture_id
      )
    ORDER BY capture.retrieved_at DESC, capture.capture_id DESC
    LIMIT 1
"""


def stored_copy_of_capture(
    connection_factory: Callable[[], Any],
    *,
    revision_table: str,
    source_code: str,
    request_fingerprint: str,
    capture_id: Any,
    payload_checksum: str,
    parser_contract: str | None,
) -> Any | None:
    """The earlier capture whose stored revision rows these bytes repeat.

    For a loader whose revision rows are source-shaped (provider codes, not
    resolved geography) and whose transform reads the newest revision of each
    cell, replaying a capture byte-identical to the newest replayed capture of
    the same request stores a second copy and changes nothing served. Returns
    that earlier capture's id, or ``None`` when the bytes or parser contract
    differ from the newest replayed capture, or when none was replayed.
    """
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                _STORED_COPY_SQL.format(revision_table=revision_table),
                (source_code, request_fingerprint, str(capture_id)),
            )
            previous = cursor.fetchone()
    finally:
        connection.close()
    if previous is None:
        return None
    previous_id, checksum, contract = previous
    if checksum != payload_checksum or contract != parser_contract:
        return None
    return previous_id
