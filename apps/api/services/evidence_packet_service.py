"""Evidence packet storage and validation (ADR-0004).

The same two disciplines as ``saved_analysis_service``: ownership is enforced
in SQL, never after the fact, and a stored document is validated against live
contracts on write and re-validated -- reported, never repaired -- on read.

One rule is this module's own and every check below follows from it:
**refuse contradictions, report incompleteness.** A packet is composed over
days, and a half-filled analytical block is the normal state of work in
progress; refusing to store one would make "save and come back to it"
impossible. But a block whose envelope names a measure its own query does not
ask for would display one measure's name over another measure's numbers --
the precise failure the envelope exists to prevent, invisible to the client,
and never made legitimate by later editing. That is refused at write, naming
the block.
"""

from __future__ import annotations

from typing import Optional
from urllib.parse import parse_qsl, urlsplit

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from apps.api.registry import grain_refusal, normalize_geo_level
from apps.api.schemas import AnalysisDocument
from apps.api.schemas.observations import OBSERVATION_FILTER_BOUNDS
from apps.api.schemas.evidence_packet import (
    MAX_ANALYTICAL_BLOCKS,
    BlockValidation,
    EvidencePacket,
    EvidencePacketDocument,
    EvidencePacketListResponse,
    EvidencePacketSummary,
    PacketBlock,
    PacketValidation,
)
from apps.api.services.neutral_observations_service import resolve_metrics
from apps.api.services.saved_analysis_service import (
    ConfigurationInvalid,
    validate_document,
)


class PacketInvalid(ValueError):
    """A packet the contracts refuse (HTTP 422). Names the block."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class PacketNotFound(LookupError):
    """No packet with that id is owned by the caller (HTTP 404)."""


class PacketConflict(Exception):
    """The stored version differs from the version the caller read (HTTP 409)."""

    def __init__(self, current_version: int) -> None:
        super().__init__(f"packet has moved to version {current_version}")
        self.current_version = current_version


class PacketNameTaken(Exception):
    """The caller already owns a packet with that name (HTTP 409)."""


class StorageQuotaReached(Exception):
    """The account already holds as many of these as it may (ADR-0005 s4).

    A bound on *how many*, beside ADR-0004's bound on how large one may be.
    Self-service registration turns "a handful of operator-issued accounts"
    into "every visitor", and an unbounded row count per account is the one
    cost that grows with the thing this platform is now inviting.

    Refused at the create rather than trimmed: deciding which of a reader's
    own saved analyses to destroy is not a decision this code gets to make.
    """

    def __init__(self, limit: int) -> None:
        super().__init__(f"this account may hold at most {limit}")
        self.limit = limit


# ---------------------------------------------------------------------------
# Validation: contradictions refused, incompleteness reported
# ---------------------------------------------------------------------------

#: Envelope fields an analytical block needs before it can be read as
#: evidence. The same list the client's ``packetIssues`` reports.
_REQUIRED_ENVELOPE_FIELDS = (
    "metric_codes",
    "source_codes",
    "geo_id",
    "period",
    "api_query",
)


def _document_metric_codes(document: AnalysisDocument) -> set[str]:
    return {
        code
        for code in (
            document.metric_code,
            document.metric_code_a,
            document.metric_code_b,
        )
        if code
    }


def _stray_sources(
    block: PacketBlock, read_by_the_query: frozenset[str]
) -> Optional[str]:
    """A source the envelope names that the block's query never read.

    The same crossing `_contradiction` makes for the envelope's measures, one
    field over. The sources are not composer opinion -- they are decided by
    which measures the query asks for -- and `EvidenceEnvelope` renders them
    to a reader as "Sources" while `packetExport` writes them into the file
    the packet is handed over as. A packet whose every number came from FRED
    and whose envelope says BLS is one identity over another's numbers, which
    is this module's opening rule (API-113).

    Case is not a contradiction: the catalog publishes upper-case codes and a
    composer's record of the same source is the same fact.

    An envelope naming nothing is incompleteness, not a contradiction:
    `_REQUIRED_ENVELOPE_FIELDS` carries `source_codes`, so the read reports
    the field as missing and the block stays stored.
    """
    if block.envelope is None:
        return None
    named = {code.upper() for code in block.envelope.source_codes if code}
    stray = sorted(named - read_by_the_query)
    if not stray:
        return None
    return (
        f"block '{block.block_id}' names source(s) {', '.join(stray)} in its "
        "envelope that its query does not read"
    )


#: How the recorded request's own words read as a boolean. The reductions
#: travel in a URL as text, and these are the tokens the request layer itself
#: accepts for a boolean query parameter; a token outside them is not read
#: rather than guessed, because a rule that guesses is a rule that can refuse
#: a block for something the composer never said.
_TRUE_TOKENS = frozenset({"true", "1", "yes", "on"})
_FALSE_TOKENS = frozenset({"false", "0", "no", "off"})


def _recorded_boolean(token: str) -> Optional[bool]:
    word = token.strip().lower()
    if word in _TRUE_TOKENS:
        return True
    if word in _FALSE_TOKENS:
        return False
    return None


def _recorded_request_contradiction(block: PacketBlock) -> Optional[str]:
    """The recorded request asking for something the block's query does not.

    ``api_query`` is the one envelope field a reader *uses*: `EvidenceEnvelope`
    presents it under "Reproducible request" and the export writes it into the
    file the packet is handed over as, so it is the request someone re-derives
    the block from. Every other duplicated request parameter is crossed
    against the query above; this one was not, and it is the same fact spelled
    out in full.

    The failure is not hypothetical. WEB-048 records it happening on the
    client: a map block's envelope recorded `newest_per_geography=true` in its
    `api_query` while the document beside it asked for no reduction, so "the
    block did not reproduce the request its own envelope names, in the one
    resource whose purpose is that a reader can re-derive the evidence without
    this application". That was fixed by building both from one place, which
    holds for one client. Storage is not a back door for a claim the API would
    refuse -- the reason API-117 gave for checking a stored filter name and
    API-123 for checking its value -- and this is the same check for the
    request itself (API-129).

    The comparison is asymmetric, and deliberately:

    - A parameter the recorded request names and the query does not ask for is
      refused. The query would then answer a *wider* set than the reader's own
      request returns -- every stratum where the request named one -- so the
      block does not reproduce its own numbers.
    - A filter the query asks for and the request does not name is not.
      ``api_query`` records the request the view issued, and a block narrating
      one geography of that view carries `geo_id` in its query while the map's
      request never sent one; the envelope's `geo_id` field records exactly
      that narrowing.
    - The resource path is not compared at all. One document is legitimately
      served by more than one path -- a source-scoped `/{source}/observations/
      latest` and the neutral `/observations` answer the same question, and a
      comparison records `/comparison/preflight` until the pair is comparable
      -- so a path rule would refuse requests that reproduce the block.
    """
    envelope, document = block.envelope, block.document
    if envelope is None or document is None:
        return None
    recorded = envelope.api_query.strip()
    if "?" not in recorded:
        # Nothing this can read as a request: a path with no parameters, or a
        # composer's note. An unfilled `api_query` is incompleteness, which
        # `_validation_state` reports on read, and a string this cannot parse
        # is not a contradiction it may claim.
        return None
    named = {
        name: value
        for name, value in parse_qsl(urlsplit(recorded).query, keep_blank_values=True)
    }

    def refusal(field: str, recorded_value: str, asked: str) -> str:
        if asked:
            return (
                f"block '{block.block_id}' records a request for "
                f"{field}='{recorded_value}' but its query asks for '{asked}'"
            )
        return (
            f"block '{block.block_id}' records a request for "
            f"{field}='{recorded_value}' but its query does not ask for "
            f"{field}, so replaying it answers a wider set than the recorded "
            f"request does"
        )

    for field in ("metric_code", "metric_code_a", "metric_code_b"):
        recorded_value = (named.get(field) or "").strip()
        if not recorded_value:
            continue
        asked = str(getattr(document, field) or "").strip()
        if recorded_value != asked:
            return refusal(field, recorded_value, asked)

    recorded_scope = (named.get("scope") or "").strip()
    if recorded_scope and recorded_scope != document.scope:
        return refusal("scope", recorded_scope, document.scope)

    recorded_release = (named.get("release") or "").strip()
    if recorded_release and recorded_release != str(document.release or ""):
        return refusal("release", recorded_release, str(document.release or ""))

    for field in ("newest_per_geography", "newest_release_per_period"):
        recorded_reduction = _recorded_boolean(named.get(field) or "")
        if recorded_reduction is None:
            continue
        asked_reduction = bool(getattr(document, field))
        if recorded_reduction != asked_reduction:
            return (
                f"block '{block.block_id}' records a request for {field}="
                f"{str(recorded_reduction).lower()} but its query asks for "
                f"{str(asked_reduction).lower()}"
            )

    # The filters, under the names the observation resource declares them by,
    # so a filter added there is crossed here without being named again.
    filters = document.filters or {}
    for field in sorted(set(named) & set(OBSERVATION_FILTER_BOUNDS)):
        recorded_value = (named.get(field) or "").strip()
        if not recorded_value:
            continue
        asked = str(filters.get(field) or "").strip()
        if field == "geo_level":
            # Through the one vocabulary mapping, for the reason the envelope's
            # own grain is: a request recorded when the catalog published
            # `NATION` and a query asking for `NATIONAL` name one grain.
            agrees = bool(asked) and normalize_geo_level(
                recorded_value
            ) == normalize_geo_level(asked)
        else:
            agrees = recorded_value == asked
        if not agrees:
            return refusal(field, recorded_value, asked)
    return None


def _contradiction(block: PacketBlock) -> Optional[str]:
    """The reason a block can never be stored, or ``None``."""
    if not block.analytical:
        if block.document is not None or block.envelope is not None:
            return (
                f"block '{block.block_id}' is {block.type} prose and cannot carry "
                "a query or an envelope"
            )
        return None
    if block.envelope is None or block.document is None:
        # Incomplete, not contradictory: stored and reported on read.
        return None
    envelope, document = block.envelope, block.document
    asked = _document_metric_codes(document)
    named = set(envelope.metric_codes)
    stray = sorted(named - asked)
    if stray:
        return (
            f"block '{block.block_id}' names measure(s) {', '.join(stray)} in its "
            "envelope that its query does not ask for"
        )
    if envelope.scope != document.scope:
        return (
            f"block '{block.block_id}' records scope '{envelope.scope}' but its "
            f"query asks for '{document.scope}'"
        )
    if (envelope.release or "") != (document.release or ""):
        return (
            f"block '{block.block_id}' records release '{envelope.release}' but its "
            f"query asks for '{document.release or ''}'"
        )
    # The reduction is the third duplicated request parameter, and it decides
    # how many rows the query answers. A map block composed from
    # `newest_per_geography=true` records the one period it showed; stored
    # with the document's reduction left at its default it replays as the
    # whole publication -- every estimated year of the vintage -- under an
    # envelope that declares one period. That is a different set of rows than
    # the packet argued from (API-120).
    for reduction in ("newest_per_geography", "newest_release_per_period"):
        recorded = bool(getattr(envelope, reduction))
        queried = bool(getattr(document, reduction))
        if recorded != queried:
            return (
                f"block '{block.block_id}' records {reduction}="
                f"{str(recorded).lower()} but its query asks for "
                f"{str(queried).lower()}"
            )
    # The geography is a request parameter, not an observation about what the
    # source published: the same names the block's own `filters` carries. Left
    # uncrossed, a packet could store one geography's name over another
    # geography's numbers -- the failure this module's opening rule names, one
    # identity over (API-099).
    #
    # Only when both sides name one. A block still being composed records what
    # it has, and a block narrating one row of a many-geography answer is not
    # contradicting itself; two different answers to the same question are,
    # which is how the scope and release checks above already read.
    filters = document.filters or {}
    queried_geo_id = str(filters.get("geo_id") or "")
    if envelope.geo_id and queried_geo_id and envelope.geo_id != queried_geo_id:
        return (
            f"block '{block.block_id}' records geography '{envelope.geo_id}' but "
            f"its query asks for '{queried_geo_id}'"
        )
    queried_grain = str(filters.get("geo_level") or "")
    # A grain the API does not serve, recorded as the basis of numbers it
    # does. The vocabulary is closed and the envelope holds this field as a
    # request parameter, so the rule API-122 applies to the request and
    # API-123 to a stored query applies here too: a block whose query names
    # no grain stored `geo_level: "COUNTRY"` clean, read back `valid: true`,
    # and `EvidenceEnvelope` presented it to the reader as the packet's
    # geography -- with nothing anywhere that could tell them it is not one
    # (API-125). Refused before the comparison below, so the block is told
    # what is wrong with the word rather than which other word it differs
    # from.
    if envelope.geo_level:
        refusal = grain_refusal("geo_level", envelope.geo_level)
        if refusal is not None:
            return (
                f"block '{block.block_id}' records geography grain "
                f"'{envelope.geo_level}', which is not a grain: {refusal}"
            )
    if (
        envelope.geo_level
        and queried_grain
        # Through the one vocabulary mapping: API-092 promised the words it
        # replaced keep answering, so an envelope composed when the catalog
        # published `NATION` and a query asking for `NATIONAL` name one grain.
        and normalize_geo_level(envelope.geo_level)
        != normalize_geo_level(queried_grain)
    ):
        return (
            f"block '{block.block_id}' records geography grain "
            f"'{envelope.geo_level}' but its query asks for '{queried_grain}'"
        )
    # Last, so a disagreement the fields above can name is named in their
    # terms rather than as a difference between two URLs.
    return _recorded_request_contradiction(block)


def _metric_codes_in(packet: EvidencePacketDocument) -> list[str]:
    """Every metric code any block's query names, in the order they appear.

    Read from the dumped documents rather than from a list of field names:
    the analysis documents carry `metric_code`, `metric_code_a`,
    `metric_code_b` and a `series` list that carries more, and a field added
    later would otherwise quietly fall out of the batch and back into a round
    trip per block.
    """

    def _walk(value: object, into: list[str]) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if isinstance(key, str) and key.startswith("metric_code"):
                    if isinstance(item, str) and item:
                        into.append(item)
                else:
                    _walk(item, into)
        elif isinstance(value, (list, tuple)):
            for item in value:
                _walk(item, into)

    codes: list[str] = []
    for block in packet.blocks:
        if block.document is not None:
            _walk(block.document.model_dump(), codes)
    return list(dict.fromkeys(codes))


def validate_packet(warehouse: Session, packet: EvidencePacketDocument) -> None:
    """Raise ``PacketInvalid`` for anything the ADR's contradiction table refuses.

    Deliberately does *not* refuse an analytical block that is still empty:
    that is incompleteness, and ``_validation_state`` reports it on read.
    """
    seen: set[str] = set()
    analytical = 0
    for block in packet.blocks:
        if block.block_id in seen:
            raise PacketInvalid(f"block id '{block.block_id}' appears more than once")
        seen.add(block.block_id)
        if block.analytical:
            analytical += 1
        reason = _contradiction(block)
        if reason:
            raise PacketInvalid(reason)
    if analytical > MAX_ANALYTICAL_BLOCKS:
        raise PacketInvalid(
            f"a packet may carry at most {MAX_ANALYTICAL_BLOCKS} analytical blocks"
        )

    # Each block's query is validated by the one definition of "a query this
    # API accepts". Codes repeat across a packet -- a needs assessment reuses
    # three or four measures over a dozen blocks -- so each distinct document
    # is checked once and its verdict reused.
    # Every measure the packet names, resolved in one statement before any
    # block is validated (API-147). `validate_document` still asks for each
    # code it needs; the session memo answers from this read. A packet at the
    # declared cap used to issue a round trip per code per distinct block.
    resolve_metrics(warehouse, _metric_codes_in(packet))

    verdicts: dict[str, Optional[str]] = {}
    sources: dict[str, frozenset[str]] = {}
    for block in packet.blocks:
        if block.document is None:
            continue
        key = block.document.model_dump_json()
        if key not in verdicts:
            try:
                # The sources the query reads come back from the validation
                # that already resolved its measures, so crossing the
                # envelope against them costs no further lookup.
                sources[key] = validate_document(warehouse, block.document)
                verdicts[key] = None
            except ConfigurationInvalid as exc:
                verdicts[key] = exc.detail
        if verdicts[key]:
            raise PacketInvalid(f"block '{block.block_id}': {verdicts[key]}")
        stray = _stray_sources(block, sources[key])
        if stray:
            raise PacketInvalid(stray)


def _block_state(
    block: PacketBlock, warehouse: Session, verdicts: dict
) -> BlockValidation:
    if not block.analytical:
        return BlockValidation(block_id=block.block_id, valid=True)
    if block.envelope is None:
        return BlockValidation(
            block_id=block.block_id,
            valid=False,
            reason="this block presents no analysis yet, so it carries no reproducibility envelope",
            missing=list(_REQUIRED_ENVELOPE_FIELDS),
        )
    envelope = block.envelope
    missing = []
    if not envelope.metric_codes:
        missing.append("metric_codes")
    if not envelope.source_codes:
        missing.append("source_codes")
    if not envelope.geo_id and not envelope.geo_level:
        missing.append("geo_id")
    if not envelope.period:
        missing.append("period")
    if not envelope.api_query:
        missing.append("api_query")
    if block.document is None:
        missing.append("document")
        return BlockValidation(
            block_id=block.block_id,
            valid=False,
            reason="this block records an envelope but no query to replay",
            missing=missing,
        )
    # A contradiction cannot be written, but it can be *read*: a packet stored
    # before a duplicated request parameter joined the envelope carries that
    # field at its default while its query carries the composer's answer. The
    # reduction is the case that made this reachable -- an envelope declaring
    # one period over a query that replays every estimated year (API-120) --
    # and it is reported first because it is a fact about the document itself,
    # decided without asking the warehouse anything.
    contradiction = _contradiction(block)
    if contradiction:
        return BlockValidation(
            block_id=block.block_id,
            valid=False,
            reason=contradiction,
            missing=missing,
        )
    key = block.document.model_dump_json()
    if key not in verdicts:
        try:
            validate_document(warehouse, block.document)
            verdicts[key] = None
        except ConfigurationInvalid as exc:
            verdicts[key] = exc.detail
    stale = verdicts[key]
    if stale:
        # Stale wins over incomplete in the reason: a retired measure is the
        # thing to fix first, and the missing list still travels beside it.
        return BlockValidation(
            block_id=block.block_id, valid=False, reason=stale, missing=missing
        )
    if missing:
        return BlockValidation(
            block_id=block.block_id,
            valid=False,
            reason="this block would present values without the context needed to read them",
            missing=missing,
        )
    return BlockValidation(block_id=block.block_id, valid=True)


def _validation_state(
    warehouse: Session, packet: EvidencePacketDocument
) -> PacketValidation:
    verdicts: dict[str, Optional[str]] = {}
    blocks = [_block_state(block, warehouse, verdicts) for block in packet.blocks]
    failing = [state for state in blocks if not state.valid]
    analytical = sum(1 for block in packet.blocks if block.analytical)
    if failing:
        return PacketValidation(
            valid=False,
            reason=f"{len(failing)} of {analytical} analytical blocks cannot be read as evidence",
            blocks=blocks,
        )
    return PacketValidation(valid=True, blocks=blocks)


# ---------------------------------------------------------------------------
# Owner-scoped storage
# ---------------------------------------------------------------------------

_INSERT = text(
    """
    INSERT INTO app_api.evidence_packet (
        owner_user_id, name, version, document
    ) VALUES (:owner_user_id, :name, 1, CAST(:document AS JSONB))
    RETURNING packet_id, name, version, created_at, updated_at
    """
)

_SELECT_ONE = text(
    """
    SELECT packet_id, name, version, document, created_at, updated_at
    FROM app_api.evidence_packet
    WHERE packet_id = :packet_id AND owner_user_id = :owner_user_id
    """
)

# One statement for the page and its total, for the reasons written out over
# `saved_analysis_service._SELECT_PAGE` (API-103): this engine writes, so it
# cannot take the warehouse engine's `REPEATABLE READ`, and a total counted
# in its own statement can miss a row the page carries.
_SELECT_PAGE = text(
    """
    WITH owned AS (
        SELECT packet_id, name, version, document, created_at, updated_at
        FROM app_api.evidence_packet
        WHERE owner_user_id = :owner_user_id
    ),
    counted AS (SELECT COUNT(*) AS total FROM owned),
    page AS (
        SELECT * FROM owned
        ORDER BY name, packet_id
        LIMIT :limit OFFSET :offset
    )
    SELECT counted.total,
           page.packet_id, page.name, page.version, page.document,
           page.created_at, page.updated_at
    FROM counted LEFT JOIN page ON TRUE
    ORDER BY page.name, page.packet_id
    """
)

_UPDATE = text(
    """
    UPDATE app_api.evidence_packet
    SET name = :name,
        document = CAST(:document AS JSONB),
        version = version + 1,
        updated_at = NOW()
    WHERE packet_id = :packet_id
      AND owner_user_id = :owner_user_id
      AND version = :expected_version
    RETURNING packet_id, name, version, created_at, updated_at
    """
)

_CURRENT_VERSION = text(
    """
    SELECT version FROM app_api.evidence_packet
    WHERE packet_id = :packet_id AND owner_user_id = :owner_user_id
    """
)

_DELETE = text(
    """
    DELETE FROM app_api.evidence_packet
    WHERE packet_id = :packet_id AND owner_user_id = :owner_user_id
    RETURNING packet_id
    """
)

_NAME_TAKEN = text(
    """
    SELECT 1 FROM app_api.evidence_packet
    WHERE owner_user_id = :owner_user_id AND name = :name
      AND packet_id <> :packet_id
    """
)


def _document_of(row) -> EvidencePacketDocument:
    return EvidencePacketDocument.model_validate(row["document"])


def _detail(
    row, document: EvidencePacketDocument, validation: PacketValidation
) -> EvidencePacket:
    return EvidencePacket(
        packet_id=int(row["packet_id"]),
        name=str(row["name"]),
        version=int(row["version"]),
        document=document,
        validation=validation,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _summary(row) -> EvidencePacketSummary:
    document = _document_of(row)
    return EvidencePacketSummary(
        packet_id=int(row["packet_id"]),
        name=str(row["name"]),
        version=int(row["version"]),
        block_count=len(document.blocks),
        analytical_block_count=sum(1 for block in document.blocks if block.analytical),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _refuse_taken_name(
    storage: Session, owner_user_id: int, name: str, packet_id: int
) -> None:
    taken = storage.execute(
        _NAME_TAKEN,
        {"owner_user_id": owner_user_id, "name": name, "packet_id": packet_id},
    ).first()
    if taken is not None:
        raise PacketNameTaken(name)


_COUNT_OWNED = text(
    """
    SELECT COUNT(*) FROM app_api.evidence_packet
    WHERE owner_user_id = :owner_user_id
    """
)


def create_packet(
    storage: Session,
    warehouse: Session,
    owner_user_id: int,
    name: str,
    document: EvidencePacketDocument,
    quota: int = 0,
) -> EvidencePacket:
    validate_packet(warehouse, document)
    if quota > 0:
        held = storage.execute(_COUNT_OWNED, {"owner_user_id": owner_user_id}).scalar()
        if int(held or 0) >= quota:
            raise StorageQuotaReached(quota)
    _refuse_taken_name(storage, owner_user_id, name, packet_id=-1)
    try:
        row = (
            storage.execute(
                _INSERT,
                {
                    "owner_user_id": owner_user_id,
                    "name": name,
                    "document": document.model_dump_json(),
                },
            )
            .mappings()
            .one()
        )
        storage.commit()
    except IntegrityError as conflict:
        # `_refuse_taken_name` is the friendly path and cannot be the whole
        # answer: two creates with the same name both pass it and one insert
        # meets `UNIQUE (owner_user_id, name)`. Uncaught, the router logs an
        # ERROR and answers the sanitized 503, telling a client that wrote a
        # legitimate conflict that the database is down (API-148).
        storage.rollback()
        raise PacketNameTaken(name) from conflict
    return _detail(row, document, _validation_state(warehouse, document))


def get_packet(
    storage: Session, warehouse: Session, owner_user_id: int, packet_id: int
) -> EvidencePacket:
    row = (
        storage.execute(
            _SELECT_ONE, {"packet_id": packet_id, "owner_user_id": owner_user_id}
        )
        .mappings()
        .first()
    )
    if row is None:
        raise PacketNotFound(packet_id)
    document = _document_of(row)
    return _detail(row, document, _validation_state(warehouse, document))


def list_packets(
    storage: Session, owner_user_id: int, limit: int, offset: int
) -> EvidencePacketListResponse:
    rows = (
        storage.execute(
            _SELECT_PAGE,
            {"owner_user_id": owner_user_id, "limit": limit, "offset": offset},
        )
        .mappings()
        .all()
    )
    total = int(rows[0]["total"]) if rows else 0
    return EvidencePacketListResponse(
        total=total,
        limit=limit,
        offset=offset,
        # The count's own row when the page is empty, carrying no packet.
        items=[_summary(row) for row in rows if row["packet_id"] is not None],
    )


def update_packet(
    storage: Session,
    warehouse: Session,
    owner_user_id: int,
    packet_id: int,
    name: str,
    document: EvidencePacketDocument,
    expected_version: int,
) -> EvidencePacket:
    validate_packet(warehouse, document)
    _refuse_taken_name(storage, owner_user_id, name, packet_id=packet_id)
    try:
        row = (
            storage.execute(
                _UPDATE,
                {
                    "packet_id": packet_id,
                    "owner_user_id": owner_user_id,
                    "name": name,
                    "document": document.model_dump_json(),
                    "expected_version": expected_version,
                },
            )
            .mappings()
            .first()
        )
    except IntegrityError as conflict:
        # A rename racing a create takes the same name by the same route.
        storage.rollback()
        raise PacketNameTaken(name) from conflict
    if row is None:
        storage.rollback()
        current = storage.execute(
            _CURRENT_VERSION, {"packet_id": packet_id, "owner_user_id": owner_user_id}
        ).scalar()
        if current is None:
            raise PacketNotFound(packet_id)
        raise PacketConflict(int(current))
    storage.commit()
    return _detail(row, document, _validation_state(warehouse, document))


def delete_packet(storage: Session, owner_user_id: int, packet_id: int) -> None:
    row = storage.execute(
        _DELETE, {"packet_id": packet_id, "owner_user_id": owner_user_id}
    ).first()
    storage.commit()
    if row is None:
        raise PacketNotFound(packet_id)
