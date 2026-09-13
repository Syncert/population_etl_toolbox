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

from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.registry import normalize_geo_level
from apps.api.schemas import AnalysisDocument
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
    return None


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
    verdicts: dict[str, Optional[str]] = {}
    for block in packet.blocks:
        if block.document is None:
            continue
        key = block.document.model_dump_json()
        if key not in verdicts:
            try:
                validate_document(warehouse, block.document)
                verdicts[key] = None
            except ConfigurationInvalid as exc:
                verdicts[key] = exc.detail
        if verdicts[key]:
            raise PacketInvalid(f"block '{block.block_id}': {verdicts[key]}")


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


def create_packet(
    storage: Session,
    warehouse: Session,
    owner_user_id: int,
    name: str,
    document: EvidencePacketDocument,
) -> EvidencePacket:
    validate_packet(warehouse, document)
    _refuse_taken_name(storage, owner_user_id, name, packet_id=-1)
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
