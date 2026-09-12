"""Evidence packets: API-owned, user-scoped (ADR-0004).

A sibling of ``/analysis-configurations`` with the same discipline: every
route requires a bearer token and is scoped to the authenticated owner,
responses are ``private, no-store``, and the paths sit outside the cacheable
public prefixes so user content has no path into the shared response cache.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.auth import Account, get_app_session_dep, require_account
from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.schemas.evidence_packet import (
    EvidencePacket,
    EvidencePacketCreateRequest,
    EvidencePacketListResponse,
    EvidencePacketUpdateRequest,
)
from apps.api.services.evidence_packet_service import (
    PacketConflict,
    PacketInvalid,
    PacketNameTaken,
    PacketNotFound,
    create_packet,
    delete_packet,
    get_packet,
    list_packets,
    update_packet,
)

router = APIRouter(prefix="/evidence-packets", tags=["evidence-packets"])

NOT_FOUND_DETAIL = "packet not found"

_PRIVATE_CACHE = "private, no-store"


def _private(response: Response) -> None:
    response.headers["cache-control"] = _PRIVATE_CACHE


def _not_found() -> HTTPException:
    """The same answer for another owner's id as for one that never existed."""
    return HTTPException(status_code=404, detail=NOT_FOUND_DETAIL)


def _name_taken(name: str) -> HTTPException:
    return HTTPException(status_code=409, detail=f"a packet named '{name}' exists")


@router.get("", response_model=EvidencePacketListResponse)
def list_evidence_packets(
    response: Response,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0, le=100000),
    account: Account = Depends(require_account),
    storage: Session = Depends(get_app_session_dep),
) -> EvidencePacketListResponse:
    """The caller's own packets, ordered by name. Carries no validation verdict."""
    _private(response)
    try:
        return list_packets(
            storage, owner_user_id=account.user_account_id, limit=limit, offset=offset
        )
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.post("", response_model=EvidencePacket, status_code=201)
def create_evidence_packet(
    payload: EvidencePacketCreateRequest,
    response: Response,
    account: Account = Depends(require_account),
    storage: Session = Depends(get_app_session_dep),
    warehouse: Session = Depends(get_db_session_dep),
) -> EvidencePacket:
    """Store a packet, refusing contradictions and reporting incompleteness."""
    _private(response)
    try:
        return create_packet(
            storage,
            warehouse,
            owner_user_id=account.user_account_id,
            name=payload.name,
            document=payload.document,
        )
    except PacketInvalid as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    except PacketNameTaken as exc:
        raise _name_taken(payload.name) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get("/{packet_id}", response_model=EvidencePacket)
def get_evidence_packet(
    response: Response,
    packet_id: int = Path(..., ge=1),
    account: Account = Depends(require_account),
    storage: Session = Depends(get_app_session_dep),
    warehouse: Session = Depends(get_db_session_dep),
) -> EvidencePacket:
    """One packet, with every block's live state reported."""
    _private(response)
    try:
        return get_packet(
            storage, warehouse, owner_user_id=account.user_account_id, packet_id=packet_id
        )
    except PacketNotFound as exc:
        raise _not_found() from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.put("/{packet_id}", response_model=EvidencePacket)
def update_evidence_packet(
    payload: EvidencePacketUpdateRequest,
    response: Response,
    packet_id: int = Path(..., ge=1),
    account: Account = Depends(require_account),
    storage: Session = Depends(get_app_session_dep),
    warehouse: Session = Depends(get_db_session_dep),
) -> EvidencePacket:
    """Replace a packet, refusing a stale expected version."""
    _private(response)
    try:
        return update_packet(
            storage,
            warehouse,
            owner_user_id=account.user_account_id,
            packet_id=packet_id,
            name=payload.name,
            document=payload.document,
            expected_version=payload.expected_version,
        )
    except PacketInvalid as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    except PacketNotFound as exc:
        raise _not_found() from exc
    except PacketConflict as exc:
        raise HTTPException(
            status_code=409,
            detail=(
                "packet was modified; expected version "
                f"{payload.expected_version}, current version {exc.current_version}"
            ),
        ) from exc
    except PacketNameTaken as exc:
        raise _name_taken(payload.name) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.delete("/{packet_id}", status_code=204)
def delete_evidence_packet(
    packet_id: int = Path(..., ge=1),
    account: Account = Depends(require_account),
    storage: Session = Depends(get_app_session_dep),
) -> Response:
    """Delete the caller's packet outright; effective immediately."""
    try:
        delete_packet(storage, owner_user_id=account.user_account_id, packet_id=packet_id)
    except PacketNotFound as exc:
        raise _not_found() from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
    return Response(status_code=204, headers={"cache-control": _PRIVATE_CACHE})
