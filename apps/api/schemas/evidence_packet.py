"""Evidence packet contracts (ADR-0004).

A packet is an ordered composition of blocks -- narrative, methodology,
caveats, and analytical blocks that each carry a query plus the
reproducibility envelope the composer recorded. It is deliberately a separate
resource from a saved analysis configuration: a configuration is a live
question its owner re-asks, and a packet is a document its owner hands to
somebody else. Each analytical block therefore embeds its own
``AnalysisDocument`` rather than referencing a configuration, so editing a
configuration later can never silently rewrite what an issued packet argued.

Nothing here stores an observation value. A live block is still replayed
against the latest publication; what is frozen is the question.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from apps.api.schemas.saved_analysis import AnalysisDocument

PacketBlockType = Literal[
    "text", "analysis", "table", "map", "source-note", "methodology", "caveat"
]

#: Block kinds that present provider data and therefore carry a query and an
#: envelope. Every other kind is prose.
ANALYTICAL_BLOCK_TYPES: frozenset[str] = frozenset({"analysis", "table", "map"})

#: The ADR's bounds. The document cap is the binding constraint and is checked
#: as a request-body bound before parsing; these keep any single field sane.
MAX_BLOCKS = 100
MAX_ANALYTICAL_BLOCKS = 50


class ReproducibilityEnvelope(BaseModel):
    """What the composer recorded about a block, as it recorded it.

    Only the fields that duplicate the block's query are cross-checked against
    it: ``metric_codes``, ``scope``, ``release``, and the geography
    (``geo_id``, ``geo_level``) when the query filters to one. Those are
    request parameters the ``document`` carries under the same names. The rest
    -- ``period``, ``units``, ``source_codes``, ``transformation``,
    ``caveats`` -- are observations about what the source published when the
    block was composed; the API second-guessing them would substitute its
    present view for what the composer actually saw.
    """

    model_config = ConfigDict(extra="forbid")

    metric_codes: list[str] = Field(default_factory=list, max_length=8)
    source_codes: list[str] = Field(default_factory=list, max_length=8)
    geo_id: str = Field("", max_length=100)
    geo_level: str = Field("", max_length=50)
    scope: Literal["latest", "as_released"] = "latest"
    release: str = Field("", max_length=100)
    period: str = Field("", max_length=100)
    units: str = Field("", max_length=100)
    transformation: str = Field("none", max_length=200)
    api_query: str = Field("", max_length=2000)
    caveats: list[str] = Field(default_factory=list, max_length=20)


class PacketBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    block_id: str = Field(..., min_length=1, max_length=100)
    type: PacketBlockType
    title: str = Field("", max_length=200)
    content: str = Field("", max_length=20_000)
    envelope: Optional[ReproducibilityEnvelope] = None
    document: Optional[AnalysisDocument] = None
    #: Where this block came from. Informational only: never resolved, never
    #: a foreign key, so deleting a configuration cannot break a packet.
    source_configuration_id: Optional[int] = Field(default=None, ge=1)

    @property
    def analytical(self) -> bool:
        return self.type in ANALYTICAL_BLOCK_TYPES


class EvidencePacketDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    #: The document's own shape version. Deliberately not ``version``: the
    #: row's ``version`` is the optimistic-concurrency counter and the two
    #: would be confused at every call site.
    schema_version: Literal[1] = 1
    title: str = Field("", max_length=200)
    purpose: str = Field("", max_length=2_000)
    blocks: list[PacketBlock] = Field(default_factory=list, max_length=MAX_BLOCKS)


class BlockValidation(BaseModel):
    """One block's read-time state.

    ``missing`` names envelope fields the composer never filled (incomplete);
    ``reason`` carries the live contract's verdict (stale) or the incomplete
    explanation. They are different problems with different fixes, and a
    twelve-block packet with one retired measure should name the block.
    """

    block_id: str
    valid: bool
    reason: Optional[str] = None
    missing: list[str] = Field(default_factory=list)


class PacketValidation(BaseModel):
    valid: bool
    reason: Optional[str] = None
    blocks: list[BlockValidation] = Field(default_factory=list)


class EvidencePacketSummary(BaseModel):
    """List-view row: identity, lifecycle, and size -- never a verdict.

    Validating every row would multiply glossary lookups by page size on a
    route whose job is to let someone pick a packet. An absent verdict here
    means "not checked", never "valid".
    """

    model_config = ConfigDict(from_attributes=True)

    packet_id: int
    name: str
    version: int
    block_count: int
    analytical_block_count: int
    created_at: datetime
    updated_at: datetime


class EvidencePacketListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[EvidencePacketSummary]


class EvidencePacket(BaseModel):
    """Detail view: the stored document plus its per-block live state."""

    packet_id: int
    name: str
    version: int
    document: EvidencePacketDocument
    validation: PacketValidation
    created_at: datetime
    updated_at: datetime


class EvidencePacketCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, max_length=200)
    document: EvidencePacketDocument


class EvidencePacketUpdateRequest(BaseModel):
    """An update states the version it read; a mismatch is refused."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, max_length=200)
    document: EvidencePacketDocument
    expected_version: int = Field(..., ge=1)
