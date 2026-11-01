"""What a metro page needs before it reads a figure (web-metro-pages).

A metro (a CBSA) is not inside one state, so the place pages' address rule
cannot reach it. These reads give it one: ``/metro/<segment>``, where the
segment is the slug of the published name, or the five-digit CBSA code when
another metro's name slugs the same way (``place_paths.metro_segments``).

``resolve_metro`` answers one page: the metro, its member counties under the
newest delineation the reference holds, the states they lie in, and the
areas its principal state lies in. ``list_metros`` answers the metros with a
member county in one state, for a state page's list. Both read the search
projection and the relationship view only (gold relations), and every
relationship read starts from known keys.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from sqlalchemy.orm import Session

from apps.api.schemas import (
    MetroArea,
    MetroListResponse,
    MetroMemberCounty,
    MetroPageResponse,
    MetroState,
    MetroSummary,
)
from apps.api.services.contracts import require_relation
from apps.api.services.place_paths import (
    PagePaths,
    SegmentRow,
    cbsa_code,
    metro_segments,
)
from data_ingestion_toolbox.sql.catalog_queries import (
    GEO_SEARCH_RELATION,
    GEO_SEARCH_SEGMENTS_QUERY,
    GEOGRAPHY_RELATION,
    METRO_MEMBER_COUNTIES_QUERY,
    METRO_PAGES_QUERY,
    METRO_STATE_MEMBERS_QUERY,
    RELATIONSHIP_RELATION,
    STATE_CONTAINING_AREAS_QUERY,
)

MAX_LIMIT = 1000
DEFAULT_LIMIT = 100


class MetroRefusal(ValueError):
    """A request this resource does not answer."""


def _number(value: Any) -> Optional[float]:
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _year(value: Any) -> Optional[int]:
    return None if value is None else int(value)


class _Metros:
    """Every current metro and its segment, read once per request."""

    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        self.rows = [row for row in rows if cbsa_code(str(row["geo_id"])) is not None]
        self.segments = metro_segments(
            SegmentRow.from_mapping(row) for row in self.rows
        )
        self.by_id = {str(row["geo_id"]): row for row in self.rows}

    def summary(self, row: Mapping[str, Any]) -> MetroSummary:
        geo_id = str(row["geo_id"])
        segment = self.segments[geo_id]
        return MetroSummary(
            geo_id=geo_id,
            geo_name=str(row["geo_name"]),
            short_name=str(row.get("short_name") or row["geo_name"]),
            cbsa_code=str(cbsa_code(geo_id)),
            segment=segment,
            page_path=f"/metro/{segment}",
            principal_state_fips=row.get("state_fips"),
            principal_state_name=row.get("state_name"),
            population=_number(row.get("population")),
            population_year=_year(row.get("population_year")),
            population_metric_code=row.get("population_metric_code"),
        )

    def find(self, segment: str) -> Optional[Mapping[str, Any]]:
        """A metro by its canonical segment or by its CBSA code."""
        wanted = segment.strip().lower()
        for geo_id, own in self.segments.items():
            if own == wanted:
                return self.by_id[geo_id]
        return self.by_id.get(f"cbsa:{wanted}") if wanted.isdigit() else None


def _metros(db: Session) -> _Metros:
    require_relation(db, GEO_SEARCH_RELATION)
    return _Metros(db.execute(METRO_PAGES_QUERY).mappings().all())


def list_metros(
    db: Session,
    state_fips: Optional[str] = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
) -> MetroListResponse:
    """Metros, by name; with ``state_fips``, those with a member county there."""
    state = (state_fips or "").strip() or None
    if state is not None and not (len(state) == 2 and state.isdigit()):
        raise MetroRefusal("state_fips must be a two-digit state FIPS code")
    metros = _metros(db)
    rows = metros.rows
    if state is not None:
        require_relation(db, RELATIONSHIP_RELATION)
        members = {
            str(row["metro_geo_id"])
            for row in db.execute(METRO_STATE_MEMBERS_QUERY, {"state_fips": [state]})
            .mappings()
            .all()
        }
        rows = [row for row in rows if str(row["geo_id"]) in members]
    ordered = sorted(rows, key=lambda row: (str(row["geo_name"]), str(row["geo_id"])))
    page = ordered[offset : offset + limit]
    return MetroListResponse(
        total=len(ordered),
        limit=limit,
        offset=offset,
        items=[metros.summary(row) for row in page],
    )


def resolve_metro(db: Session, segment: str) -> Optional[MetroPageResponse]:
    """One metro page's identity and neighbourhood, or ``None`` for no metro."""
    metros = _metros(db)
    found = metros.find(segment)
    if found is None:
        return None
    require_relation(db, RELATIONSHIP_RELATION)
    require_relation(db, GEOGRAPHY_RELATION)
    metro = metros.summary(found)
    members = (
        db.execute(METRO_MEMBER_COUNTIES_QUERY, {"metro_geo_id": metro.geo_id})
        .mappings()
        .all()
    )
    principal = metro.principal_state_fips
    counts = Counter(str(row["state_fips"]) for row in members if row.get("state_fips"))
    state_fips = set(counts) | ({principal} if principal else set())
    paths = PagePaths(
        db.execute(GEO_SEARCH_SEGMENTS_QUERY, {"state_fips": sorted(state_fips)})
        .mappings()
        .all()
    )
    names = {
        str(row["state_fips"]): row.get("state_name")
        for row in members
        if row.get("state_fips")
    }
    if principal:
        names.setdefault(principal, metro.principal_state_name)
    states = sorted(
        (
            MetroState(
                geo_id=f"state:{fips}",
                state_fips=fips,
                state_name=names.get(fips),
                page_path=paths.path(f"state:{fips}"),
                member_counties=counts.get(fips, 0),
                principal=fips == principal,
            )
            for fips in state_fips
        ),
        key=lambda item: (
            not item.principal,
            -item.member_counties,
            item.state_name or "",
        ),
    )
    areas: list[MetroArea] = []
    if principal:
        areas = [
            MetroArea(
                geo_id=str(row["geo_id"]),
                geo_level=str(row["geo_level"]).upper(),
                geo_name=row.get("geo_name"),
            )
            for row in db.execute(
                STATE_CONTAINING_AREAS_QUERY, {"state_geo_id": f"state:{principal}"}
            )
            .mappings()
            .all()
        ]
    vintages = [
        int(row["geography_vintage"])
        for row in members
        if row.get("geography_vintage") is not None
    ]
    evidence = sorted(
        {str(row["evidence_source"]) for row in members if row.get("evidence_source")}
    )
    return MetroPageResponse(
        requested_segment=segment,
        metro=metro,
        delineation_vintage=max(vintages) if vintages else None,
        evidence_source=", ".join(evidence) or None,
        member_counties=[
            MetroMemberCounty(
                geo_id=str(row["geo_id"]),
                geo_name=str(row["geo_name"]),
                state_fips=row.get("state_fips"),
                state_name=row.get("state_name"),
                page_path=paths.path(str(row["geo_id"])),
                population=_number(row.get("population")),
                population_year=_year(row.get("population_year")),
            )
            for row in members
        ],
        states=states,
        principal_state_areas=areas,
    )
