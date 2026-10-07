"""Census regions, divisions and CBSAs: areas larger than a county.

The grocery-and-gasoline-prices plan needs geographies that contain states
and counties: BLS publishes prices for the four Census regions and nine
divisions, and BEA and BLS publish for metro areas. Both are defined by the
Census Bureau's own codes, and both are read here from Census files that
carry those codes beside their members:

* the Population Estimates state file (``NST-EST<v>-ALLDATA.csv``) lists each
  region (summary level 020) and division (030) and gives every state (040)
  its ``REGION`` and ``DIVISION``;
* the Population Estimates CBSA file (``cbsa-est<v>-alldata.csv``) lists each
  metropolitan and micropolitan statistical area by its OMB code, followed by
  its component counties by ``STCOU``, under the delineation the estimates
  use.

Membership is taken from those codes and never from a name. A member the
shared reference does not hold -- a county the delineation names that the
loaded county vintage lacks -- is recorded in
``silver_ref.geography_resolution`` as ``unmapped`` with the delineation that
named it, rather than dropped. Capture comes first: each file is stored byte
for byte, and the parse reads the stored bytes.
"""

from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import httpx
from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import (
    CaptureControl,
    ResponseCapture,
    load_captured_payload,
    persist_response_capture,
)
from data_ingestion_toolbox.silver_ref import geography_pipeline
from data_ingestion_toolbox.silver_ref.geography_contract import canonical_geo_id
from data_ingestion_toolbox.silver_ref.geography_pipeline import (
    HTTP_MAX_ATTEMPTS,
    SOURCE_CODE,
    GeographyRecord,
    GeographyRepository,
)

logger = logging.getLogger(__name__)

PARSER_VERSION = "census-area-geography-v1"
POPEST_ROOT = "https://www2.census.gov/programs-surveys/popest/datasets"

#: The statistical-area kinds the CBSA file lists that are CBSAs. Metropolitan
#: divisions are parts of a CBSA and are not loaded; counties are members.
CBSA_KINDS = frozenset(
    {"Metropolitan Statistical Area", "Micropolitan Statistical Area"}
)
COUNTY_MEMBER = "County or equivalent"


@dataclass(frozen=True)
class AreaRelease:
    """One Population Estimates vintage's area files."""

    vintage: int
    #: The OMB delineation the CBSA file's membership follows.
    delineation_vintage: int
    states_url: str
    cbsa_url: str


AREA_RELEASES: dict[int, AreaRelease] = {
    2024: AreaRelease(
        vintage=2024,
        # Vintage 2024 estimates use the July 2023 delineations.
        delineation_vintage=2023,
        states_url=f"{POPEST_ROOT}/2020-2024/state/totals/NST-EST2024-ALLDATA.csv",
        cbsa_url=f"{POPEST_ROOT}/2020-2024/metro/totals/cbsa-est2024-alldata.csv",
    ),
}
LATEST_AREA_RELEASE = max(AREA_RELEASES)


@dataclass
class AreaSnapshot:
    """Areas and their members, as one file published them."""

    records: list[GeographyRecord] = field(default_factory=list)
    #: ``(parent_geo_id, member_geo_id)`` pairs, from codes only.
    memberships: list[tuple[str, str]] = field(default_factory=list)
    evidence_source: str = ""
    vintage: int = 0
    #: The member type and code for each membership, for the ledger.
    member_codes: dict[str, tuple[str, str]] = field(default_factory=dict)


def _rows(payload: bytes) -> list[dict[str, str]]:
    # The estimates files are Latin-1 (place names carry accents).
    text = payload.decode("latin-1")
    return [
        {key.strip(): (value or "").strip() for key, value in row.items() if key}
        for row in csv.DictReader(io.StringIO(text))
    ]


def parse_regions_and_divisions(payload: bytes, *, vintage: int) -> AreaSnapshot:
    """Regions and divisions, and the states each contains, by Census code."""
    rows = _rows(payload)
    if not rows or not {"SUMLEV", "REGION", "DIVISION", "STATE", "NAME"} <= set(rows[0]):
        raise ValueError("the state estimates file lacks its geography columns")
    snapshot = AreaSnapshot(evidence_source="census_region_division_codes", vintage=vintage)
    for row in rows:
        level, region, division = row["SUMLEV"], row["REGION"], row["DIVISION"]
        if level == "020":
            geo_id = canonical_geo_id("census_region", area_code=region)
            snapshot.records.append(
                GeographyRecord(
                    "census_region", geo_id, region, None, None, None,
                    row["NAME"], vintage, area_code=region,
                )
            )
        elif level == "030":
            geo_id = canonical_geo_id("census_division", area_code=division)
            snapshot.records.append(
                GeographyRecord(
                    "census_division", geo_id, division, None, None, None,
                    row["NAME"], vintage, area_code=division,
                )
            )
            snapshot.memberships.append(
                (canonical_geo_id("census_region", area_code=region), geo_id)
            )
        elif level == "040":
            # Puerto Rico is in no region or division (`X`): it has no
            # membership rather than a guessed one.
            if region == "X" or division == "X":
                continue
            state = canonical_geo_id("state", state_fips=row["STATE"])
            snapshot.member_codes[state] = ("state", row["STATE"])
            snapshot.memberships.append(
                (canonical_geo_id("census_region", area_code=region), state)
            )
            snapshot.memberships.append(
                (canonical_geo_id("census_division", area_code=division), state)
            )
    regions = {r.area_code for r in snapshot.records if r.geo_type == "census_region"}
    divisions = {r.area_code for r in snapshot.records if r.geo_type == "census_division"}
    if len(regions) != 4 or len(divisions) != 9:
        raise ValueError(
            f"expected 4 regions and 9 divisions, found {len(regions)} and {len(divisions)}"
        )
    return snapshot


def parse_cbsa_delineation(payload: bytes, *, delineation_vintage: int) -> AreaSnapshot:
    """CBSAs by OMB code, and the counties each contains, by FIPS."""
    rows = _rows(payload)
    if not rows or not {"CBSA", "MDIV", "STCOU", "NAME", "LSAD"} <= set(rows[0]):
        raise ValueError("the CBSA estimates file lacks its delineation columns")
    snapshot = AreaSnapshot(
        evidence_source="census_cbsa_delineation", vintage=delineation_vintage
    )
    for row in rows:
        kind = row["LSAD"]
        if kind in CBSA_KINDS:
            geo_id = canonical_geo_id("metro", area_code=row["CBSA"])
            snapshot.records.append(
                GeographyRecord(
                    "metro", geo_id, row["CBSA"], None, None, None, row["NAME"],
                    delineation_vintage, lsad=kind, area_code=row["CBSA"],
                )
            )
        elif kind == COUNTY_MEMBER:
            stcou = row["STCOU"]
            if len(stcou) != 5 or not stcou.isdigit():
                raise ValueError(f"county member without a five-digit code: {stcou!r}")
            county = canonical_geo_id(
                "county", state_fips=stcou[:2], county_fips=stcou[2:]
            )
            snapshot.member_codes[county] = ("county", stcou)
            snapshot.memberships.append(
                (canonical_geo_id("metro", area_code=row["CBSA"]), county)
            )
    if not snapshot.records:
        raise ValueError("the CBSA estimates file lists no statistical area")
    return snapshot


def publish_area_snapshot(
    repository: GeographyRepository,
    snapshot: AreaSnapshot,
    *,
    capture_id: UUID,
    connection: Any,
) -> dict[str, int]:
    """Write the areas and every membership whose member the reference holds.

    A member the reference does not hold is recorded ``unmapped`` in the
    resolution ledger with the vintage that named it.
    """
    loaded = repository.load_attributes(
        snapshot.records, capture_id=capture_id, connection=connection
    )
    with connection.cursor() as cursor:
        # Membership pairs are staged, then joined to the reference once.
        cursor.execute(
            "CREATE TEMP TABLE IF NOT EXISTS area_membership "
            "(parent_geo_id TEXT, member_geo_id TEXT) ON COMMIT DROP"
        )
        cursor.execute("TRUNCATE area_membership")
        execute_values(
            cursor,
            "INSERT INTO area_membership (parent_geo_id, member_geo_id) VALUES %s",
            snapshot.memberships,
        )
        cursor.execute(
            """
            INSERT INTO silver_ref.bridge_geo_relationship_version (
                parent_geo_sk, related_geo_sk, relationship_type,
                geography_vintage, evidence_source, source_snapshot_id
            )
            SELECT parent.geo_sk, member.geo_sk, 'contains', %s, %s, %s
            FROM area_membership AS pair
            JOIN silver_ref.dim_geo_entity AS parent ON parent.geo_id = pair.parent_geo_id
            JOIN silver_ref.dim_geo_entity AS member ON member.geo_id = pair.member_geo_id
            ON CONFLICT DO NOTHING
            """,
            (snapshot.vintage, snapshot.evidence_source, str(capture_id)),
        )
        related = cursor.rowcount
        cursor.execute(
            """
            SELECT DISTINCT pair.member_geo_id
            FROM area_membership AS pair
            LEFT JOIN silver_ref.dim_geo_entity AS member
                   ON member.geo_id = pair.member_geo_id
            WHERE member.geo_sk IS NULL
            ORDER BY 1
            """
        )
        missing = [row[0] for row in cursor.fetchall()]
        for geo_id in missing:
            member_type, code = snapshot.member_codes.get(geo_id, ("unknown", geo_id))
            cursor.execute(
                """
                INSERT INTO silver_ref.geography_resolution (
                    provider_source, provider_dataset, source_geo_type,
                    source_code, source_vintage, status, reason_code,
                    evidence_capture_id
                ) VALUES (%s, %s, %s, %s, %s, 'unmapped', 'member_not_in_reference', %s)
                ON CONFLICT (
                    provider_source, provider_dataset, source_geo_type,
                    source_code, source_vintage
                ) DO UPDATE SET
                    status = EXCLUDED.status,
                    reason_code = EXCLUDED.reason_code,
                    evidence_capture_id = EXCLUDED.evidence_capture_id,
                    resolved_at = NOW()
                """,
                (
                    SOURCE_CODE,
                    snapshot.evidence_source,
                    member_type,
                    code,
                    snapshot.vintage,
                    str(capture_id),
                ),
            )
    if missing:
        logger.warning(
            "%s: %d member(s) not in the shared reference, recorded unmapped",
            snapshot.evidence_source,
            len(missing),
        )
    return {"areas": loaded, "memberships": related, "unmapped_members": len(missing)}


def sync_area_geography(source_year: int | None = None) -> dict[str, int]:
    """Capture one vintage's region and CBSA files and publish both.

    Run after the county reference: a county membership is written only for
    a county the reference already holds.
    """
    release = AREA_RELEASES[source_year or LATEST_AREA_RELEASE]
    # Read through the module, so the one retrying download and the one hook
    # the reference pipeline uses are the ones used here.
    hook = geography_pipeline._get_hook()
    factory = hook.get_conn
    control = CaptureControl(factory, source_code=SOURCE_CODE)
    repository = GeographyRepository(factory)
    run_id = control.start_run(
        watermark={"geography_vintage": release.vintage, "snapshot_scope": "areas"}
    )
    try:
        captured: dict[str, UUID] = {}
        with httpx.Client(follow_redirects=True, timeout=300) as client:
            for product, url in (
                ("regions_and_divisions", release.states_url),
                ("cbsa_delineation", release.cbsa_url),
            ):
                parameters = {
                    "geography_vintage": release.vintage,
                    "delineation_vintage": release.delineation_vintage,
                    "product": product,
                }
                request = control.start_request(
                    run_id=run_id,
                    endpoint=url,
                    parameters=parameters,
                    max_attempts=HTTP_MAX_ATTEMPTS,
                )
                try:
                    response = geography_pipeline._download_with_retry(
                        client, url, control=control, request_id=request.request_id
                    )
                except BaseException as exc:
                    control.finish_request(request.request_id, status="failed", error=exc)
                    raise
                capture_id = uuid4()
                persist_response_capture(
                    factory,
                    ResponseCapture(
                        capture_id=capture_id,
                        request_id=request.request_id,
                        run_id=run_id,
                        source_code=SOURCE_CODE,
                        endpoint=url,
                        request_parameters=parameters,
                        retrieved_at=datetime.now(timezone.utc),
                        http_status=response.status_code,
                        response_headers=response.headers,
                        media_type=response.headers.get("content-type", "text/csv"),
                        payload=response.content,
                        payload_schema_version=PARSER_VERSION,
                        source_revision=str(release.vintage),
                    ),
                )
                control.finish_request(request.request_id, status="captured")
                captured[product] = capture_id

        snapshots: list[tuple[AreaSnapshot, UUID]] = []
        for product, capture_id in captured.items():
            payload = load_captured_payload(factory, capture_id)
            try:
                snapshot = (
                    parse_regions_and_divisions(payload, vintage=release.vintage)
                    if product == "regions_and_divisions"
                    else parse_cbsa_delineation(
                        payload, delineation_vintage=release.delineation_vintage
                    )
                )
            except BaseException as exc:
                control.quarantine(
                    capture_id=capture_id,
                    run_id=run_id,
                    parser_version=PARSER_VERSION,
                    error_code="area_geography_replay_failed",
                    error=exc,
                )
                raise
            snapshots.append((snapshot, capture_id))

        counts = {"areas": 0, "memberships": 0, "unmapped_members": 0}
        publication = factory()
        try:
            for snapshot, capture_id in snapshots:
                for key, value in publish_area_snapshot(
                    repository, snapshot, capture_id=capture_id, connection=publication
                ).items():
                    counts[key] += value
            publication.commit()
        except BaseException:
            publication.rollback()
            raise
        finally:
            publication.close()
        control.finish_run(run_id, status="success")
        return counts
    except BaseException as exc:
        control.finish_run(run_id, status="failed", error=exc)
        raise
