"""Census tracts across decade boundaries (acs-place-and-tract-history, deliverable 7).

The ACS publishes on Census 2000 tracts in its 2009 release, 2010 Census
tracts for 2010-2019 and 2020 Census tracts from 2020. The same code in two
decades can be a different area, so a series that crosses a decade needs the
Bureau's tract relationship files:

* ``us2010trf.txt`` (2000 to 2010): comma-separated, no header, one row per
  part of a 2000 tract that lies in a 2010 tract. Thirty columns: the 2000
  tract (STATE00, COUNTY00, TRACT00, GEOID00, POP00, HU00, PART00, AREA00,
  AREALAND00), the 2010 tract (the same nine with ``10``), the part's area
  (AREAPT, AREALANDPT), its area and land shares of each tract in percent
  (AREAPCT00PT, AREALANDPCT00PT, AREAPCT10PT, AREALANDPCT10PT), its 2010
  population with its shares (POP10PT, POPPCT00, POPPCT10) and its 2010
  housing units with theirs (HU10PT, HUPCT00, HUPCT10).
* ``tab20_tract20_tract10_natl.txt`` (2010 to 2020): pipe-delimited with a
  header, one row per part, carrying each tract's land and water area and
  the part's (AREALAND_PART, AREAWATER_PART). It publishes no population.

Rows are matched to the shared reference by code *and decade*: the earlier
tract must have a version of the earlier census's vintage and the later tract
one of the later decade's, so a code that exists only in another decade is
never matched across decades. A tract the reference does not hold for that
decade is ledgered ``unmapped`` rather than dropped.
"""

from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
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
    TRACT_2000_RELATIONSHIP_URL,
)

logger = logging.getLogger(__name__)

PARSER_VERSION = "census-tract-decade-crosswalk-v1"
TRACT_2010_2020_RELATIONSHIP_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/tract/"
    "tab20_tract20_tract10_natl.txt"
)
#: Earlier census -> the relationship file to the next one.
RELATIONSHIP_FILES: dict[int, str] = {
    2000: TRACT_2000_RELATIONSHIP_URL,
    2010: TRACT_2010_2020_RELATIONSHIP_URL,
}
#: The ACS vintages a decade's tracts carry in the reference: the 2000 census
#: tracts as their own vintage, and every ACS year of the later decades.
DECADE_VINTAGES: dict[int, tuple[int, int]] = {
    2000: (2000, 2009),
    2010: (2010, 2019),
    2020: (2020, 2099),
}


@dataclass(frozen=True)
class CrosswalkPart:
    earlier_vintage: int
    earlier_geo_id: str
    later_geo_id: str
    land_area_part_m2: int
    total_area_part_m2: int | None
    earlier_land_share: Decimal | None
    later_land_share: Decimal | None
    later_population_part: int | None = None
    later_housing_units_part: int | None = None
    earlier_population_share: Decimal | None = None


def _tract_id(geoid: str, line: int) -> str:
    if len(geoid) != 11 or not geoid.isdigit():
        raise ValueError(f"relationship row {line}: tract GEOID {geoid!r}")
    return canonical_geo_id(
        "tract", state_fips=geoid[:2], county_fips=geoid[2:5], tract_code=geoid[5:]
    )


def _percent(text: str) -> Decimal | None:
    value = text.strip()
    return None if not value else min(Decimal(value) / 100, Decimal(1))


def _share(part: int, whole: int) -> Decimal | None:
    return None if whole <= 0 else min(Decimal(part) / Decimal(whole), Decimal(1))


def parse_2000_2010(payload: bytes) -> list[CrosswalkPart]:
    """Every part of every 2000 tract, with its 2010 population weight."""
    parts: dict[tuple[str, str], CrosswalkPart] = {}
    text = payload.decode("utf-8-sig")
    for line, row in enumerate(csv.reader(io.StringIO(text)), start=1):
        if not row:
            continue
        if len(row) != 30:
            raise ValueError(f"relationship row {line} has {len(row)} columns, not 30")
        part = CrosswalkPart(
            earlier_vintage=2000,
            earlier_geo_id=_tract_id(row[3].strip(), line),
            later_geo_id=_tract_id(row[12].strip(), line),
            land_area_part_m2=int(row[19]),
            total_area_part_m2=int(row[18]),
            earlier_land_share=_percent(row[21]),
            later_land_share=_percent(row[23]),
            later_population_part=int(row[24]),
            later_housing_units_part=int(row[27]),
            earlier_population_share=_percent(row[25]),
        )
        key = (part.earlier_geo_id, part.later_geo_id)
        if parts.setdefault(key, part) != part:
            raise ValueError(f"relationship row {line}: part {key} described twice")
    if not parts:
        raise ValueError("the 2000-to-2010 relationship file lists no part")
    return list(parts.values())


_COLUMNS_2010_2020 = {
    "GEOID_TRACT_20",
    "AREALAND_TRACT_20",
    "GEOID_TRACT_10",
    "AREALAND_TRACT_10",
    "AREALAND_PART",
    "AREAWATER_PART",
}


def parse_2010_2020(payload: bytes) -> list[CrosswalkPart]:
    """Every part of every 2010 tract; the file publishes land area only."""
    reader = csv.DictReader(io.StringIO(payload.decode("utf-8-sig")), delimiter="|")
    if not _COLUMNS_2010_2020 <= set(reader.fieldnames or ()):
        raise ValueError("the 2010-to-2020 relationship file lacks its columns")
    parts: dict[tuple[str, str], CrosswalkPart] = {}
    for line, row in enumerate(reader, start=2):
        land = int(row["AREALAND_PART"])
        part = CrosswalkPart(
            earlier_vintage=2010,
            earlier_geo_id=_tract_id(row["GEOID_TRACT_10"], line),
            later_geo_id=_tract_id(row["GEOID_TRACT_20"], line),
            land_area_part_m2=land,
            total_area_part_m2=land + int(row["AREAWATER_PART"]),
            earlier_land_share=_share(land, int(row["AREALAND_TRACT_10"])),
            later_land_share=_share(land, int(row["AREALAND_TRACT_20"])),
        )
        key = (part.earlier_geo_id, part.later_geo_id)
        if parts.setdefault(key, part) != part:
            raise ValueError(f"relationship row {line}: part {key} described twice")
    if not parts:
        raise ValueError("the 2010-to-2020 relationship file lists no part")
    return list(parts.values())


PARSERS = {2000: parse_2000_2010, 2010: parse_2010_2020}


def publish_crosswalk(
    connection: Any, parts: list[CrosswalkPart], *, capture_id: UUID
) -> dict[str, int]:
    """Write every part whose two tracts the reference holds for their decades.

    A tract the reference lacks for its decade is ledgered ``unmapped`` with
    the decade that named it.
    """
    if not parts:
        return {"parts": 0, "unmapped_tracts": 0}
    earlier = parts[0].earlier_vintage
    later = earlier + 10
    with connection.cursor() as cursor:
        cursor.execute(
            "CREATE TEMP TABLE IF NOT EXISTS crosswalk_part ("
            " earlier_geo_id TEXT, later_geo_id TEXT, land NUMERIC, total NUMERIC,"
            " earlier_share NUMERIC, later_share NUMERIC, population INTEGER,"
            " housing INTEGER, population_share NUMERIC) ON COMMIT DROP"
        )
        cursor.execute("TRUNCATE crosswalk_part")
        execute_values(
            cursor,
            "INSERT INTO crosswalk_part VALUES %s",
            [
                (
                    part.earlier_geo_id,
                    part.later_geo_id,
                    part.land_area_part_m2,
                    part.total_area_part_m2,
                    part.earlier_land_share,
                    part.later_land_share,
                    part.later_population_part,
                    part.later_housing_units_part,
                    part.earlier_population_share,
                )
                for part in parts
            ],
            page_size=10000,
        )
        # A tract counts for a decade only through a version of that decade.
        decade_sql = """
            SELECT entity.geo_id, entity.geo_sk
            FROM silver_ref.dim_geo_entity AS entity
            WHERE entity.geo_type = 'tract'
              AND EXISTS (
                  SELECT 1 FROM silver_ref.dim_geo_entity_version AS version
                  WHERE version.geo_sk = entity.geo_sk
                    AND version.geography_vintage BETWEEN %s AND %s
              )
        """
        cursor.execute(
            f"""
            WITH earlier AS ({decade_sql}), later AS ({decade_sql})
            INSERT INTO silver_ref.tract_decade_crosswalk (
                earlier_vintage, later_vintage, earlier_geo_sk, later_geo_sk,
                land_area_part_m2, total_area_part_m2, earlier_land_share,
                later_land_share, later_population_part, later_housing_units_part,
                earlier_population_share, source_snapshot_id
            )
            SELECT %s, %s, earlier.geo_sk, later.geo_sk, part.land, part.total,
                   part.earlier_share, part.later_share, part.population,
                   part.housing, part.population_share, %s
            FROM crosswalk_part AS part
            JOIN earlier ON earlier.geo_id = part.earlier_geo_id
            JOIN later ON later.geo_id = part.later_geo_id
            ON CONFLICT (earlier_vintage, earlier_geo_sk, later_geo_sk) DO UPDATE SET
                land_area_part_m2 = EXCLUDED.land_area_part_m2,
                total_area_part_m2 = EXCLUDED.total_area_part_m2,
                earlier_land_share = EXCLUDED.earlier_land_share,
                later_land_share = EXCLUDED.later_land_share,
                later_population_part = EXCLUDED.later_population_part,
                later_housing_units_part = EXCLUDED.later_housing_units_part,
                earlier_population_share = EXCLUDED.earlier_population_share,
                source_snapshot_id = EXCLUDED.source_snapshot_id,
                ingested_at = NOW()
            """,
            (
                *DECADE_VINTAGES[earlier],
                *DECADE_VINTAGES[later],
                earlier,
                later,
                str(capture_id),
            ),
        )
        written = cursor.rowcount
        cursor.execute(
            f"""
            WITH earlier AS ({decade_sql}), later AS ({decade_sql}),
            named AS (
                SELECT DISTINCT part.earlier_geo_id AS geo_id, %s AS vintage
                FROM crosswalk_part AS part
                WHERE NOT EXISTS (
                    SELECT 1 FROM earlier WHERE earlier.geo_id = part.earlier_geo_id
                )
                UNION
                SELECT DISTINCT part.later_geo_id, %s
                FROM crosswalk_part AS part
                WHERE NOT EXISTS (
                    SELECT 1 FROM later WHERE later.geo_id = part.later_geo_id
                )
            )
            INSERT INTO silver_ref.geography_resolution (
                provider_source, provider_dataset, source_geo_type, source_code,
                source_vintage, geo_sk, status, reason_code, evidence_capture_id
            )
            SELECT %s, 'census_tract_relationship', 'tract', named.geo_id,
                   named.vintage, NULL, 'unmapped', 'reference_lacks_decade', %s
            FROM named
            ON CONFLICT (provider_source, provider_dataset, source_geo_type,
                         source_code, source_vintage)
            DO UPDATE SET status = EXCLUDED.status,
                          reason_code = EXCLUDED.reason_code,
                          evidence_capture_id = EXCLUDED.evidence_capture_id,
                          resolved_at = NOW()
            """,
            (
                *DECADE_VINTAGES[earlier],
                *DECADE_VINTAGES[later],
                earlier,
                later,
                SOURCE_CODE,
                str(capture_id),
            ),
        )
        unmapped = cursor.rowcount
    if unmapped:
        logger.warning(
            "tract crosswalk %s-%s: %d tract(s) the reference lacks for their "
            "decade, recorded unmapped",
            earlier,
            later,
            unmapped,
        )
    return {"parts": written, "unmapped_tracts": unmapped}


def sync_tract_crosswalk() -> dict[str, dict[str, int]]:
    """Capture both relationship files, then publish each decade's parts.

    Run after the decade tract references: a part is written only when the
    reference holds both of its tracts for their decades.
    """
    hook = geography_pipeline._get_hook()
    factory = hook.get_conn
    control = CaptureControl(factory, source_code=SOURCE_CODE)
    run_id = control.start_run(watermark={"snapshot_scope": "tract_crosswalk"})
    try:
        captured: dict[int, UUID] = {}
        with httpx.Client(follow_redirects=True, timeout=300) as client:
            for earlier, url in RELATIONSHIP_FILES.items():
                parameters = {
                    "earlier_vintage": earlier,
                    "later_vintage": earlier + 10,
                    "product": "tract_relationship",
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
                    control.finish_request(
                        request.request_id, status="failed", error=exc
                    )
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
                        media_type=response.headers.get("content-type", "text/plain"),
                        payload=response.content,
                        payload_schema_version=PARSER_VERSION,
                        source_revision=f"{earlier}-{earlier + 10}",
                    ),
                )
                control.finish_request(request.request_id, status="captured")
                captured[earlier] = capture_id

        counts: dict[str, dict[str, int]] = {}
        for earlier, capture_id in captured.items():
            try:
                parts = PARSERS[earlier](load_captured_payload(factory, capture_id))
            except BaseException as exc:
                control.quarantine(
                    capture_id=capture_id,
                    run_id=run_id,
                    parser_version=PARSER_VERSION,
                    error_code="tract_crosswalk_replay_failed",
                    error=exc,
                )
                raise
            publication = factory()
            try:
                counts[f"{earlier}-{earlier + 10}"] = publish_crosswalk(
                    publication, parts, capture_id=capture_id
                )
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
