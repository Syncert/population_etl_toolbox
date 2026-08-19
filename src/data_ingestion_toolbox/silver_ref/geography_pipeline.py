"""Capture-first, replayable Census geography reference pipeline."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import time
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Iterable
from uuid import UUID, uuid4

import httpx
import shapefile

from data_ingestion_toolbox.capture import (
    CaptureControl,
    ResponseCapture,
    load_captured_payload,
    persist_response_capture,
)
from data_ingestion_toolbox.silver_ref.config import CONFIG
from data_ingestion_toolbox.silver_ref.geography_contract import canonical_geo_id

logger = logging.getLogger(__name__)

SOURCE_CODE = "CENSUS_GEO"
PARSER_VERSION = "census-geography-v1"
HTTP_MAX_ATTEMPTS = 3
GAZ_ROOT = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer"
BOUNDARY_ROOT = "https://www2.census.gov/geo/tiger/GENZ"


@dataclass(frozen=True)
class GeographyRecord:
    geo_type: str
    geo_id: str
    census_geoid: str
    state_fips: str | None
    county_fips: str | None
    place_fips: str | None
    name: str
    geography_vintage: int
    geoidfq: str | None = None
    usps: str | None = None
    lsad: str | None = None
    functional_status: str | None = None
    legal_statistical_class: str | None = None
    land_area_m2: int | None = None
    water_area_m2: int | None = None
    latitude: float | None = None
    longitude: float | None = None

    @property
    def attribute_checksum(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()


@dataclass(frozen=True)
class GeometryRecord:
    geo_id: str
    boundary_vintage: int
    geojson: str
    resolution: str = "500k"
    geography: GeographyRecord | None = None

    @property
    def geometry_checksum(self) -> str:
        return hashlib.sha256(self.geojson.encode()).hexdigest()


def _optional_int(value: object) -> int | None:
    text = str(value or "").strip()
    return int(text) if text and text.lstrip("-").isdigit() else None


def _optional_float(value: object) -> float | None:
    text = str(value or "").strip()
    try:
        return float(text) if text else None
    except ValueError:
        return None


def _area_m2(
    row: dict[str, Any], metric_name: str, square_miles_name: str
) -> int | None:
    metric = _optional_int(_first(row, metric_name))
    if metric is not None:
        return metric
    square_miles = _optional_float(_first(row, square_miles_name))
    return round(square_miles * 2_589_988.110336) if square_miles is not None else None


def _first(row: dict[str, Any], *names: str) -> str | None:
    normalized = {key.strip().upper(): value for key, value in row.items()}
    for name in names:
        value = normalized.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def parse_gazetteer_capture(
    payload: bytes, *, geo_type: str, geography_vintage: int
) -> list[GeographyRecord]:
    """Parse a captured Gazetteer ZIP without network or database access."""
    if not payload.startswith(b"PK"):
        raise ValueError("gazetteer capture is not a ZIP archive")
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = [name for name in archive.namelist() if name.lower().endswith(".txt")]
        if len(names) != 1:
            raise ValueError("gazetteer capture must contain exactly one text file")
        text = archive.read(names[0]).decode("utf-8-sig", errors="replace")
    header = text.splitlines()[0] if text else ""
    delimiter = "\t" if "\t" in header else "|"
    rows = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    records: list[GeographyRecord] = []
    for row in rows:
        geoid = _first(row, "GEOID")
        name = _first(row, "NAME", "NAME10")
        if not geoid or not name:
            raise ValueError("gazetteer row is missing GEOID or NAME")
        if geo_type == "state":
            state, county, place = geoid.zfill(2), None, None
        elif geo_type == "county":
            code = geoid.zfill(5)
            state, county, place = code[:2], code[2:], None
        elif geo_type == "place":
            code = geoid.zfill(7)
            state, county, place = code[:2], None, code[2:]
        else:
            raise ValueError(f"unsupported Gazetteer type: {geo_type}")
        records.append(
            GeographyRecord(
                geo_type=geo_type,
                geo_id=canonical_geo_id(
                    geo_type,
                    state_fips=state,
                    county_fips=county,
                    place_fips=place,
                ),
                census_geoid=geoid.zfill(
                    {"state": 2, "county": 5, "place": 7}[geo_type]
                ),
                state_fips=state,
                county_fips=county,
                place_fips=place,
                name=name,
                geography_vintage=geography_vintage,
                geoidfq=_first(row, "GEOIDFQ"),
                usps=_first(row, "USPS"),
                lsad=_first(row, "LSAD", "LSAD_CODE"),
                functional_status=_first(row, "FUNCSTAT", "FUNCTIONAL_STATUS"),
                legal_statistical_class=_first(row, "CLASSFP", "CLASS"),
                land_area_m2=_area_m2(row, "ALAND", "ALAND_SQMI"),
                water_area_m2=_area_m2(row, "AWATER", "AWATER_SQMI"),
                latitude=_optional_float(_first(row, "INTPTLAT")),
                longitude=_optional_float(_first(row, "INTPTLONG")),
            )
        )
    if len({record.geo_id for record in records}) != len(records):
        raise ValueError("gazetteer capture contains duplicate canonical identities")
    return records


def parse_boundary_capture(
    payload: bytes, *, geo_type: str, boundary_vintage: int
) -> list[GeometryRecord]:
    """Parse captured Census boundary components into canonical GeoJSON."""
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = archive.namelist()
        component = lambda suffix: next(  # noqa: E731
            (name for name in names if name.lower().endswith(suffix)), None
        )
        shp, shx, dbf = component(".shp"), component(".shx"), component(".dbf")
        if not all((shp, shx, dbf)):
            raise ValueError("boundary capture is missing shp/shx/dbf components")
        reader = shapefile.Reader(
            shp=io.BytesIO(archive.read(shp)),
            shx=io.BytesIO(archive.read(shx)),
            dbf=io.BytesIO(archive.read(dbf)),
        )
    fields = [field[0] for field in reader.fields[1:]]
    records: list[GeometryRecord] = []
    for shaped in reader.shapeRecords():
        properties = dict(zip(fields, shaped.record))
        state = str(properties.get("STATEFP") or "").zfill(2)
        if geo_type == "state":
            geo_id = canonical_geo_id("state", state_fips=state)
            census_geoid, county, place = state, None, None
        elif geo_type == "county":
            county = str(properties.get("COUNTYFP") or "").zfill(3)
            geo_id = canonical_geo_id("county", state_fips=state, county_fips=county)
            census_geoid, place = f"{state}{county}", None
        elif geo_type == "place":
            place = str(properties.get("PLACEFP") or "").zfill(5)
            geo_id = canonical_geo_id("place", state_fips=state, place_fips=place)
            census_geoid, county = f"{state}{place}", None
        else:
            raise ValueError(f"unsupported boundary type: {geo_type}")
        geojson = json.dumps(
            shaped.shape.__geo_interface__, sort_keys=True, separators=(",", ":")
        )
        name = _first(properties, "NAMELSAD", "NAME")
        geography = None
        if name:
            geography = GeographyRecord(
                geo_type=geo_type,
                geo_id=geo_id,
                census_geoid=census_geoid,
                state_fips=state,
                county_fips=county,
                place_fips=place,
                name=name,
                geography_vintage=boundary_vintage,
                geoidfq=_first(properties, "GEOIDFQ", "AFFGEOID"),
                usps=_first(properties, "STUSPS"),
                lsad=_first(properties, "LSAD"),
                functional_status=_first(properties, "FUNCSTAT"),
                legal_statistical_class=_first(properties, "CLASSFP"),
                land_area_m2=_area_m2(properties, "ALAND", "ALAND_SQMI"),
                water_area_m2=_area_m2(properties, "AWATER", "AWATER_SQMI"),
                latitude=_optional_float(_first(properties, "INTPTLAT")),
                longitude=_optional_float(_first(properties, "INTPTLON", "INTPTLONG")),
            )
        records.append(
            GeometryRecord(
                geo_id,
                boundary_vintage,
                geojson,
                geography=geography,
            )
        )
    return records


class GeographyRepository:
    """Transactional writer for replayed geography snapshots."""

    def __init__(self, connection_factory: Any) -> None:
        self.connection_factory = connection_factory

    def load_attributes(
        self,
        records: Iterable[GeographyRecord],
        *,
        capture_id: UUID,
        connection: Any | None = None,
    ) -> int:
        rows = list(records)
        owns_connection = connection is None
        connection = connection or self.connection_factory()
        try:
            with connection.cursor() as cursor:
                for row in rows:
                    cursor.execute(
                        """
                        INSERT INTO silver_ref.dim_geo_entity (
                            geo_id, geo_type, census_geoid, state_fips, county_fips,
                            place_fips, first_seen_version, last_seen_version
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (geo_id) DO UPDATE SET
                            last_seen_version = GREATEST(
                                silver_ref.dim_geo_entity.last_seen_version,
                                EXCLUDED.last_seen_version
                            ), updated_at = NOW()
                        RETURNING geo_sk
                        """,
                        (
                            row.geo_id,
                            row.geo_type,
                            row.census_geoid,
                            row.state_fips,
                            row.county_fips,
                            row.place_fips,
                            row.geography_vintage,
                            row.geography_vintage,
                        ),
                    )
                    geo_sk = cursor.fetchone()[0]
                    cursor.execute(
                        """
                        INSERT INTO silver_ref.dim_geo_entity_version (
                            geo_sk, geography_vintage, source_snapshot_id, geoidfq,
                            name, usps, lsad, functional_status, legal_statistical_class,
                            land_area_m2, water_area_m2, latitude, longitude,
                            attribute_checksum
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            geo_sk,
                            row.geography_vintage,
                            str(capture_id),
                            row.geoidfq,
                            row.name,
                            row.usps,
                            row.lsad,
                            row.functional_status,
                            row.legal_statistical_class,
                            row.land_area_m2,
                            row.water_area_m2,
                            row.latitude,
                            row.longitude,
                            row.attribute_checksum,
                        ),
                    )
            if owns_connection:
                connection.commit()
        except BaseException:
            if owns_connection:
                connection.rollback()
            raise
        finally:
            if owns_connection:
                connection.close()
        return len(rows)

    def load_geometries(
        self,
        records: Iterable[GeometryRecord],
        *,
        capture_id: UUID,
        connection: Any | None = None,
    ) -> int:
        rows = list(records)
        owns_connection = connection is None
        connection = connection or self.connection_factory()
        try:
            with connection.cursor() as cursor:
                for row in rows:
                    cursor.execute(
                        """
                        INSERT INTO silver_ref.dim_geo_geometry_version (
                            geo_sk, boundary_vintage, geometry_source, resolution,
                            source_snapshot_id, geom, geometry_checksum, is_valid
                        )
                        SELECT entity.geo_sk, %s, 'census_cartographic_boundary', %s,
                               %s,
                               ST_Multi(ST_CollectionExtract(ST_MakeValid(
                                   ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)
                               ), 3)), %s,
                               ST_IsValid(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
                        FROM silver_ref.dim_geo_entity AS entity WHERE entity.geo_id = %s
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            row.boundary_vintage,
                            row.resolution,
                            str(capture_id),
                            row.geojson,
                            row.geometry_checksum,
                            row.geojson,
                            row.geo_id,
                        ),
                    )
                    if cursor.rowcount == 0:
                        cursor.execute(
                            "SELECT 1 FROM silver_ref.dim_geo_entity WHERE geo_id = %s",
                            (row.geo_id,),
                        )
                        if cursor.fetchone() is None:
                            raise ValueError(
                                f"boundary has no matching entity: {row.geo_id}"
                            )
            if owns_connection:
                connection.commit()
        except BaseException:
            if owns_connection:
                connection.rollback()
            raise
        finally:
            if owns_connection:
                connection.close()
        return len(rows)

    def retire_missing(
        self,
        *,
        active_geo_ids: set[str],
        vintage: int,
        capture_id: UUID,
        connection: Any | None = None,
    ) -> int:
        """Retire only after the caller proves a complete successful snapshot."""
        owns_connection = connection is None
        connection = connection or self.connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    WITH missing AS (
                        SELECT entity.geo_sk, current.geoidfq, current.name, current.usps,
                               current.lsad, current.functional_status,
                               current.legal_statistical_class, current.land_area_m2,
                               current.water_area_m2, current.latitude, current.longitude
                        FROM silver_ref.dim_geo_entity entity
                        JOIN LATERAL (
                            SELECT version.* FROM silver_ref.dim_geo_entity_version version
                            WHERE version.geo_sk = entity.geo_sk
                            ORDER BY version.geography_vintage DESC, version.ingested_at DESC
                            LIMIT 1
                        ) current ON TRUE
                        WHERE entity.geo_type IN ('nation','state','county','place')
                          AND NOT (entity.geo_id = ANY(%s))
                          AND current.is_active
                    )
                    INSERT INTO silver_ref.dim_geo_entity_version (
                        geo_sk, geography_vintage, source_snapshot_id, geoidfq, name,
                        usps, lsad, functional_status, legal_statistical_class,
                        land_area_m2, water_area_m2, latitude, longitude, is_active,
                        attribute_checksum
                    )
                    SELECT geo_sk, %s, %s, geoidfq, name, usps, lsad,
                           functional_status, legal_statistical_class, land_area_m2,
                           water_area_m2, latitude, longitude, FALSE,
                           ENCODE(DIGEST(geo_sk::TEXT || ':' || %s::TEXT || ':retired', 'sha256'), 'hex')
                    FROM missing ON CONFLICT DO NOTHING
                    """,
                    (list(active_geo_ids), vintage, str(capture_id), vintage),
                )
                retired = cursor.rowcount
            if owns_connection:
                connection.commit()
            return retired
        except BaseException:
            if owns_connection:
                connection.rollback()
            raise
        finally:
            if owns_connection:
                connection.close()

    def reconcile_relationships(
        self,
        *,
        vintage: int,
        capture_id: UUID,
        connection: Any | None = None,
    ) -> None:
        owns_connection = connection is None
        connection = connection or self.connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO silver_ref.bridge_geo_relationship_version (
                        parent_geo_sk, related_geo_sk, relationship_type,
                        geography_vintage, evidence_source, source_snapshot_id
                    )
                    SELECT parent.geo_sk, child.geo_sk, 'contains', %s,
                           'exact_census_code_hierarchy', %s
                    FROM silver_ref.dim_geo_entity child
                    JOIN silver_ref.dim_geo_entity parent ON
                        (child.geo_type = 'state' AND parent.geo_type = 'nation') OR
                        (child.geo_type IN ('county','place') AND parent.geo_type = 'state'
                         AND parent.state_fips = child.state_fips)
                    ON CONFLICT DO NOTHING
                    """,
                    (vintage, str(capture_id)),
                )
                cursor.execute(
                    """
                    INSERT INTO silver_ref.bridge_geo_relationship_version (
                        parent_geo_sk, related_geo_sk, relationship_type,
                        geography_vintage, overlap_area_m2, overlap_weight,
                        evidence_source, source_snapshot_id
                    )
                    SELECT county.geo_sk, place.geo_sk, 'intersects', %s,
                           ST_Area(ST_Intersection(cg.geom, pg.geom)::geography),
                           LEAST(1, ST_Area(ST_Intersection(cg.geom, pg.geom)::geography)
                               / NULLIF(ST_Area(pg.geom::geography), 0)),
                           'census_boundary_intersection', %s
                    FROM silver_ref.dim_geo_entity county
                    JOIN silver_ref.dim_geo_geometry_version cg ON cg.geo_sk = county.geo_sk
                         AND cg.boundary_vintage = %s
                    JOIN silver_ref.dim_geo_entity place ON place.geo_type = 'place'
                         AND place.state_fips = county.state_fips
                    JOIN silver_ref.dim_geo_geometry_version pg ON pg.geo_sk = place.geo_sk
                         AND pg.boundary_vintage = %s
                    WHERE county.geo_type = 'county' AND ST_Intersects(cg.geom, pg.geom)
                    ON CONFLICT DO NOTHING
                    """,
                    (vintage, str(capture_id), vintage, vintage),
                )
            if owns_connection:
                connection.commit()
        except BaseException:
            if owns_connection:
                connection.rollback()
            raise
        finally:
            if owns_connection:
                connection.close()


def _get_hook():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def _urls(year: int) -> list[tuple[str, str, str]]:
    gaz = f"{GAZ_ROOT}/{year}_Gazetteer"
    genz = f"{BOUNDARY_ROOT}{year}/shp"
    return [
        ("attributes", "state", f"{gaz}/{year}_Gaz_state_national.zip"),
        ("attributes", "county", f"{gaz}/{year}_Gaz_counties_national.zip"),
        ("attributes", "place", f"{gaz}/{year}_Gaz_place_national.zip"),
        ("geometry", "state", f"{genz}/cb_{year}_us_state_500k.zip"),
        ("geometry", "county", f"{genz}/cb_{year}_us_county_500k.zip"),
        ("geometry", "place", f"{genz}/cb_{year}_us_place_500k.zip"),
    ]


def resolve_latest_complete_year(
    start_year: int | None = None, min_year: int = 2013
) -> int:
    """Select the newest vintage where every required attribute/boundary exists."""
    first = start_year or datetime.now(timezone.utc).year
    with httpx.Client(timeout=20, follow_redirects=True) as client:
        for year in range(first, min_year - 1, -1):
            available = True
            for _, _, url in _urls(year):
                response = client.head(url)
                if response.status_code in (403, 405):
                    response = client.get(url, headers={"Range": "bytes=0-3"})
                if response.status_code not in (200, 206):
                    available = False
                    break
            if available:
                return year
    raise RuntimeError(
        f"no complete Census geography snapshot found for {min_year}..{first}"
    )


def _retryable_http_error(error: BaseException) -> bool:
    if isinstance(error, httpx.RequestError):
        return True
    if isinstance(error, httpx.HTTPStatusError):
        status = error.response.status_code
        return status in (408, 429) or status >= 500
    return False


def _download_with_retry(
    client: httpx.Client,
    url: str,
    *,
    control: CaptureControl,
    request_id: UUID,
) -> httpx.Response:
    """Download with bounded, control-plane-visible transient retries."""
    for attempt in range(1, HTTP_MAX_ATTEMPTS + 1):
        try:
            response = client.get(url)
            response.raise_for_status()
            return response
        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            if attempt == HTTP_MAX_ATTEMPTS or not _retryable_http_error(exc):
                raise
            control.record_request_retry(request_id, error=exc)
            time.sleep(2 ** (attempt - 1))
    raise AssertionError("bounded HTTP retry loop exhausted without a result")


def sync_geography_reference(source_year: int | None = None) -> dict[str, int]:
    """Capture/parse a complete snapshot, then publish it in one transaction."""
    year = source_year or resolve_latest_complete_year()
    hook = _get_hook()
    factory = hook.get_conn
    control = CaptureControl(factory, source_code=SOURCE_CODE)
    repository = GeographyRepository(factory)
    run_id = control.start_run(watermark={"geography_vintage": year})
    captured: list[tuple[str, str, str, UUID]] = []
    try:
        with httpx.Client(follow_redirects=True, timeout=300) as client:
            for product, geo_type, url in _urls(year):
                parameters = {
                    "geography_vintage": year,
                    "geo_type": geo_type,
                    "product": product,
                }
                request = control.start_request(
                    run_id=run_id,
                    endpoint=url,
                    parameters=parameters,
                    max_attempts=HTTP_MAX_ATTEMPTS,
                )
                capture_id = uuid4()
                try:
                    response = _download_with_retry(
                        client,
                        url,
                        control=control,
                        request_id=request.request_id,
                    )
                except BaseException as exc:
                    control.finish_request(
                        request.request_id, status="failed", error=exc
                    )
                    raise
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
                        media_type=response.headers.get(
                            "content-type", "application/zip"
                        ),
                        payload=response.content,
                        payload_schema_version=PARSER_VERSION,
                        source_revision=str(year),
                    ),
                )
                control.finish_request(request.request_id, status="captured")
                captured.append((product, geo_type, url, capture_id))

        attribute_batches: list[tuple[list[GeographyRecord], UUID]] = []
        geometry_batches: list[tuple[list[GeometryRecord], UUID]] = []
        active_geo_ids: set[str] = {"us:1"}
        for product, geo_type, _, capture_id in captured:
            payload = load_captured_payload(factory, capture_id)
            try:
                if product == "attributes":
                    records = parse_gazetteer_capture(
                        payload, geo_type=geo_type, geography_vintage=year
                    )
                    active_geo_ids.update(record.geo_id for record in records)
                    if geo_type == "state":
                        records = [
                            GeographyRecord(
                                "nation",
                                "us:1",
                                "1",
                                None,
                                None,
                                None,
                                "United States",
                                year,
                            ),
                            *records,
                        ]
                    attribute_batches.append((records, capture_id))
                else:
                    boundary_records = parse_boundary_capture(
                        payload, geo_type=geo_type, boundary_vintage=year
                    )
                    boundary_only_entities = [
                        record.geography
                        for record in boundary_records
                        if record.geography is not None
                        and record.geo_id not in active_geo_ids
                    ]
                    if boundary_only_entities:
                        attribute_batches.append((boundary_only_entities, capture_id))
                        active_geo_ids.update(
                            record.geo_id for record in boundary_only_entities
                        )
                    geometry_batches.append((boundary_records, capture_id))
            except BaseException as exc:
                control.quarantine(
                    capture_id=capture_id,
                    run_id=run_id,
                    parser_version=PARSER_VERSION,
                    error_code="geography_replay_failed",
                    error=exc,
                )
                raise

        relationship_capture = captured[-1][3]
        counts = {"attributes": 0, "geometries": 0}
        publication = factory()
        try:
            for records, capture_id in attribute_batches:
                counts["attributes"] += repository.load_attributes(
                    records, capture_id=capture_id, connection=publication
                )
            for records, capture_id in geometry_batches:
                counts["geometries"] += repository.load_geometries(
                    records, capture_id=capture_id, connection=publication
                )
            counts["retired"] = repository.retire_missing(
                active_geo_ids=active_geo_ids,
                vintage=year,
                capture_id=relationship_capture,
                connection=publication,
            )
            repository.reconcile_relationships(
                vintage=year,
                capture_id=relationship_capture,
                connection=publication,
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
