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
from psycopg2.extras import execute_values

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
#: Rows per multi-row statement. The loaders are round-trip bound rather
#: than throughput bound, so this only has to be large enough to amortise
#: the round trip; oversized pages just grow the statement psycopg2 builds.
WRITE_PAGE_SIZE = 1000
PARSER_VERSION = "census-geography-v2"
HTTP_MAX_ATTEMPTS = 3
MIN_SUPPORTED_GEOGRAPHY_YEAR = 2013
MIN_HISTORICAL_COUNTY_YEAR = 1990
GAZ_ROOT = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer"
BOUNDARY_ROOT = "https://www2.census.gov/geo/tiger/GENZ"
LEGACY_COUNTY_URLS = {
    1990: f"{GAZ_ROOT}/counties.zip",
    2000: f"{GAZ_ROOT}/county2k.zip",
}


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
    return int(text) if text and text.lstrip("+-").isdigit() else None


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


def _read_single_text_capture(payload: bytes, *, label: str) -> str:
    if not payload.startswith(b"PK"):
        raise ValueError(f"{label} capture is not a ZIP archive")
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = [name for name in archive.namelist() if name.lower().endswith(".txt")]
        if len(names) != 1:
            raise ValueError(f"{label} capture must contain exactly one text file")
        contents = archive.read(names[0])
    try:
        return contents.decode("utf-8-sig")
    except UnicodeDecodeError:
        return contents.decode("cp1252")


def parse_gazetteer_capture(
    payload: bytes, *, geo_type: str, geography_vintage: int
) -> list[GeographyRecord]:
    """Parse a captured Gazetteer ZIP without network or database access."""
    text = _read_single_text_capture(payload, label="gazetteer")
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


def parse_legacy_county_gazetteer_capture(
    payload: bytes, *, geography_vintage: int
) -> list[GeographyRecord]:
    """Parse the fixed-width national county Gazetteers from 1990 and 2000."""
    if geography_vintage not in LEGACY_COUNTY_URLS:
        raise ValueError(
            f"unsupported legacy county Gazetteer vintage: {geography_vintage}"
        )
    text = _read_single_text_capture(payload, label="legacy county Gazetteer")
    records: list[GeographyRecord] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        if geography_vintage == 1990:
            if len(line) < 141:
                raise ValueError(
                    f"1990 county row {line_number} is shorter than 141 columns"
                )
            state = line[0:2].strip()
            county = line[5:8].strip()
            name = line[9:75].strip()
            usps = line[76:78].strip() or None
            land_source = _optional_int(line[99:109])
            water_source = _optional_int(line[110:120])
            latitude_source = _optional_int(line[121:130])
            longitude_source = _optional_int(line[131:141])
            # The 1990 source stores area in thousandths of a square kilometer
            # and internal points in millionths of a degree.
            land_area_m2 = land_source * 1_000 if land_source is not None else None
            water_area_m2 = water_source * 1_000 if water_source is not None else None
            latitude = (
                latitude_source / 1_000_000 if latitude_source is not None else None
            )
            longitude = (
                longitude_source / 1_000_000 if longitude_source is not None else None
            )
        else:
            if len(line) < 162:
                raise ValueError(
                    f"2000 county row {line_number} is shorter than 162 columns"
                )
            usps = line[0:2].strip() or None
            state = line[2:4].strip()
            county = line[4:7].strip()
            name = line[7:71].strip()
            land_area_m2 = _optional_int(line[89:103])
            water_area_m2 = _optional_int(line[103:117])
            latitude = _optional_float(line[141:151])
            longitude = _optional_float(line[151:162])

        if (
            len(state) != 2
            or not state.isdigit()
            or len(county) != 3
            or not county.isdigit()
            or not name
        ):
            raise ValueError(
                f"legacy county row {line_number} has an invalid state/county code or name"
            )
        census_geoid = f"{state}{county}"
        records.append(
            GeographyRecord(
                geo_type="county",
                geo_id=canonical_geo_id("county", state_fips=state, county_fips=county),
                census_geoid=census_geoid,
                state_fips=state,
                county_fips=county,
                place_fips=None,
                name=name,
                geography_vintage=geography_vintage,
                usps=usps,
                land_area_m2=land_area_m2,
                water_area_m2=water_area_m2,
                latitude=latitude,
                longitude=longitude,
            )
        )
    if not records:
        raise ValueError("legacy county Gazetteer contains no records")
    if len({record.geo_id for record in records}) != len(records):
        raise ValueError(
            "legacy county Gazetteer contains duplicate canonical identities"
        )
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
                if rows:
                    self._upsert_entities(cursor, rows)
                    geo_sk_by_geo_id = self._geo_sk_map(
                        cursor, [row.geo_id for row in rows]
                    )
                    self._insert_entity_versions(
                        cursor, rows, geo_sk_by_geo_id, capture_id
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

    @staticmethod
    def _upsert_entities(cursor: Any, rows: list[GeographyRecord]) -> None:
        """Upsert one entity per distinct ``geo_id`` in a few multi-row statements.

        ``ON CONFLICT DO UPDATE`` refuses to touch the same row twice within one
        command, so repeated ``geo_id`` values are folded here first. Folding
        reproduces what the per-row loop did across those duplicates exactly:
        the first occurrence supplies the immutable identity columns, which only
        the insert path ever writes, and the vintage bounds collapse to the
        ``LEAST``/``GREATEST`` merge the conflict clause would have applied one
        row at a time.
        """
        merged: dict[str, list[Any]] = {}
        for row in rows:
            existing = merged.get(row.geo_id)
            if existing is None:
                merged[row.geo_id] = [
                    row.geo_id,
                    row.geo_type,
                    row.census_geoid,
                    row.state_fips,
                    row.county_fips,
                    row.place_fips,
                    row.geography_vintage,
                    row.geography_vintage,
                ]
                continue
            existing[6] = min(existing[6], row.geography_vintage)
            existing[7] = max(existing[7], row.geography_vintage)

        execute_values(
            cursor,
            """
            INSERT INTO silver_ref.dim_geo_entity (
                geo_id, geo_type, census_geoid, state_fips, county_fips,
                place_fips, first_seen_version, last_seen_version
            ) VALUES %s
            ON CONFLICT (geo_id) DO UPDATE SET
                first_seen_version = LEAST(
                    silver_ref.dim_geo_entity.first_seen_version,
                    EXCLUDED.first_seen_version
                ),
                last_seen_version = GREATEST(
                    silver_ref.dim_geo_entity.last_seen_version,
                    EXCLUDED.last_seen_version
                ), updated_at = NOW()
            """,
            list(merged.values()),
            template=(
                "(%s::TEXT,%s::TEXT,%s::TEXT,%s::TEXT,%s::TEXT,%s::TEXT,"
                "%s::INTEGER,%s::INTEGER)"
            ),
            page_size=WRITE_PAGE_SIZE,
        )

    @staticmethod
    def _geo_sk_map(cursor: Any, geo_ids: list[str]) -> dict[str, int]:
        """Re-read the surrogate keys the batched upsert assigned.

        The per-row loader took these from ``RETURNING``; one indexed read of the
        distinct identifiers replaces that without changing which key each
        ``geo_id`` resolves to.
        """
        cursor.execute(
            """
            SELECT geo_id, geo_sk FROM silver_ref.dim_geo_entity
            WHERE geo_id = ANY(%s)
            """,
            (sorted(set(geo_ids)),),
        )
        return {geo_id: geo_sk for geo_id, geo_sk in cursor.fetchall()}

    @staticmethod
    def _insert_entity_versions(
        cursor: Any,
        rows: list[GeographyRecord],
        geo_sk_by_geo_id: dict[str, int],
        capture_id: UUID,
    ) -> None:
        """Insert every supplied attribute version, duplicates included.

        Only the entity upsert folds duplicates; two records sharing a ``geo_id``
        still contribute their own version rows here, exactly as the per-row
        loader did. ``ON CONFLICT DO NOTHING`` tolerates duplicates inside a
        single command, so replays stay idempotent.
        """
        execute_values(
            cursor,
            """
            INSERT INTO silver_ref.dim_geo_entity_version (
                geo_sk, geography_vintage, source_snapshot_id, geoidfq,
                name, usps, lsad, functional_status, legal_statistical_class,
                land_area_m2, water_area_m2, latitude, longitude,
                attribute_checksum
            ) VALUES %s
            ON CONFLICT DO NOTHING
            """,
            [
                (
                    geo_sk_by_geo_id[row.geo_id],
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
                )
                for row in rows
            ],
            template=(
                "(%s::BIGINT,%s::INTEGER,%s::UUID,%s::TEXT,%s::TEXT,%s::TEXT,"
                "%s::TEXT,%s::TEXT,%s::TEXT,%s::NUMERIC,%s::NUMERIC,"
                "%s::DOUBLE PRECISION,%s::DOUBLE PRECISION,%s::TEXT)"
            ),
            page_size=WRITE_PAGE_SIZE,
        )

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
                if rows:
                    self._reject_boundaries_without_entity(cursor, rows)
                    self._insert_geometry_versions(cursor, rows, capture_id)
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

    @staticmethod
    def _reject_boundaries_without_entity(
        cursor: Any, rows: list[GeometryRecord]
    ) -> None:
        """Refuse a boundary whose entity was never loaded.

        The per-row loader detected this by probing after an insert wrote no row,
        which also fires when ``ON CONFLICT DO NOTHING`` skips an already-present
        geometry -- so the probe, not the row count, decided. Resolving every
        identifier in one indexed read preserves that: the guard still raises for
        a missing entity and stays silent for a duplicate geometry. The offending
        identifier is chosen in input order so the same batch reports the same
        ``geo_id`` the loop reached first.
        """
        cursor.execute(
            """
            SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)
            """,
            (sorted({row.geo_id for row in rows}),),
        )
        known = {geo_id for (geo_id,) in cursor.fetchall()}
        for row in rows:
            if row.geo_id not in known:
                raise ValueError(f"boundary has no matching entity: {row.geo_id}")

    @staticmethod
    def _insert_geometry_versions(
        cursor: Any, rows: list[GeometryRecord], capture_id: UUID
    ) -> None:
        """Write every boundary version in a few multi-row statements.

        The projection is the per-row statement unchanged: the same
        ``ST_MakeValid``/``ST_CollectionExtract`` repair, the same ``ST_IsValid``
        record of the raw input, the same capture lineage and checksum, and the
        same join to the entity that supplies ``geo_sk``.
        """
        execute_values(
            cursor,
            """
            INSERT INTO silver_ref.dim_geo_geometry_version (
                geo_sk, boundary_vintage, geometry_source, resolution,
                source_snapshot_id, geom, geometry_checksum, is_valid
            )
            SELECT entity.geo_sk, incoming.boundary_vintage,
                   'census_cartographic_boundary', incoming.resolution,
                   incoming.source_snapshot_id,
                   ST_Multi(ST_CollectionExtract(ST_MakeValid(
                       ST_SetSRID(ST_GeomFromGeoJSON(incoming.geojson), 4326)
                   ), 3)), incoming.geometry_checksum,
                   ST_IsValid(
                       ST_SetSRID(ST_GeomFromGeoJSON(incoming.geojson), 4326)
                   )
            FROM (VALUES %s) AS incoming (
                geo_id, boundary_vintage, resolution, source_snapshot_id,
                geojson, geometry_checksum
            )
            JOIN silver_ref.dim_geo_entity AS entity
                 ON entity.geo_id = incoming.geo_id
            ON CONFLICT DO NOTHING
            """,
            [
                (
                    row.geo_id,
                    row.boundary_vintage,
                    row.resolution,
                    str(capture_id),
                    row.geojson,
                    row.geometry_checksum,
                )
                for row in rows
            ],
            template=("(%s::TEXT,%s::INTEGER,%s::TEXT,%s::UUID,%s::TEXT,%s::TEXT)"),
            page_size=WRITE_PAGE_SIZE,
        )

    def retire_missing(
        self,
        *,
        active_geo_ids: set[str],
        geo_types: set[str] | None = None,
        vintage: int,
        capture_id: UUID,
        connection: Any | None = None,
    ) -> int:
        """Retire only after the caller proves a complete successful snapshot."""
        owns_connection = connection is None
        connection = connection or self.connection_factory()
        scoped_geo_types = sorted(geo_types or {"nation", "state", "county", "place"})
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
                        WHERE entity.geo_type = ANY(%s)
                          AND entity.first_seen_version <= %s
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
                    (
                        scoped_geo_types,
                        vintage,
                        list(active_geo_ids),
                        vintage,
                        str(capture_id),
                        vintage,
                    ),
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
        active_geo_ids: set[str],
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
                    WHERE child.geo_id = ANY(%s)
                    ON CONFLICT DO NOTHING
                    """,
                    (vintage, str(capture_id), list(active_geo_ids)),
                )
                # Candidate pairs are restricted to same-state county/place
                # boundaries *before* any spatial predicate runs. Written as one
                # flat join the planner inverts this: it drives the GiST index
                # with every county boundary against every geometry row in the
                # vintage, materialises the resulting cross product, and only
                # then applies the state_fips equality -- tens of millions of
                # ST_Intersects calls to keep a few million pairs. Materialising
                # the two boundary sets first makes the cheap relational
                # restriction the driver and the spatial predicate the filter.
                #
                # The intersection is also evaluated once per pair rather than
                # twice: the area and the weight both derive from the same
                # materialised overlap, and each place's own area is computed
                # once per place instead of once per candidate pair.
                cursor.execute(
                    """
                    WITH county_boundary AS MATERIALIZED (
                        SELECT entity.geo_sk, entity.state_fips, boundary.geom
                        FROM silver_ref.dim_geo_entity AS entity
                        JOIN silver_ref.dim_geo_geometry_version AS boundary
                             ON boundary.geo_sk = entity.geo_sk
                            AND boundary.boundary_vintage = %s
                        WHERE entity.geo_type = 'county'
                    ),
                    place_boundary AS MATERIALIZED (
                        SELECT entity.geo_sk, entity.state_fips, boundary.geom,
                               ST_Area(boundary.geom::geography) AS place_area_m2
                        FROM silver_ref.dim_geo_entity AS entity
                        JOIN silver_ref.dim_geo_geometry_version AS boundary
                             ON boundary.geo_sk = entity.geo_sk
                            AND boundary.boundary_vintage = %s
                        WHERE entity.geo_type = 'place'
                    ),
                    overlap AS MATERIALIZED (
                        SELECT county.geo_sk AS parent_geo_sk,
                               place.geo_sk AS related_geo_sk,
                               ST_Area(
                                   ST_Intersection(county.geom, place.geom)::geography
                               ) AS overlap_area_m2,
                               place.place_area_m2
                        FROM county_boundary AS county
                        JOIN place_boundary AS place
                             ON place.state_fips = county.state_fips
                        WHERE ST_Intersects(county.geom, place.geom)
                    )
                    INSERT INTO silver_ref.bridge_geo_relationship_version (
                        parent_geo_sk, related_geo_sk, relationship_type,
                        geography_vintage, overlap_area_m2, overlap_weight,
                        evidence_source, source_snapshot_id
                    )
                    SELECT parent_geo_sk, related_geo_sk, 'intersects', %s,
                           overlap_area_m2,
                           LEAST(1, overlap_area_m2
                               / NULLIF(place_area_m2, 0)),
                           'census_boundary_intersection', %s
                    FROM overlap
                    ON CONFLICT DO NOTHING
                    """,
                    (vintage, vintage, vintage, str(capture_id)),
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


def _historical_county_asset(year: int) -> tuple[str, str, str]:
    legacy_url = LEGACY_COUNTY_URLS.get(year)
    if legacy_url:
        return ("attributes", "county", legacy_url)
    if year < MIN_SUPPORTED_GEOGRAPHY_YEAR:
        raise ValueError(f"no supported county geography asset for vintage {year}")
    return _urls(year)[1]


def resolve_complete_years(
    start_year: int | None = None,
    min_year: int = MIN_SUPPORTED_GEOGRAPHY_YEAR,
) -> list[int]:
    """Return every supported vintage with all required attribute/boundary assets."""
    first = start_year or datetime.now(timezone.utc).year
    if first < min_year:
        raise ValueError("start_year must be greater than or equal to min_year")

    complete: list[int] = []
    with httpx.Client(timeout=20, follow_redirects=True) as client:
        for year in range(min_year, first + 1):
            available = True
            for _, _, url in _urls(year):
                response = client.head(url)
                if response.status_code in (403, 405):
                    response = client.get(url, headers={"Range": "bytes=0-3"})
                if response.status_code not in (200, 206):
                    available = False
                    break
            if available:
                complete.append(year)
    if complete:
        return complete
    raise RuntimeError(
        f"no complete Census geography snapshot found for {min_year}..{first}"
    )


def resolve_latest_complete_year(
    start_year: int | None = None,
    min_year: int = MIN_SUPPORTED_GEOGRAPHY_YEAR,
) -> int:
    """Select the newest vintage where every required attribute/boundary exists."""
    return resolve_complete_years(start_year=start_year, min_year=min_year)[-1]


def resolve_historical_county_years(
    start_year: int,
    min_year: int = MIN_HISTORICAL_COUNTY_YEAR,
) -> list[int]:
    """Return vintages with the national county Gazetteer needed by ACS."""
    if start_year < min_year:
        raise ValueError("start_year must be greater than or equal to min_year")
    years: list[int] = []
    candidates = [
        year for year in sorted(LEGACY_COUNTY_URLS) if min_year <= year <= start_year
    ]
    candidates.extend(
        range(max(min_year, MIN_SUPPORTED_GEOGRAPHY_YEAR), start_year + 1)
    )
    with httpx.Client(timeout=20, follow_redirects=True) as client:
        for year in candidates:
            url = _historical_county_asset(year)[2]
            response = client.head(url)
            if response.status_code in (403, 405):
                response = client.get(url, headers={"Range": "bytes=0-3"})
            if response.status_code in (200, 206):
                years.append(year)
    if years:
        return years
    raise RuntimeError(
        f"no Census county Gazetteer snapshots found for {min_year}..{start_year}"
    )


def successful_geography_vintages(
    connection_factory: Any,
    *,
    snapshot_scopes: tuple[str, ...] = ("full",),
) -> set[int]:
    """Return vintages that completed atomic publication successfully."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT (source_watermark->>'geography_vintage')::INTEGER
                FROM control.ingestion_run
                WHERE source_code = %s
                  AND status = 'success'
                  AND source_watermark ? 'geography_vintage'
                  AND source_watermark->>'geography_vintage' ~ '^[0-9]+$'
                  AND COALESCE(source_watermark->>'snapshot_scope', 'full') = ANY(%s)
                """,
                (SOURCE_CODE, list(snapshot_scopes)),
            )
            return {int(row[0]) for row in cursor.fetchall()}
    finally:
        connection.close()


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


def sync_geography_reference(
    source_year: int | None = None,
    *,
    assets: list[tuple[str, str, str]] | None = None,
    retire_geo_types: set[str] | None = None,
    snapshot_scope: str = "full",
) -> dict[str, int]:
    """Capture/parse a complete snapshot, then publish it in one transaction."""
    year = source_year or resolve_latest_complete_year()
    hook = _get_hook()
    factory = hook.get_conn
    control = CaptureControl(factory, source_code=SOURCE_CODE)
    repository = GeographyRepository(factory)
    run_id = control.start_run(
        watermark={"geography_vintage": year, "snapshot_scope": snapshot_scope}
    )
    captured: list[tuple[str, str, str, UUID]] = []
    try:
        with httpx.Client(follow_redirects=True, timeout=300) as client:
            for product, geo_type, url in assets or _urls(year):
                parameters = {
                    "geography_vintage": year,
                    "geo_type": geo_type,
                    "product": product,
                    "source_format": (
                        "legacy_fixed_width"
                        if geo_type == "county" and year in LEGACY_COUNTY_URLS
                        else "modern_gazetteer_or_shapefile"
                    ),
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
                    if geo_type == "county" and year in LEGACY_COUNTY_URLS:
                        records = parse_legacy_county_gazetteer_capture(
                            payload, geography_vintage=year
                        )
                    else:
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
                geo_types=retire_geo_types,
                vintage=year,
                capture_id=relationship_capture,
                connection=publication,
            )
            repository.reconcile_relationships(
                vintage=year,
                capture_id=relationship_capture,
                active_geo_ids=active_geo_ids,
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


def sync_geography_history(
    source_year: int | None = None,
    *,
    min_year: int = MIN_HISTORICAL_COUNTY_YEAR,
) -> dict[str, int]:
    """Backfill every complete vintage once and always refresh the newest vintage.

    Historical vintages are published oldest-to-newest. The newest vintage is always
    replayed last so entities absent from it receive an authoritative retirement
    version. Successfully published historical vintages are skipped on later monthly
    runs, keeping routine refreshes bounded to the newest snapshot.
    """
    latest_year = resolve_latest_complete_year(
        start_year=source_year,
        min_year=MIN_SUPPORTED_GEOGRAPHY_YEAR,
    )
    historical_years = resolve_historical_county_years(
        latest_year,
        min_year=min_year,
    )
    factory = _get_hook().get_conn
    completed = successful_geography_vintages(
        factory,
        snapshot_scopes=("historical_county", "full"),
    )
    historical_to_sync = [
        year
        for year in historical_years
        if year != latest_year and year not in completed
    ]

    totals = {
        "vintages_discovered": len(historical_years),
        "vintages_synced": 0,
        "vintages_skipped": len(historical_years) - len(historical_to_sync) - 1,
        "latest_vintage": latest_year,
        "attributes": 0,
        "geometries": 0,
        "retired": 0,
    }
    for year in historical_to_sync:
        logger.info(
            "[CENSUS_GEO] Publishing geography vintage %s (%s/%s)",
            year,
            totals["vintages_synced"] + 1,
            len(historical_to_sync) + 1,
        )
        counts = sync_geography_reference(
            source_year=year,
            assets=[_historical_county_asset(year)],
            retire_geo_types={"county"},
            snapshot_scope="historical_county",
        )
        totals["vintages_synced"] += 1
        for key in ("attributes", "geometries", "retired"):
            totals[key] += counts[key]

    logger.info(
        "[CENSUS_GEO] Publishing latest complete geography vintage %s last",
        latest_year,
    )
    counts = sync_geography_reference(
        source_year=latest_year,
        snapshot_scope="full",
    )
    totals["vintages_synced"] += 1
    for key in ("attributes", "geometries", "retired"):
        totals[key] += counts[key]
    return totals
