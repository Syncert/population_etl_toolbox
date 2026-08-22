"""Small database helpers for capture-first integration fixtures."""

from __future__ import annotations

import hashlib
from uuid import UUID, uuid4

from psycopg2.extensions import cursor

from data_ingestion_toolbox.silver_ref.geography_contract import canonical_geo_id


def seed_capture(db_cursor: cursor, source_code: str, payload: bytes = b"{}") -> UUID:
    """Insert the minimum valid run/request/capture graph and return capture_id."""
    run_id, request_id, capture_id = uuid4(), uuid4(), uuid4()
    fingerprint = uuid4().hex * 2
    checksum = hashlib.sha256(payload).hexdigest()
    db_cursor.execute(
        "INSERT INTO control.ingestion_run (run_id, source_code, status) VALUES (%s, %s, 'success')",
        (run_id, source_code),
    )
    db_cursor.execute(
        """INSERT INTO control.ingestion_request
           (request_id, run_id, source_code, endpoint, request_parameters,
            request_fingerprint, status) VALUES
           (%s, %s, %s, 'test://capture', '{}'::jsonb, %s, 'captured')""",
        (request_id, run_id, source_code, fingerprint),
    )
    db_cursor.execute(
        """INSERT INTO raw_capture.payload_blob
           (payload_checksum, payload, payload_size) VALUES (%s, %s, %s)
           ON CONFLICT (payload_checksum) DO NOTHING""",
        (checksum, payload, len(payload)),
    )
    db_cursor.execute(
        """INSERT INTO raw_capture.response_capture
           (capture_id, request_id, run_id, source_code, endpoint,
            request_parameters, request_fingerprint, retrieved_at, http_status,
            response_headers, media_type, payload_checksum) VALUES
           (%s, %s, %s, %s, 'test://capture', '{}'::jsonb, %s, NOW(), 200,
            '{}'::jsonb, 'application/json', %s)""",
        (capture_id, request_id, run_id, source_code, fingerprint, checksum),
    )
    return capture_id


def seed_geography(
    db_cursor: cursor,
    *,
    geo_type: str,
    vintage: int,
    name: str,
    state_fips: str | None = None,
    county_fips: str | None = None,
    place_fips: str | None = None,
    geo_sk: int | None = None,
) -> int:
    """Seed a target-model identity/version pair and return its surrogate key."""
    capture_id = seed_capture(db_cursor, "CENSUS_GEO")
    geo_id = canonical_geo_id(
        geo_type,
        state_fips=state_fips,
        county_fips=county_fips,
        place_fips=place_fips,
    )
    census_geoid = (
        "1"
        if geo_type == "nation"
        else state_fips
        if geo_type == "state"
        else f"{state_fips}{county_fips}"
        if geo_type == "county"
        else f"{state_fips}{place_fips}"
    )
    columns = "geo_id, geo_type, census_geoid, state_fips, county_fips, place_fips, first_seen_version, last_seen_version"
    values = (
        geo_id,
        geo_type,
        census_geoid,
        state_fips,
        county_fips,
        place_fips,
        vintage,
        vintage,
    )
    if geo_sk is not None:
        columns = "geo_sk, " + columns
        values = (geo_sk, *values)
    placeholders = ",".join(["%s"] * len(values))
    db_cursor.execute(
        f"""INSERT INTO silver_ref.dim_geo_entity ({columns})
            VALUES ({placeholders})
            ON CONFLICT (geo_id) DO UPDATE SET updated_at = NOW()
            RETURNING geo_sk""",
        values,
    )
    resolved_geo_sk = db_cursor.fetchone()[0]
    checksum = hashlib.sha256(f"{geo_id}:{vintage}:{name}".encode()).hexdigest()
    db_cursor.execute(
        """
        INSERT INTO silver_ref.dim_geo_entity_version (
            geo_sk, geography_vintage, source_snapshot_id, name, attribute_checksum
        ) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
        """,
        (resolved_geo_sk, vintage, capture_id, name, checksum),
    )
    return resolved_geo_sk


def delete_geography(db_cursor: cursor, geo_id: str) -> None:
    """Delete a test-owned geography after dependent facts have been removed."""
    db_cursor.execute(
        "DELETE FROM silver_ref.geography_resolution WHERE geo_sk IN "
        "(SELECT geo_sk FROM silver_ref.dim_geo_entity WHERE geo_id = %s)",
        (geo_id,),
    )
    db_cursor.execute(
        "DELETE FROM silver_ref.dim_geo_geometry_version WHERE geo_sk IN "
        "(SELECT geo_sk FROM silver_ref.dim_geo_entity WHERE geo_id = %s)",
        (geo_id,),
    )
    db_cursor.execute(
        "DELETE FROM silver_ref.dim_geo_entity_version WHERE geo_sk IN "
        "(SELECT geo_sk FROM silver_ref.dim_geo_entity WHERE geo_id = %s)",
        (geo_id,),
    )
    db_cursor.execute(
        "DELETE FROM silver_ref.dim_geo_entity WHERE geo_id = %s", (geo_id,)
    )
