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
    """Delete a test-owned geography after dependent facts have been removed.

    ``seed_geography`` commits a whole capture graph -- a CENSUS_GEO run,
    request, response capture, and payload blob -- as the version row's
    lineage evidence. Removing only the dimension rows left that graph behind
    on every seeded geography in every tier, which is why a suite could not be
    run twice against one persistent database: the leftover captures re-entered
    the next session's transforms.
    """
    db_cursor.execute(
        "SELECT source_snapshot_id FROM silver_ref.dim_geo_entity_version "
        "WHERE geo_sk IN (SELECT geo_sk FROM silver_ref.dim_geo_entity "
        "WHERE geo_id = %s) AND source_snapshot_id IS NOT NULL",
        (geo_id,),
    )
    seed_capture_ids = [row[0] for row in db_cursor.fetchall()]
    db_cursor.execute(
        "DELETE FROM silver_ref.bridge_geo_relationship_version WHERE "
        "parent_geo_sk IN (SELECT geo_sk FROM silver_ref.dim_geo_entity "
        "WHERE geo_id = %s) OR related_geo_sk IN "
        "(SELECT geo_sk FROM silver_ref.dim_geo_entity WHERE geo_id = %s)",
        (geo_id, geo_id),
    )
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
    delete_seed_captures(db_cursor, seed_capture_ids)


def delete_seed_captures(db_cursor: cursor, capture_ids: list[UUID]) -> None:
    """Remove seeded capture graphs once nothing references them.

    A capture another version row still cites is left in place; the delete is
    scoped to the ids passed in and guarded by that reference check, so one
    suite's teardown cannot remove another's lineage evidence.
    """
    if not capture_ids:
        return
    db_cursor.execute(
        "SELECT capture_id FROM raw_capture.response_capture "
        "WHERE capture_id = ANY(%s) AND NOT EXISTS ("
        "SELECT 1 FROM silver_ref.dim_geo_entity_version AS version "
        "WHERE version.source_snapshot_id = response_capture.capture_id)",
        (capture_ids,),
    )
    removable = [row[0] for row in db_cursor.fetchall()]
    if not removable:
        return
    db_cursor.execute(
        "SELECT DISTINCT payload_checksum, run_id FROM raw_capture.response_capture "
        "WHERE capture_id = ANY(%s)",
        (removable,),
    )
    rows = db_cursor.fetchall()
    checksums = [row[0] for row in rows]
    run_ids = [row[1] for row in rows]

    # Captures and payloads are append-only by trigger. The disposable test
    # database is the only place that guard is lifted, and only for rows a
    # fixture created.
    db_cursor.execute(
        "ALTER TABLE raw_capture.response_capture "
        "DISABLE TRIGGER response_capture_reject_mutation"
    )
    db_cursor.execute(
        "DELETE FROM raw_capture.response_capture WHERE capture_id = ANY(%s)",
        (removable,),
    )
    db_cursor.execute(
        "ALTER TABLE raw_capture.response_capture "
        "ENABLE TRIGGER response_capture_reject_mutation"
    )
    db_cursor.execute(
        "ALTER TABLE raw_capture.payload_blob "
        "DISABLE TRIGGER payload_blob_reject_mutation"
    )
    db_cursor.execute(
        "DELETE FROM raw_capture.payload_blob AS payload "
        "WHERE payload.payload_checksum = ANY(%s) AND NOT EXISTS ("
        "SELECT 1 FROM raw_capture.response_capture AS capture "
        "WHERE capture.payload_checksum = payload.payload_checksum)",
        (checksums,),
    )
    db_cursor.execute(
        "ALTER TABLE raw_capture.payload_blob "
        "ENABLE TRIGGER payload_blob_reject_mutation"
    )
    db_cursor.execute(
        "DELETE FROM control.ingestion_request AS request "
        "WHERE request.run_id = ANY(%s) AND NOT EXISTS ("
        "SELECT 1 FROM raw_capture.response_capture AS capture "
        "WHERE capture.request_id = request.request_id)",
        (run_ids,),
    )
    db_cursor.execute(
        "DELETE FROM control.ingestion_run AS run "
        "WHERE run.run_id = ANY(%s) AND NOT EXISTS ("
        "SELECT 1 FROM control.ingestion_request AS request "
        "WHERE request.run_id = run.run_id)",
        (run_ids,),
    )
