"""Replay a captured read of an EIA energy route into silver (home-heating-fuel-prices).

The energy routes' counterpart of ``load.replay_run``: every page is parsed
from its stored bytes, each price is kept as a revision row and conformed
into ``silver_eia.fact_energy_price`` under its capture. A period EIA
revises is a second row beside the first; a price that repeats the newest
stored revision of its series and period is not stored again (CHURN RULE),
so a rerun over unchanged bytes writes no silver row. A state resolves
through the USPS code the shared reference's Census Gazetteer carries; the
nation and EIA's PADDs by their canonical ids. Every area is ledgered in
``silver_ref.geography_resolution`` and only a changed ledger row is
rewritten.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..config import SOURCE_CODE
from ..routes import EiaRoute
from .energy_parse import parse_energy_page

_COLUMNS = (
    "capture_id, row_index, run_id, route, series_id, period_start, period_end, "
    "frequency, area_code, area_name, metric_key, product, process, units, "
    "geo_type, geo_id, state_usps, value_source, value, value_status, "
    "outside_survey_season, source_record_id"
)

_INCOMING_TABLE_SQL = """
    DROP TABLE IF EXISTS pg_temp.eia_incoming_energy;
    CREATE TEMP TABLE eia_incoming_energy
        (LIKE silver_eia.energy_price_revision INCLUDING DEFAULTS) ON COMMIT DROP
"""

_STORE_NEW_REVISIONS_SQL = f"""
    INSERT INTO silver_eia.energy_price_revision ({_COLUMNS})
    SELECT {_COLUMNS}
    FROM eia_incoming_energy AS incoming
    WHERE NOT EXISTS (
        SELECT 1
        FROM (
            SELECT stored.value, stored.value_status
            FROM silver_eia.energy_price_revision AS stored
            WHERE stored.series_id = incoming.series_id
              AND stored.period_start = incoming.period_start
              AND stored.capture_id <> incoming.capture_id
            ORDER BY stored.created_at DESC, stored.capture_id DESC
            LIMIT 1
        ) AS newest
        WHERE newest.value IS NOT DISTINCT FROM incoming.value
          AND newest.value_status = incoming.value_status
    )
    ON CONFLICT (capture_id, row_index) DO NOTHING
"""


class EiaEnergyReconciliationError(RuntimeError):
    """A replayed energy read does not account for every captured row."""


def replay_energy_run(
    connection_factory: Callable[[], Any], *, run_id: UUID, route: EiaRoute
) -> int:
    """Parse and conform one captured energy read; return the fact count."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT page.capture_id::TEXT
                FROM control.eia_page AS page
                WHERE page.run_id = %s
                ORDER BY page.page_index
                """,
                (str(run_id),),
            )
            captures = [row[0] for row in cursor.fetchall()]
            if not captures:
                raise EiaEnergyReconciliationError(
                    f"run {run_id} has no captured EIA page"
                )
            parsed_rows = rejected = 0
            payload_rejected = False
            cursor.execute(_INCOMING_TABLE_SQL)
            for capture_id in captures:
                page = parse_energy_page(
                    route, load_captured_payload(connection_factory, UUID(capture_id))
                )
                parsed_rows += page.row_count
                rejected += sum(1 for q in page.quarantined if q.row_index >= 0)
                payload_rejected = payload_rejected or any(
                    q.row_index < 0 for q in page.quarantined
                )
                if page.prices:
                    execute_values(
                        cursor,
                        f"INSERT INTO eia_incoming_energy ({_COLUMNS}) VALUES %s",
                        [
                            (
                                capture_id,
                                price.row_index,
                                str(run_id),
                                price.route,
                                price.series_id,
                                price.period_start,
                                price.period_end,
                                price.frequency,
                                price.area_code,
                                price.area_name,
                                price.metric_key,
                                price.product,
                                price.process,
                                price.units,
                                price.geo_type,
                                price.geo_id,
                                price.state_usps,
                                price.value_source,
                                price.value,
                                price.value_status,
                                price.outside_survey_season,
                                price.source_record_id,
                            )
                            for price in page.prices
                        ],
                        page_size=5000,
                    )
                if page.quarantined:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO silver_eia.observation_quarantine (
                            run_id, capture_id, row_index, error_code, error_summary
                        ) VALUES %s
                        ON CONFLICT (capture_id, row_index, error_code) DO NOTHING
                        """,
                        [
                            (
                                str(run_id),
                                capture_id,
                                q.row_index,
                                q.error_code,
                                q.error_summary,
                            )
                            for q in page.quarantined
                        ],
                    )
            cursor.execute(_STORE_NEW_REVISIONS_SQL)
            cursor.execute(
                """
                SELECT COUNT(*) FROM eia_incoming_energy AS incoming
                WHERE NOT EXISTS (
                    SELECT 1 FROM silver_eia.energy_price_revision AS stored
                    WHERE stored.capture_id = incoming.capture_id
                      AND stored.row_index = incoming.row_index
                )
                """
            )
            repeated = int(cursor.fetchone()[0])
            cursor.execute(
                """
                WITH state_by_usps AS (
                    SELECT DISTINCT ON (version.usps) version.usps, entity.geo_id
                    FROM silver_ref.dim_geo_entity_version AS version
                    JOIN silver_ref.dim_geo_entity AS entity USING (geo_sk)
                    WHERE entity.geo_type = 'state' AND version.usps IS NOT NULL
                    ORDER BY version.usps, version.geography_vintage DESC, version.ingested_at DESC
                )
                UPDATE silver_eia.energy_price_revision AS revision
                   SET geo_id = state_by_usps.geo_id
                  FROM state_by_usps
                 WHERE revision.run_id = %s
                   AND revision.geo_type = 'state'
                   AND revision.geo_id IS NULL
                   AND state_by_usps.usps = revision.state_usps
                """,
                (str(run_id),),
            )
            cursor.execute(
                """
                INSERT INTO silver_ref.geography_resolution (
                    provider_source, provider_dataset, source_geo_type,
                    source_code, source_label, source_vintage, geo_sk,
                    resolution_method, evidence_capture_id, status, reason_code
                )
                SELECT DISTINCT ON (revision.geo_type, revision.area_code)
                       %s, %s, revision.geo_type, revision.area_code,
                       revision.area_name, EXTRACT(YEAR FROM revision.period_start)::INTEGER,
                       entity.geo_sk,
                       CASE WHEN entity.geo_sk IS NOT NULL THEN 'exact_code' END,
                       revision.capture_id,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       CASE WHEN entity.geo_sk IS NULL THEN 'canonical_geography_absent' END
                FROM silver_eia.energy_price_revision AS revision
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ORDER BY revision.geo_type, revision.area_code, revision.period_start DESC
                ON CONFLICT (provider_source, provider_dataset, source_geo_type, source_code, source_vintage)
                DO UPDATE SET
                    source_label = EXCLUDED.source_label,
                    geo_sk = EXCLUDED.geo_sk,
                    resolution_method = EXCLUDED.resolution_method,
                    evidence_capture_id = EXCLUDED.evidence_capture_id,
                    status = EXCLUDED.status,
                    reason_code = EXCLUDED.reason_code,
                    resolved_at = NOW()
                WHERE (silver_ref.geography_resolution.source_label, silver_ref.geography_resolution.geo_sk, silver_ref.geography_resolution.resolution_method, silver_ref.geography_resolution.status, silver_ref.geography_resolution.reason_code)
                      IS DISTINCT FROM
                      (EXCLUDED.source_label, EXCLUDED.geo_sk, EXCLUDED.resolution_method, EXCLUDED.status, EXCLUDED.reason_code)
                """,
                (SOURCE_CODE, route.dataset, str(run_id)),
            )
            cursor.execute(
                """
                INSERT INTO silver_eia.fact_energy_price (
                    series_id, period_start, capture_id, run_id, retrieved_at,
                    route, frequency, period_end, metric_key, area_code, area_name,
                    geo_type, geo_id, geo_sk, geography_status, units,
                    value_source, value, value_status, outside_survey_season,
                    source_record_id
                )
                SELECT revision.series_id, revision.period_start, revision.capture_id,
                       revision.run_id, capture.retrieved_at, revision.route,
                       revision.frequency, revision.period_end, revision.metric_key,
                       revision.area_code, revision.area_name, revision.geo_type,
                       revision.geo_id, entity.geo_sk,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       revision.units, revision.value_source, revision.value,
                       revision.value_status, revision.outside_survey_season,
                       revision.source_record_id
                FROM silver_eia.energy_price_revision AS revision
                JOIN raw_capture.response_capture AS capture ON capture.capture_id = revision.capture_id
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ON CONFLICT (series_id, period_start, capture_id) DO NOTHING
                """,
                (str(run_id),),
            )
            cursor.execute(
                "SELECT COUNT(*) FROM silver_eia.energy_price_revision WHERE run_id = %s",
                (str(run_id),),
            )
            revisions = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT COUNT(*) FROM silver_eia.fact_energy_price WHERE run_id = %s",
                (str(run_id),),
            )
            facts = int(cursor.fetchone()[0])
            if revisions + repeated + rejected != parsed_rows or facts != revisions:
                raise EiaEnergyReconciliationError(
                    f"run {run_id} reconciles {revisions} revisions, {repeated} "
                    f"repeats of a stored period, {rejected} set aside "
                    f"and {facts} facts against {parsed_rows} captured rows"
                )
            cursor.execute(
                """
                UPDATE control.eia_read
                   SET parsed_row_count = %s, repeated_row_count = %s,
                       status = %s, updated_at = NOW()
                 WHERE run_id = %s AND status <> 'published'
                """,
                (
                    parsed_rows,
                    repeated,
                    "quarantined" if payload_rejected else "silver_ready",
                    str(run_id),
                ),
            )
            # Planner statistics for the served reads (PREVENTION LIST).
            cursor.execute("ANALYZE silver_eia.fact_energy_price")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return facts
