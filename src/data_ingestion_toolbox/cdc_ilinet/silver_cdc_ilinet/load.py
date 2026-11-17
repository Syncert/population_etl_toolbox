"""Replay one captured ILINet release into silver and publish it.

A replay reads the release's three downloads, places each reporting region
(the nation; a state by CDC's own state FIPS code from the init document; an
HHS region or a separately reported jurisdiction kept but not served), and
writes a figure only when CDC's text for it is new or differs from the
newest published one (CHURN RULE). The replay is one transaction and commits
only when every row of every download is stored or set aside.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload
from data_ingestion_toolbox.hhs_hospital_capacity.silver_hhs_hospital_capacity.parse import (
    USPS_TO_FIPS,
)

from ..client import ilinet_csv, read_init
from ..registry import MEASURES, PARSER_CONTRACT_VERSION, SEPARATE_JURISDICTIONS
from .parse import RegionWeek, parse_download

PUBLISHER_SCHEMA = "gold_cdc_ilinet"

_STATE_FIPS = frozenset(USPS_TO_FIPS.values())


class CdcIlinetReconciliationError(RuntimeError):
    """A replay that cannot account for every captured row."""


def region_placements(
    init_states: tuple[tuple[int, str, str], ...], regions: set[tuple[str, str]]
) -> list[tuple[str, str, str | None, str | None, str, str | None]]:
    """(region_type, region, geo_id, geo_type, status, note) for each region read.

    ``status`` is ``resolved`` here for a served region; the caller downgrades
    it to ``unmapped`` when the shared geography lacks the area.
    """
    fips_of = {name: fips for _sid, name, fips in init_states}
    beside = {state: city for city, state in SEPARATE_JURISDICTIONS.items()}
    placed = []
    for region_type, region in sorted(regions):
        if region_type == "national":
            placed.append((region_type, region, "us:1", "nation", "resolved", None))
            continue
        if region_type == "hhs_region":
            placed.append(
                (
                    region_type,
                    region,
                    None,
                    None,
                    "unsupported",
                    "An HHS region is not a geography this site serves.",
                )
            )
            continue
        if region in SEPARATE_JURISDICTIONS:
            placed.append(
                (
                    region_type,
                    region,
                    None,
                    None,
                    "unsupported",
                    f"CDC reports {region} as its own ILINet jurisdiction, beside "
                    f"{SEPARATE_JURISDICTIONS[region]}.",
                )
            )
            continue
        fips = fips_of.get(region, "")
        if fips not in _STATE_FIPS:
            placed.append(
                (
                    region_type,
                    region,
                    None,
                    None,
                    "unsupported",
                    "FluView lists no state FIPS code for this jurisdiction.",
                )
            )
            continue
        note = (
            f"CDC reports {beside[region]} as its own ILINet jurisdiction, beside this state."
            if region in beside
            else None
        )
        placed.append((region_type, region, f"state:{fips}", "state", "resolved", note))
    return placed


def _upsert_measures(cursor: Any) -> None:
    execute_values(
        cursor,
        """
        INSERT INTO silver_cdc_ilinet.dim_measure (
            measure_id, source_column, measure_label, unit, basis, parser_contract_version
        ) VALUES %s
        ON CONFLICT (measure_id) DO UPDATE SET
            source_column = EXCLUDED.source_column,
            measure_label = EXCLUDED.measure_label,
            unit = EXCLUDED.unit,
            basis = EXCLUDED.basis,
            parser_contract_version = EXCLUDED.parser_contract_version,
            updated_at = NOW()
        WHERE (silver_cdc_ilinet.dim_measure.source_column,
               silver_cdc_ilinet.dim_measure.measure_label,
               silver_cdc_ilinet.dim_measure.unit,
               silver_cdc_ilinet.dim_measure.basis,
               silver_cdc_ilinet.dim_measure.parser_contract_version)
              IS DISTINCT FROM
              (EXCLUDED.source_column, EXCLUDED.measure_label, EXCLUDED.unit,
               EXCLUDED.basis, EXCLUDED.parser_contract_version)
        """,
        [
            (m.measure_id, m.column, m.label, m.unit, m.basis, PARSER_CONTRACT_VERSION)
            for m in MEASURES
        ],
    )


def _upsert_regions(cursor: Any, placed: list[tuple]) -> None:
    if not placed:
        return
    geo_ids = sorted({row[2] for row in placed if row[2]})
    cursor.execute(
        "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
        (geo_ids,),
    )
    known = {row[0] for row in cursor.fetchall()}
    rows = [
        (
            region_type,
            region,
            geo_id,
            geo_type,
            "unmapped" if status == "resolved" and geo_id not in known else status,
            note,
        )
        for region_type, region, geo_id, geo_type, status, note in placed
    ]
    execute_values(
        cursor,
        """
        INSERT INTO silver_cdc_ilinet.region (
            region_type, region, geo_id, geo_type, geography_status, jurisdiction_note
        ) VALUES %s
        ON CONFLICT (region_type, region) DO UPDATE SET
            geo_id = EXCLUDED.geo_id,
            geo_type = EXCLUDED.geo_type,
            geography_status = EXCLUDED.geography_status,
            jurisdiction_note = EXCLUDED.jurisdiction_note,
            updated_at = NOW()
        WHERE (silver_cdc_ilinet.region.geo_id, silver_cdc_ilinet.region.geo_type,
               silver_cdc_ilinet.region.geography_status,
               silver_cdc_ilinet.region.jurisdiction_note)
              IS DISTINCT FROM
              (EXCLUDED.geo_id, EXCLUDED.geo_type, EXCLUDED.geography_status,
               EXCLUDED.jurisdiction_note)
        """,
        rows,
    )


_STAGE_SQL = """
    CREATE TEMP TABLE ilinet_stage (
        region_type TEXT, region TEXT, mmwr_year SMALLINT, mmwr_week SMALLINT,
        measure_id TEXT, week_start DATE, capture_id UUID, source_row_index INTEGER,
        value_source TEXT, value NUMERIC, value_status TEXT
    ) ON COMMIT DROP
"""

#: A figure is written only when it is new or CDC's text for it changed since
#: the newest published revision.
_CHANGED_SQL = """
    INSERT INTO silver_cdc_ilinet.week_value (
        region_type, region, mmwr_year, mmwr_week, measure_id, release_id,
        week_start, capture_id, source_row_index, value_source, value, value_status
    )
    SELECT stage.region_type, stage.region, stage.mmwr_year, stage.mmwr_week,
           stage.measure_id, %(release_id)s, stage.week_start, stage.capture_id,
           stage.source_row_index, stage.value_source, stage.value, stage.value_status
    FROM ilinet_stage AS stage
    LEFT JOIN LATERAL (
        SELECT TRUE AS found, value.value_source
        FROM silver_cdc_ilinet.week_value AS value
        JOIN control.cdc_ilinet_release AS release ON release.release_id = value.release_id
        WHERE release.status = 'published'
          AND value.region_type = stage.region_type
          AND value.region = stage.region
          AND value.mmwr_year = stage.mmwr_year
          AND value.mmwr_week = stage.mmwr_week
          AND value.measure_id = stage.measure_id
        ORDER BY release.published_at DESC
        LIMIT 1
    ) AS newest ON TRUE
    WHERE newest.found IS NULL OR newest.value_source IS DISTINCT FROM stage.value_source
"""


def _stage(cursor: Any, capture_id: str, rows: tuple[RegionWeek, ...]) -> None:
    values = [
        (
            row.region_type,
            row.region,
            row.mmwr_year,
            row.mmwr_week,
            measure_id,
            row.week_start,
            capture_id,
            row.source_row_index,
            figure.source,
            figure.value,
            figure.status,
        )
        for row in rows
        for measure_id, figure in row.figures.items()
    ]
    if values:
        execute_values(
            cursor,
            """
            INSERT INTO ilinet_stage (
                region_type, region, mmwr_year, mmwr_week, measure_id, week_start,
                capture_id, source_row_index, value_source, value, value_status
            ) VALUES %s
            """,
            values,
            page_size=10_000,
        )


def replay_release(factory: Callable[[], Any], *, release_id: UUID) -> int:
    """Parse and conform one captured release; return the figures written."""
    connection = factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT status, run_id::TEXT, init_capture_id::TEXT
                FROM control.cdc_ilinet_release WHERE release_id = %s
                """,
                (str(release_id),),
            )
            found = cursor.fetchone()
            if found is None:
                raise CdcIlinetReconciliationError(f"no release {release_id}")
            status, run_id, init_capture_id = found
            if status != "captured":
                return 0
            _upsert_measures(cursor)
            for relation in ("week_value", "quarantine"):
                cursor.execute(
                    f"DELETE FROM silver_cdc_ilinet.{relation} WHERE release_id = %s",
                    (str(release_id),),
                )
            init = read_init(load_captured_payload(factory, UUID(init_capture_id)))
            cursor.execute(_STAGE_SQL)
            cursor.execute(
                """
                SELECT region_type, capture_id::TEXT FROM control.cdc_ilinet_download
                WHERE release_id = %s ORDER BY region_type
                """,
                (str(release_id),),
            )
            downloads = list(cursor.fetchall())
            regions: set[tuple[str, str]] = set()
            stored = set_aside = total = 0
            for region_type, capture_id in downloads:
                payload = load_captured_payload(factory, UUID(capture_id))
                parsed = parse_download(ilinet_csv(payload), region_type=region_type)
                cursor.execute(
                    """
                    UPDATE control.cdc_ilinet_download SET text_encoding = %s
                    WHERE release_id = %s AND region_type = %s
                      AND text_encoding IS DISTINCT FROM %s
                    """,
                    (
                        parsed.text_encoding,
                        str(release_id),
                        region_type,
                        parsed.text_encoding,
                    ),
                )
                if parsed.quarantined:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO silver_cdc_ilinet.quarantine (
                            release_id, run_id, capture_id, source_row_index,
                            error_code, error_summary
                        ) VALUES %s
                        ON CONFLICT (capture_id, source_row_index, error_code) DO NOTHING
                        """,
                        [
                            (
                                str(release_id),
                                run_id,
                                capture_id,
                                item.source_row_index,
                                item.error_code,
                                item.error_summary,
                            )
                            for item in parsed.quarantined
                        ],
                    )
                if any(item.source_row_index == 0 for item in parsed.quarantined):
                    cursor.execute(
                        """
                        UPDATE control.cdc_ilinet_release
                           SET status = 'quarantined', updated_at = NOW()
                         WHERE release_id = %s
                        """,
                        (str(release_id),),
                    )
                    connection.commit()
                    return 0
                regions.update((row.region_type, row.region) for row in parsed.rows)
                _stage(cursor, capture_id, parsed.rows)
                stored += len(parsed.rows)
                set_aside += len(parsed.quarantined)
                total += parsed.row_count
            if stored + set_aside != total or len(downloads) != 3:
                raise CdcIlinetReconciliationError(
                    f"release {release_id} stored {stored} and set aside {set_aside} "
                    f"of {total} rows from {len(downloads)} downloads"
                )
            _upsert_regions(cursor, region_placements(init.states, regions))
            cursor.execute("ANALYZE ilinet_stage")
            cursor.execute(_CHANGED_SQL, {"release_id": str(release_id)})
            changed = cursor.rowcount
            cursor.execute(
                """
                UPDATE control.cdc_ilinet_release
                   SET row_count = %s, kept_row_count = %s, changed_value_count = %s,
                       status = 'silver_ready', updated_at = NOW()
                 WHERE release_id = %s
                """,
                (total, stored, changed, str(release_id)),
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return changed


def publish_release(factory: Callable[[], Any], *, release_id: UUID) -> int:
    """Expose a reconciled release through the gold views; return 1 if published."""
    connection = factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.cdc_ilinet_release
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE release_id = %s AND status = 'silver_ready'
                """,
                (str(release_id),),
            )
            published = cursor.rowcount
            if published:
                cursor.execute("ANALYZE silver_cdc_ilinet.week_value")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(factory, publisher_schema=PUBLISHER_SCHEMA)
    return published
