"""Replay one captured government finance run into silver, reconcile it, publish it.

Everything here reads the committed captures, never the network. One
transaction per run: the measure dimension, the parsed records and
quarantine, the county-area roll-ups, the geography resolution ledger and
the conformed observations land together or not at all, and the run is only
marked ready when the counts reconcile.

The unit-file records go in by ``COPY`` (an edition is a few million
records). The roll-ups are one grouped statement each, computed here at
publish and never per request.
"""

from __future__ import annotations

import csv
import io
from collections.abc import Callable, Iterable, Sequence
from typing import Any
from uuid import UUID

from psycopg2.extras import Json, execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..capture import parser_contract_version
from ..config import SOURCE_CODE
from ..registry import MEASURES, get_edition, measures_for_item
from .parse import (
    ApiObservation,
    Quarantined,
    parse_archive,
    parse_govs,
    parse_qtax,
)

METHODOLOGY_URLS = {
    "cog_units": "https://www.census.gov/programs-surveys/cog.html",
    "asslgf": "https://www.census.gov/programs-surveys/gov-finances.html",
    "qtax": "https://www.census.gov/programs-surveys/qtax.html",
}

DATASET_UNITS = {
    "cog_units": "thousands of dollars",
    "asslgf": "thousands of dollars",
    "qtax": "millions of dollars",
}


class GovFinReconciliationError(RuntimeError):
    """A replayed run does not account for every captured record."""


class GovFinPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


def _copy(
    cursor: Any, table: str, columns: Sequence[str], rows: Iterable[Sequence[Any]]
) -> int:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    count = 0
    for row in rows:
        writer.writerow(["\\N" if value is None else value for value in row])
        count += 1
    buffer.seek(0)
    if count:
        cursor.copy_expert(
            f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')",
            buffer,
        )
    return count


def _slices(cursor: Any, run_id: UUID) -> list[tuple[str, int, str, str, str]]:
    cursor.execute(
        """
        SELECT dataset_id, period, slice_key, capture_id::TEXT, status
        FROM control.census_govfin_slice
        WHERE run_id = %s
        ORDER BY slice_key
        """,
        (str(run_id),),
    )
    return list(cursor.fetchall())


def _measure_dimension(cursor: Any, dataset_id: str, period: int) -> None:
    cadence = "quarterly" if dataset_id == "qtax" else "annual"
    unit = DATASET_UNITS[dataset_id]
    measures = MEASURES
    if dataset_id == "asslgf":
        measures = tuple(measure for measure in MEASURES if measure.govs_code)
    elif dataset_id == "qtax":
        measures = tuple(measure for measure in MEASURES if measure.qtax_code)
    if not measures:
        return
    execute_values(
        cursor,
        """
        INSERT INTO silver_census_govfin.dim_measure (
            dataset_id, measure_id, measure_label, definition, unit, cadence,
            methodology_url, parser_contract_version
        ) VALUES %s
        ON CONFLICT (dataset_id, measure_id) DO UPDATE SET
            measure_label = EXCLUDED.measure_label,
            definition = EXCLUDED.definition,
            unit = EXCLUDED.unit,
            cadence = EXCLUDED.cadence,
            methodology_url = EXCLUDED.methodology_url,
            parser_contract_version = EXCLUDED.parser_contract_version,
            updated_at = NOW()
        """,
        [
            (
                dataset_id,
                measure.measure_id,
                measure.label,
                measure.definition
                if dataset_id == "cog_units"
                else f"Census's published {measure.label.lower()}.",
                unit,
                cadence,
                METHODOLOGY_URLS[dataset_id],
                parser_contract_version(dataset_id, period),
            )
            for measure in measures
        ],
    )


def _quarantine(
    cursor: Any, run_id: UUID, capture_id: str, items: Sequence[Quarantined]
) -> None:
    if not items:
        return
    execute_values(
        cursor,
        """
        INSERT INTO silver_census_govfin.quarantine (
            run_id, capture_id, member, source_row_index, error_code, error_summary
        ) VALUES %s
        ON CONFLICT (capture_id, member, source_row_index, error_code) DO NOTHING
        """,
        [
            (
                str(run_id),
                capture_id,
                item.member,
                item.source_row_index,
                item.error_code,
                item.error_summary,
            )
            for item in items
        ],
    )


def _mark_slice(
    cursor: Any,
    run_id: UUID,
    slice_key: str,
    *,
    rows: int,
    quarantined: int,
    rejected: bool,
) -> None:
    cursor.execute(
        """
        UPDATE control.census_govfin_slice
           SET captured_row_count = %s,
               quarantined_row_count = %s,
               status = %s,
               updated_at = NOW()
         WHERE run_id = %s AND slice_key = %s AND status <> 'published'
        """,
        (
            rows,
            quarantined,
            "quarantined" if rejected else "silver_ready",
            str(run_id),
            slice_key,
        ),
    )


def _replay_units(
    connection_factory: Callable[[], Any],
    cursor: Any,
    run_id: UUID,
    period: int,
    slices: list[tuple[str, int, str, str, str]],
) -> int:
    """COPY one edition's records and directory; return the records kept."""
    edition = get_edition(period)
    archives = {archive.slice_key: archive for archive in edition.archives}
    kept = 0
    for _dataset, _period, slice_key, capture_id, status in slices:
        if status == "empty":
            continue
        payload = load_captured_payload(connection_factory, UUID(capture_id))
        parsed = parse_archive(edition, archives[slice_key], payload)
        if parsed.amounts:
            kept += _copy(
                cursor,
                "silver_census_govfin.unit_amount",
                (
                    "capture_id",
                    "source_row_index",
                    "run_id",
                    "census_year",
                    "government_code",
                    "type_code",
                    "fips_state",
                    "fips_county",
                    "item_code",
                    "amount_source",
                    "amount",
                    "value_status",
                    "data_year",
                    "imputation_flag",
                ),
                (
                    (
                        capture_id,
                        item.source_row_index,
                        str(run_id),
                        period,
                        item.government_code,
                        item.type_code,
                        item.fips_state,
                        item.fips_county,
                        item.item_code,
                        item.amount_source,
                        item.amount,
                        item.value_status,
                        item.data_year,
                        item.imputation_flag,
                    )
                    for item in parsed.amounts
                ),
            )
        if parsed.governments:
            _copy(
                cursor,
                "silver_census_govfin.unit_government",
                (
                    "capture_id",
                    "source_row_index",
                    "run_id",
                    "census_year",
                    "government_code",
                    "type_code",
                    "government_type",
                    "government_name",
                    "county_name",
                    "fips_state",
                    "fips_county",
                    "population",
                    "fiscal_year_ending",
                ),
                (
                    (
                        capture_id,
                        item.source_row_index,
                        str(run_id),
                        period,
                        item.government_code,
                        item.type_code,
                        item.government_type,
                        item.name,
                        item.county_name,
                        item.fips_state,
                        item.fips_county,
                        item.population,
                        item.fiscal_year_ending,
                    )
                    for item in parsed.governments
                ),
            )
        if parsed.state_totals:
            _copy(
                cursor,
                "silver_census_govfin.state_type_total",
                (
                    "capture_id",
                    "source_row_index",
                    "run_id",
                    "census_year",
                    "fips_state",
                    "level_code",
                    "item_code",
                    "amount",
                    "coefficient_of_variation",
                ),
                (
                    (
                        capture_id,
                        item.source_row_index,
                        str(run_id),
                        period,
                        item.fips_state,
                        item.level_code,
                        item.item_code,
                        item.amount,
                        item.coefficient_of_variation,
                    )
                    for item in parsed.state_totals
                ),
            )
        _quarantine(cursor, run_id, capture_id, parsed.quarantined)
        _mark_slice(
            cursor,
            run_id,
            slice_key,
            rows=parsed.row_count,
            quarantined=len(
                {
                    (item.member, item.source_row_index)
                    for item in parsed.quarantined
                    if item.source_row_index > 0
                }
            ),
            rejected=any(item.source_row_index == 0 for item in parsed.quarantined),
        )
    cursor.execute(
        """
        SELECT item_code FROM silver_census_govfin.unit_amount WHERE run_id = %s
        UNION
        SELECT item_code FROM silver_census_govfin.state_type_total WHERE run_id = %s
        """,
        (str(run_id), str(run_id)),
    )
    pairs = [
        (measure_id, item_code)
        for (item_code,) in cursor.fetchall()
        for measure_id in measures_for_item(item_code)
    ]
    if pairs:
        execute_values(
            cursor,
            """
            INSERT INTO silver_census_govfin.measure_item (measure_id, item_code)
            VALUES %s ON CONFLICT DO NOTHING
            """,
            pairs,
        )
    return kept


#: The three roll-ups of one edition. Census writes no record for an item a
#: government did not report, so a place whose governments filed records
#: and none under a measure's items has zero for it; an amount Census wrote
#: that is not a number makes the place's figure missing, never a smaller
#: sum. `%(run)s` is the run, `%(year)s` its census year.
_ROLLUP_SQL = """
WITH member AS (
    SELECT amount.government_code,
           amount.item_code,
           amount.amount,
           amount.value_status,
           amount.capture_id,
           -- The state and type from the government code itself, so a
           -- government no directory row places in a county still counts for
           -- its state; the county from the code where the edition's codes
           -- carry it, else from the directory.
           amount.type_code AS code_type,
           amount.fips_state,
           COALESCE(amount.fips_county, government.fips_county) AS fips_county
    FROM silver_census_govfin.unit_amount AS amount
    LEFT JOIN silver_census_govfin.unit_government AS government
      ON government.run_id = amount.run_id
     AND government.government_code = amount.government_code
    WHERE amount.run_id = %(run)s
), placed AS (
    SELECT 'state:' || fips_state || '|county:' || fips_county AS geo_id,
           'county'::TEXT AS geo_type, 'local'::TEXT AS government_level,
           'county_area_rollup'::TEXT AS derivation, member.*
    FROM member
    WHERE code_type <> '0' AND fips_county IS NOT NULL
    UNION ALL
    SELECT 'state:' || fips_state, 'state', 'local', 'state_local_rollup', member.*
    FROM member
    WHERE code_type <> '0'
    UNION ALL
    SELECT 'state:' || fips_state, 'state', 'state', 'state_government_units', member.*
    FROM member
    WHERE code_type = '0'
), area AS (
    SELECT geo_id, geo_type, government_level, derivation,
           COUNT(DISTINCT government_code) AS government_count,
           MIN(capture_id::TEXT)::UUID AS capture_id
    FROM placed
    GROUP BY geo_id, geo_type, government_level, derivation
), summed AS (
    SELECT placed.geo_id, placed.derivation, item.measure_id,
           SUM(placed.amount) AS amount,
           COUNT(*) FILTER (WHERE placed.value_status = 'missing') AS missing
    FROM placed
    JOIN silver_census_govfin.measure_item AS item ON item.item_code = placed.item_code
    GROUP BY placed.geo_id, placed.derivation, item.measure_id
)
INSERT INTO silver_census_govfin.fact_observation (
    dataset_id, measure_id, government_level, fiscal_year, quarter, geo_id,
    capture_id, run_id, retrieved_at, geo_sk, geo_type, geography_status,
    value_source, value, value_status, government_count, derivation,
    record_coverage, source_record_id
)
SELECT 'cog_units', measure.measure_id, area.government_level, %(year)s, 0, area.geo_id,
       area.capture_id, %(run)s, capture.retrieved_at, entity.geo_sk, area.geo_type,
       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
       NULL,
       CASE WHEN COALESCE(summed.missing, 0) > 0 THEN NULL
            ELSE COALESCE(summed.amount, 0) END,
       CASE WHEN COALESCE(summed.missing, 0) > 0 THEN 'missing' ELSE 'valid' END,
       area.government_count,
       area.derivation,
       %(coverage)s,
       ENCODE(SHA256(CONVERT_TO(
           'cog_units|' || measure.measure_id || '|' || area.derivation || '|'
           || %(year)s || '|' || area.geo_id || '|' || area.capture_id::TEXT, 'UTF8')), 'hex')
FROM area
CROSS JOIN silver_census_govfin.dim_measure AS measure
JOIN raw_capture.response_capture AS capture ON capture.capture_id = area.capture_id
LEFT JOIN summed
  ON summed.geo_id = area.geo_id
 AND summed.derivation = area.derivation
 AND summed.measure_id = measure.measure_id
LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = area.geo_id
WHERE measure.dataset_id = 'cog_units'
ON CONFLICT DO NOTHING
"""


def _rollup(cursor: Any, run_id: UUID, period: int) -> int:
    cursor.execute(
        _ROLLUP_SQL,
        {
            "run": str(run_id),
            "year": period,
            "coverage": get_edition(period).record_coverage,
        },
    )
    return int(cursor.rowcount)


def _replay_api(
    connection_factory: Callable[[], Any],
    cursor: Any,
    run_id: UUID,
    dataset_id: str,
    period: int,
    slices: list[tuple[str, int, str, str, str]],
) -> int:
    expected = 0
    for _dataset, _period, slice_key, capture_id, status in slices:
        if status == "empty":
            continue
        payload = load_captured_payload(connection_factory, UUID(capture_id))
        parsed = (
            parse_govs(payload, period=period, slice_key=slice_key)
            if dataset_id == "asslgf"
            else parse_qtax(payload, period=period, slice_key=slice_key)
        )
        if parsed.observations:
            execute_values(
                cursor,
                """
                INSERT INTO silver_census_govfin.api_revision (
                    capture_id, source_row_index, run_id, dataset_id, measure_id,
                    government_level, fiscal_year, quarter, geo_type, geo_source_code,
                    geo_id, value_source, value, value_status, source_record_id,
                    source_record
                ) VALUES %s
                ON CONFLICT (capture_id, source_row_index) DO NOTHING
                """,
                [
                    _api_row(capture_id, run_id, dataset_id, item)
                    for item in parsed.observations
                ],
            )
        _quarantine(cursor, run_id, capture_id, parsed.quarantined)
        expected += len(parsed.observations)
        _mark_slice(
            cursor,
            run_id,
            slice_key,
            rows=parsed.row_count,
            quarantined=len(
                {
                    (item.member, item.source_row_index)
                    for item in parsed.quarantined
                    if item.source_row_index > 0
                }
            ),
            rejected=any(item.source_row_index == 0 for item in parsed.quarantined),
        )
    cursor.execute(
        """
        INSERT INTO silver_census_govfin.fact_observation (
            dataset_id, measure_id, government_level, fiscal_year, quarter, geo_id,
            capture_id, run_id, retrieved_at, geo_sk, geo_type, geography_status,
            value_source, value, value_status, government_count, derivation,
            source_record_id
        )
        SELECT revision.dataset_id, revision.measure_id, revision.government_level,
               revision.fiscal_year, revision.quarter, revision.geo_id,
               revision.capture_id, revision.run_id, capture.retrieved_at,
               entity.geo_sk, revision.geo_type,
               CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
               revision.value_source, revision.value, revision.value_status,
               NULL, NULL, revision.source_record_id
        FROM silver_census_govfin.api_revision AS revision
        JOIN raw_capture.response_capture AS capture ON capture.capture_id = revision.capture_id
        LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
        WHERE revision.run_id = %s
        ON CONFLICT DO NOTHING
        """,
        (str(run_id),),
    )
    return expected


def _api_row(
    capture_id: str, run_id: UUID, dataset_id: str, item: ApiObservation
) -> tuple[Any, ...]:
    return (
        capture_id,
        item.source_row_index,
        str(run_id),
        dataset_id,
        item.measure_id,
        item.government_level,
        item.fiscal_year,
        item.quarter or 0,
        item.geo_type,
        item.geo_source_code,
        item.geo_id,
        item.value_source,
        item.value,
        item.value_status,
        item.source_record_id,
        Json(item.source_record),
    )


def _record_geography(cursor: Any, run_id: UUID) -> None:
    cursor.execute(
        """
        INSERT INTO silver_ref.geography_resolution (
            provider_source, provider_dataset, source_geo_type,
            source_code, source_label, source_vintage, geo_sk,
            resolution_method, evidence_capture_id, status, reason_code
        )
        SELECT DISTINCT ON (fact.dataset_id, fact.geo_type, fact.geo_id, fact.fiscal_year)
               %s, fact.dataset_id, fact.geo_type, fact.geo_id, NULL,
               fact.fiscal_year, fact.geo_sk,
               CASE WHEN fact.geo_sk IS NOT NULL THEN 'exact_code' END,
               fact.capture_id,
               CASE WHEN fact.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
               CASE WHEN fact.geo_sk IS NULL THEN 'canonical_geography_absent' END
        FROM silver_census_govfin.fact_observation AS fact
        WHERE fact.run_id = %s
        ORDER BY fact.dataset_id, fact.geo_type, fact.geo_id, fact.fiscal_year, fact.capture_id
        ON CONFLICT (provider_source, provider_dataset, source_geo_type, source_code, source_vintage)
        DO UPDATE SET
            geo_sk = EXCLUDED.geo_sk,
            resolution_method = EXCLUDED.resolution_method,
            evidence_capture_id = EXCLUDED.evidence_capture_id,
            status = EXCLUDED.status,
            reason_code = EXCLUDED.reason_code,
            resolved_at = NOW()
        """,
        (SOURCE_CODE, str(run_id)),
    )


def _count(cursor: Any, sql: str, run_id: UUID) -> int:
    cursor.execute(sql, (str(run_id),))
    return int(cursor.fetchone()[0])


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform every captured slice of a run; return the fact count."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            slices = _slices(cursor, run_id)
            if not slices:
                raise GovFinReconciliationError(
                    "run has no captured government finance slices"
                )
            dataset_id, period = slices[0][0], int(slices[0][1])
            _measure_dimension(cursor, dataset_id, period)
            if dataset_id == "cog_units":
                kept = _replay_units(connection_factory, cursor, run_id, period, slices)
                stored = _count(
                    cursor,
                    "SELECT COUNT(*) FROM silver_census_govfin.unit_amount WHERE run_id = %s",
                    run_id,
                )
                if stored != kept:
                    raise GovFinReconciliationError(
                        f"run {run_id} stored {stored} unit records of {kept} parsed"
                    )
                _rollup(cursor, run_id, period)
            else:
                expected = _replay_api(
                    connection_factory, cursor, run_id, dataset_id, period, slices
                )
                revisions = _count(
                    cursor,
                    "SELECT COUNT(*) FROM silver_census_govfin.api_revision WHERE run_id = %s",
                    run_id,
                )
                facts = _count(
                    cursor,
                    "SELECT COUNT(*) FROM silver_census_govfin.fact_observation WHERE run_id = %s",
                    run_id,
                )
                if revisions != expected or facts != revisions:
                    raise GovFinReconciliationError(
                        f"run {run_id} reconciles {revisions} revisions and {facts} facts "
                        f"against {expected} expected"
                    )
            _record_geography(cursor, run_id)
            facts = _count(
                cursor,
                "SELECT COUNT(*) FROM silver_census_govfin.fact_observation WHERE run_id = %s",
                run_id,
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return facts


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a reconciled run's slices through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.census_govfin_slice
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                """,
                (str(run_id),),
            )
            cursor.execute(
                """
                SELECT COUNT(*) FILTER (WHERE status = 'captured'),
                       COUNT(*) FILTER (WHERE status = 'published')
                FROM control.census_govfin_slice WHERE run_id = %s
                """,
                (str(run_id),),
            )
            unreplayed, published = cursor.fetchone()
            if unreplayed:
                raise GovFinPublicationError(
                    f"run {run_id} has slices that were never replayed"
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(
            connection_factory, publisher_schema="gold_census_govfin"
        )
    return int(published)
