"""Load the Return A master files into the FBI silver tables, one year at a time.

Run inside the Airflow image on the internal stack::

    docker exec docker-airflow-scheduler-1 \\
        python -m data_ingestion_toolbox.fbi_ucr.return_a.load --years 2025-2015

What a run does:

1. Downloads each requested year's zip and commits it to ``raw_capture``
   before anything is parsed. The release key is the newest file's
   ``Last-Modified`` date, so the same files always load into the same
   release and a changed file starts a new one.
2. Loads each year in its own transaction: the year's county crosswalk,
   agencies, agency-county links, monthly participation (population) and
   offense rows for the two products below. A year already in
   ``control.fbi_return_a_year`` with the same file is skipped, so a run that
   stops part way resumes where it stopped and a rerun changes nothing.
   Reloading a year deletes exactly that year's rows first.
3. Publishes the release once every requested year is in, and refreshes the
   FBI calendar roll-ups. Until then the previous release keeps serving.

Only one load runs at a time (a Postgres advisory lock); a second waits.
Products: ``return_a_violent_crime`` (offense ``V``) and
``return_a_property_crime`` (``P``), each with reported offenses and
offenses cleared by arrest, measure ids as the CDE products spell them.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any
from uuid import UUID, uuid4

from data_ingestion_toolbox.capture import (
    CaptureControl,
    ResponseCapture,
    persist_response_capture,
)

from ..registry import STATE_CODE_CONTRACT
from .crosswalk import CountyRef, CrosswalkRow, build_crosswalk
from .fetch import FetchedFile, fetch_year, unzip_text
from .layout import ReturnARecord, iter_records
from .registry import RETURN_A_PRODUCTS

logger = logging.getLogger(__name__)

SOURCE_CODE = "FBI_UCR"
PARSER_VERSION = "fbi-return-a-v1"
UCR_PROGRAM = "SRS_RETURN_A"
LOCK_KEY = 0x46424952  # "FBIR": one Return A load at a time.
DOCUMENTATION_URL = "https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/downloads"
METHODOLOGY_URL = (
    "https://www.fbi.gov/how-we-can-help-you/more-fbi-services-and-information/ucr"
)
COUNTED_ENTITY_NOTE = (
    "Offense series count actual offenses (reported less unfounded); clearance "
    "series count offenses cleared by arrest. NIBRS agencies' submissions are "
    "converted to these summary counts by the FBI."
)


PRODUCTS = RETURN_A_PRODUCTS
#: (counted entity basis, which month card) for each product's two measures.
MEASURES: tuple[tuple[str, str], ...] = (
    ("offense", "actual"),
    ("clearance", "cleared"),
)


@dataclass
class YearResult:
    data_year: int
    status: str
    records: int = 0
    observations: int = 0
    participation: int = 0
    unresolved_county_codes: int = 0
    error: str | None = None


class YearsFailed(RuntimeError):
    """Some requested years could not be fetched or loaded; the rest were."""


def parse_years(text: str) -> list[int]:
    """``2025-2015`` or ``2023,2024`` -> years in the order given."""
    years: list[int] = []
    for part in text.split(","):
        part = part.strip()
        if "-" in part:
            first, last = (int(value) for value in part.split("-", 1))
            step = -1 if last < first else 1
            years.extend(range(first, last + step, step))
        elif part:
            years.append(int(part))
    return years


def _period(year: int, month: int) -> tuple[str, date, date]:
    start = date(year, month, 1)
    end = date(year + (month == 12), month % 12 + 1, 1)
    return f"{month:02d}-{year}", start, date.fromordinal(end.toordinal() - 1)


def _record_id(*parts: object) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()


def _agency_type(record: ReturnARecord) -> str:
    group = record.population_group
    if not group or group == "0":
        return "Other"
    if group[0] in "89":
        return "State Police" if group.endswith("E") else "County"
    return "City"


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
    cursor.copy_expert(
        f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')",
        buffer,
    )
    return count


def _county_reference(cursor: Any) -> list[CountyRef]:
    cursor.execute(
        """
        SELECT state_fips, county_fips, county_name, geo_id, geo_sk
        FROM silver_ref.dim_geo_current
        WHERE geo_type = 'county' AND is_active
          AND state_fips IS NOT NULL AND county_fips IS NOT NULL
        """
    )
    return [CountyRef(*row) for row in cursor.fetchall()]


def _capture_year(
    connection_factory: Callable[[], Any],
    control: CaptureControl,
    run_id: UUID,
    year: int,
    fetch: Callable[[int], FetchedFile],
) -> tuple[FetchedFile, UUID, str]:
    fetched_key = f"master_files/reta/reta-{year}.zip"
    request = control.start_request(
        run_id=run_id, endpoint=fetched_key, parameters={"key": fetched_key}
    )
    try:
        fetched = fetch(year)
        capture = ResponseCapture(
            capture_id=uuid4(),
            request_id=request.request_id,
            run_id=run_id,
            source_code=SOURCE_CODE,
            endpoint=fetched.object_key,
            request_parameters={"key": fetched.object_key},
            retrieved_at=datetime.now(UTC),
            http_status=fetched.http_status,
            response_headers=fetched.response_headers,
            media_type="application/zip",
            payload=fetched.payload,
            payload_schema_version=PARSER_VERSION,
            source_revision=(
                fetched.last_modified.isoformat() if fetched.last_modified else None
            ),
        )
        receipt = persist_response_capture(connection_factory, capture)
    except BaseException as error:
        control.finish_request(request.request_id, status="failed", error=error)
        raise
    control.finish_request(request.request_id, status="captured")
    return fetched, receipt.capture_id, receipt.payload_checksum


def _ensure_release(
    cursor: Any, release_key: str, capture_id: UUID, run_id: UUID
) -> None:
    for product in PRODUCTS:
        cursor.execute(
            """
            INSERT INTO silver_fbi.dim_ucr_dataset_release (
                product_id, release_key, refresh_date, max_data_month,
                ucr_program, offense_code, offense_label, period_start,
                period_end, documentation_url, methodology_url,
                parser_contract_version, reported_status, counted_entity_note,
                release_capture_id, source_run_id, source_record_count,
                quarantine_count, status
            ) VALUES (%s, %s, %s, '01/1960', %s, %s, %s, '01-1960', '01-1960',
                      %s, %s, %s, 'reported', %s, %s, %s, 0, 0, 'replaying')
            ON CONFLICT (product_id, release_key) DO NOTHING
            """,
            (
                product.product_id,
                release_key,
                release_key,
                UCR_PROGRAM,
                product.offense_code,
                product.offense_label,
                DOCUMENTATION_URL,
                METHODOLOGY_URL,
                PARSER_VERSION,
                COUNTED_ENTITY_NOTE,
                str(capture_id),
                str(run_id),
            ),
        )
        for basis, _ in MEASURES:
            cursor.execute(
                """
                INSERT INTO silver_fbi.dim_offense_measure (
                    product_id, measure_id, ucr_program, offense_code,
                    offense_label, measure_form, counted_entity_basis, unit,
                    reported_status
                ) VALUES (%s, %s, %s, %s, %s, 'absolute_total', %s, 'count',
                          'reported')
                ON CONFLICT (product_id, measure_id) DO NOTHING
                """,
                (
                    product.product_id,
                    f"{product.offense_code}:{basis}:absolute_total",
                    UCR_PROGRAM,
                    product.offense_code,
                    product.offense_label,
                    basis,
                ),
            )


def _delete_year(cursor: Any, release_key: str, year: int) -> None:
    products = [product.product_id for product in PRODUCTS]
    bounds = (date(year, 1, 1), date(year, 12, 31))
    for table in ("fact_crime_observation", "fact_reporting_participation"):
        cursor.execute(
            f"""
            DELETE FROM silver_fbi.{table}
            WHERE product_id = ANY(%s) AND release_key = %s
              AND period_start BETWEEN %s AND %s
            """,
            (products, release_key, *bounds),
        )
    cursor.execute(
        """
        DELETE FROM silver_fbi.agency_geography_relationship
        WHERE product_id = ANY(%s) AND release_key = %s AND effective_start = %s
        """,
        (products, release_key, bounds[0]),
    )
    cursor.execute(
        """
        DELETE FROM silver_fbi.return_a_county_crosswalk
        WHERE release_key = %s AND data_year = %s
        """,
        (release_key, year),
    )


def _load_year(
    cursor: Any,
    *,
    run_id: UUID,
    release_key: str,
    year: int,
    capture_id: UUID,
    records: list[ReturnARecord],
    counties: list[CountyRef],
) -> YearResult:
    _delete_year(cursor, release_key, year)
    crosswalk = build_crosswalk(records, counties)
    _copy(
        cursor,
        "silver_fbi.return_a_county_crosswalk",
        (
            "release_key",
            "data_year",
            "state_code",
            "fbi_county_code",
            "geo_id",
            "geo_sk",
            "method",
            "status",
            "reason_code",
            "evidence",
        ),
        (
            (
                release_key,
                year,
                row.state_code,
                row.fbi_county_code,
                row.geo_id,
                row.geo_sk,
                row.method,
                row.status,
                row.reason_code,
                row.evidence,
            )
            for row in crosswalk
        ),
    )
    by_code: dict[tuple[str, int], CrosswalkRow] = {
        (row.state_code, row.fbi_county_code): row for row in crosswalk
    }

    agencies = sorted(
        {record.ori: record for record in records}.values(),
        key=lambda record: record.ori,
    )
    cursor.execute(
        "CREATE TEMP TABLE return_a_agency (ori TEXT, state_code TEXT) ON COMMIT DROP"
    )
    _copy(
        cursor,
        "return_a_agency",
        ("ori", "state_code"),
        ((record.ori, record.state_code) for record in agencies),
    )
    cursor.execute(
        """
        INSERT INTO silver_fbi.dim_agency (ori, state_code, first_seen_release,
                                           last_seen_release)
        SELECT ori, state_code, %s, %s FROM return_a_agency ORDER BY ori
        ON CONFLICT (ori) DO UPDATE
           SET last_seen_release = GREATEST(silver_fbi.dim_agency.last_seen_release,
                                            EXCLUDED.last_seen_release),
               updated_at = NOW()
        """,
        (release_key, release_key),
    )
    # One version per agency and release (the table's key): the first year
    # loaded names it, as the directory's capture names a CDE release.
    cursor.execute(
        """
        CREATE TEMP TABLE return_a_agency_version
            (LIKE silver_fbi.dim_agency_version INCLUDING DEFAULTS) ON COMMIT DROP
        """
    )
    _copy(
        cursor,
        "return_a_agency_version",
        (
            "ori",
            "release_key",
            "agency_name",
            "agency_type",
            "state_name",
            "county_labels",
            "attribute_checksum",
            "evidence_capture_id",
        ),
        (
            (
                record.ori,
                release_key,
                record.agency_name,
                _agency_type(record),
                (STATE_CODE_CONTRACT.get(record.state_code) or (None,))[0],
                "{"
                + ",".join(f'"FBI county {code:03d}"' for code in record.county_codes)
                + "}",
                _record_id(record.ori, record.agency_name, record.county_codes),
                str(capture_id),
            )
            for record in agencies
        ),
    )
    cursor.execute(
        """
        INSERT INTO silver_fbi.dim_agency_version
        SELECT * FROM return_a_agency_version ORDER BY ori
        ON CONFLICT (ori, release_key) DO NOTHING
        """
    )

    start, end = date(year, 1, 1), date(year, 12, 31)
    geography: dict[str, tuple[str | None, str]] = {}
    relationships: list[tuple[Any, ...]] = []
    for record in agencies:
        state = STATE_CODE_CONTRACT.get(record.state_code)
        resolved = [
            by_code.get((record.state_code, code)) for code in record.county_codes
        ]
        # An agency in two counties with one unresolved must not read as a
        # single-county agency of the other: none of its counties resolve.
        every_county = all(
            row is not None and row.status == "resolved" for row in resolved
        )
        for product in PRODUCTS:
            if state is not None:
                relationships.append(
                    (
                        record.ori,
                        "state",
                        record.state_code,
                        f"state:{state[1]}",
                        None,
                        "exact_state_code",
                        "resolved",
                        "exact",
                        None,
                        start,
                        end,
                        year,
                        "fbi_return_a_state_code",
                        str(capture_id),
                        product.product_id,
                        release_key,
                    )
                )
            for code, row in zip(record.county_codes, resolved, strict=True):
                label = f"FBI county {code:03d}"
                if record.covered_by:
                    # Its offenses are in the covering agency's return; linking
                    # it as well would count it as a county agency that never
                    # reports.
                    status, method, confidence, reason = (
                        "unsupported",
                        None,
                        "unresolved",
                        f"covered_by:{record.covered_by}",
                    )
                elif every_county:
                    status, method, confidence, reason = (
                        "resolved",
                        "fbi_county_code",
                        "derived",
                        None,
                    )
                elif row is not None and row.status == "resolved":
                    status, method, confidence, reason = (
                        "unresolved",
                        None,
                        "unresolved",
                        "another_county_unresolved",
                    )
                else:
                    status, method, confidence, reason = (
                        "unresolved",
                        None,
                        "unresolved",
                        row.reason_code if row is not None else "county_code_absent",
                    )
                relationships.append(
                    (
                        record.ori,
                        "county",
                        label,
                        row.geo_id
                        if row is not None and row.status == "resolved"
                        else None,
                        row.geo_sk if status == "resolved" else None,
                        method,
                        status,
                        confidence,
                        reason,
                        start,
                        end,
                        year,
                        "fbi_return_a_county_code",
                        str(capture_id),
                        product.product_id,
                        release_key,
                    )
                )
        if record.covered_by:
            geography[record.ori] = (None, "agency_only")
        elif resolved and every_county:
            geography[record.ori] = (None, "agency_county_bridged")
        elif record.county_slices:
            geography[record.ori] = (None, "agency_county_unresolved")
        else:
            geography[record.ori] = (None, "agency_only")
    _copy(
        cursor,
        "silver_fbi.agency_geography_relationship",
        (
            "ori",
            "relationship_type",
            "source_label",
            "geo_id",
            "geo_sk",
            "resolution_method",
            "resolution_status",
            "confidence_class",
            "reason_code",
            "effective_start",
            "effective_end",
            "geography_vintage",
            "evidence_source",
            "evidence_capture_id",
            "product_id",
            "release_key",
        ),
        relationships,
    )

    participation_rows: list[tuple[Any, ...]] = []
    observation_rows: list[tuple[Any, ...]] = []
    for record in agencies:
        status = geography[record.ori][1]
        population = record.population
        for product in PRODUCTS:
            for month in record.months:
                period, period_start, period_end = _period(year, month.month)
                reported = month.is_reported and not record.covered_by
                participation_rows.append(
                    (
                        product.product_id,
                        release_key,
                        UCR_PROGRAM,
                        "agency",
                        record.ori,
                        record.agency_name,
                        f"fbi_agency:{record.ori}",
                        period,
                        period_start,
                        period_end,
                        f"agency:{record.ori}",
                        None,
                        status,
                        population,
                        population if reported else 0,
                        None,
                        "return_a_agency_population",
                        "full_participation" if reported else "no_participation",
                        str(run_id),
                        str(capture_id),
                        record.line_number,
                        PARSER_VERSION,
                    )
                )
                for basis, card in MEASURES:
                    measure_id = f"{product.offense_code}:{basis}:absolute_total"
                    has_value = reported and (
                        month.is_reported
                        if card == "actual"
                        else month.clearances_reported
                    )
                    value = (
                        product.total(
                            month.actual if card == "actual" else month.cleared
                        )
                        if has_value
                        else None
                    )
                    observation_rows.append(
                        (
                            product.product_id,
                            release_key,
                            _record_id(
                                product.product_id,
                                release_key,
                                record.ori,
                                measure_id,
                                period,
                            ),
                            measure_id,
                            "agency",
                            record.ori,
                            record.agency_name,
                            f"fbi_agency:{record.ori}",
                            period,
                            period_start,
                            period_end,
                            f"agency:{record.ori}",
                            None,
                            status,
                            "return_a_card_" + card if has_value else None,
                            value,
                            "reported" if has_value else "not_reported",
                            population,
                            str(run_id),
                            str(capture_id),
                            record.line_number,
                            PARSER_VERSION,
                        )
                    )
    participation = _copy(
        cursor,
        "silver_fbi.fact_reporting_participation",
        (
            "product_id",
            "release_key",
            "ucr_program",
            "subject_type",
            "subject_code",
            "subject_label",
            "source_geo_level",
            "period",
            "period_start",
            "period_end",
            "geo_id",
            "geo_sk",
            "geography_status",
            "population",
            "participated_population",
            "coverage_percent",
            "coverage_basis",
            "participation_status",
            "source_run_id",
            "capture_id",
            "source_row_index",
            "transformation_version",
        ),
        participation_rows,
    )
    observations = _copy(
        cursor,
        "silver_fbi.fact_crime_observation",
        (
            "product_id",
            "release_key",
            "source_record_id",
            "measure_id",
            "subject_type",
            "subject_code",
            "subject_label",
            "source_geo_level",
            "period",
            "period_start",
            "period_end",
            "geo_id",
            "geo_sk",
            "geography_status",
            "value_source",
            "value",
            "value_status",
            "population_denominator",
            "source_run_id",
            "capture_id",
            "source_row_index",
            "transformation_version",
        ),
        observation_rows,
    )
    unresolved = sum(1 for row in crosswalk if row.status != "resolved")
    return YearResult(
        year, "loaded", len(records), observations, participation, unresolved
    )


def _loaded(cursor: Any, release_key: str, year: int, checksum: str) -> bool:
    cursor.execute(
        """
        SELECT 1 FROM control.fbi_return_a_year
        WHERE release_key = %s AND data_year = %s AND payload_checksum = %s
        """,
        (release_key, year, checksum),
    )
    return cursor.fetchone() is not None


def publish(cursor: Any, release_key: str) -> None:
    """Mark the release reconciled and published, with its true extent."""
    for product in PRODUCTS:
        cursor.execute(
            """
            WITH extent AS (
                SELECT MIN(period_start) AS first_month,
                       MAX(period_start) FILTER (WHERE value_status = 'reported')
                           AS last_month,
                       COUNT(*) AS rows
                FROM silver_fbi.fact_crime_observation
                WHERE product_id = %(product)s AND release_key = %(release)s
            )
            UPDATE silver_fbi.dim_ucr_dataset_release AS release
               SET period_start = TO_CHAR(extent.first_month, 'MM-YYYY'),
                   period_end = TO_CHAR(extent.last_month, 'MM-YYYY'),
                   max_data_month = TO_CHAR(extent.last_month, 'MM/YYYY'),
                   source_record_count = extent.rows,
                   status = 'published', reconciled_at = NOW(),
                   published_at = NOW(), updated_at = NOW()
              FROM extent
             WHERE release.product_id = %(product)s
               AND release.release_key = %(release)s
               AND extent.last_month IS NOT NULL
            """,
            {"product": product.product_id, "release": release_key},
        )


def load(
    connection_factory: Callable[[], Any],
    years: Sequence[int],
    *,
    fetch: Callable[[int], FetchedFile] = fetch_year,
    publish_release: bool = True,
) -> dict[str, Any]:
    """Capture, load and publish the requested years; resumable and idempotent."""
    lock = connection_factory()
    lock.autocommit = True
    with lock.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
    control = CaptureControl(connection_factory, source_code=SOURCE_CODE)
    run_id = control.start_run(watermark={"return_a_years": list(years)})
    results: list[YearResult] = []
    failed: list[YearResult] = []
    release_key: str | None = None
    published = False
    try:
        captured = []
        for year in years:
            # A year whose signed URL or download fails is recorded (its
            # request is marked failed in the capture ledger) and the run
            # moves on to the next year; a rerun picks it up.
            try:
                captured.append(
                    _capture_year(connection_factory, control, run_id, year, fetch)
                )
            except Exception as error:
                logger.warning("Return A %s could not be fetched: %s", year, error)
                failed.append(YearResult(year, "fetch_failed", error=repr(error)))
        if not captured:
            raise YearsFailed(f"no Return A year could be fetched: {failed}")
        modified = [file.last_modified for file, _, _ in captured if file.last_modified]
        release_date = max(modified) if modified else datetime.now(UTC).date()
        release_key = release_date.isoformat()
        newest_capture = captured[0][1]

        database = connection_factory()
        try:
            with database.cursor() as cursor:
                _ensure_release(cursor, release_key, newest_capture, run_id)
                counties = _county_reference(cursor)
            database.commit()
            for fetched, capture_id, checksum in captured:
                with database.cursor() as cursor:
                    if _loaded(cursor, release_key, fetched.data_year, checksum):
                        results.append(YearResult(fetched.data_year, "already_loaded"))
                        continue
                try:
                    records = list(iter_records(unzip_text(fetched.payload)))
                    wrong = sorted({r.data_year for r in records} - {fetched.data_year})
                    if wrong:
                        raise ValueError(
                            f"reta-{fetched.data_year}.zip holds records for {wrong}"
                        )
                    with database.cursor() as cursor:
                        result = _load_year(
                            cursor,
                            run_id=run_id,
                            release_key=release_key,
                            year=fetched.data_year,
                            capture_id=capture_id,
                            records=records,
                            counties=counties,
                        )
                        cursor.execute(
                            """
                            INSERT INTO control.fbi_return_a_year (
                                release_key, data_year, capture_id,
                                payload_checksum, record_count,
                                observation_count, participation_count,
                                unresolved_county_codes
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (release_key, data_year) DO UPDATE SET
                                capture_id = EXCLUDED.capture_id,
                                payload_checksum = EXCLUDED.payload_checksum,
                                record_count = EXCLUDED.record_count,
                                observation_count = EXCLUDED.observation_count,
                                participation_count = EXCLUDED.participation_count,
                                unresolved_county_codes = EXCLUDED.unresolved_county_codes,
                                loaded_at = NOW()
                            """,
                            (
                                release_key,
                                fetched.data_year,
                                str(capture_id),
                                checksum,
                                result.records,
                                result.observations,
                                result.participation,
                                result.unresolved_county_codes,
                            ),
                        )
                    database.commit()
                except Exception as error:
                    # One bad file never costs the other years: its rows roll
                    # back whole and the run carries on.
                    database.rollback()
                    logger.warning(
                        "Return A %s could not be loaded: %s", fetched.data_year, error
                    )
                    failed.append(
                        YearResult(fetched.data_year, "load_failed", error=repr(error))
                    )
                    continue
                except BaseException:
                    database.rollback()
                    raise
                logger.info("Return A %s loaded: %s", fetched.data_year, result)
                results.append(result)
            # The release publishes only once every requested year is in.
            if publish_release and not failed:
                with database.cursor() as cursor:
                    publish(cursor, release_key)
                database.commit()
                published = True
        finally:
            database.close()
        if failed:
            control.finish_run(
                run_id,
                status="failed",
                error=YearsFailed(
                    "years not loaded: "
                    + ", ".join(f"{r.data_year} ({r.status})" for r in failed)
                ),
            )
        else:
            control.finish_run(run_id, status="success")
    except BaseException as error:
        control.finish_run(run_id, status="failed", error=error)
        raise
    finally:
        lock.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(connection_factory, publisher_schema="gold_fbi")
    return {
        "run_id": str(run_id),
        "release_key": release_key,
        "published": published,
        "years": [vars(result) for result in sorted(results + failed, key=_by_year)],
        "failed_years": [result.data_year for result in failed],
    }


def _by_year(result: YearResult) -> int:
    return -result.data_year


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Load FBI Return A master files.")
    parser.add_argument(
        "--years",
        default="2025-2015",
        help="e.g. 2025-2015 or 2023,2024 (default 2025-2015)",
    )
    parser.add_argument(
        "--no-publish", action="store_true", help="load without publishing the release"
    )
    parser.add_argument("--skip-rollups", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)

    from data_ingestion_toolbox.fbi_ucr.schema import _get_hook, ensure_fbi_schema

    hook = _get_hook()
    ensure_fbi_schema(hook)
    summary = load(
        hook.get_conn, parse_years(args.years), publish_release=not args.no_publish
    )
    print(json.dumps(summary, indent=2, default=str))
    if summary["failed_years"]:
        # Exit non-zero so the operator reruns; loaded years are skipped then.
        return 1
    if not args.no_publish and not args.skip_rollups:
        from data_ingestion_toolbox.semantics.rollups import refresh_calendar_rollups

        with hook.get_conn() as connection:
            print(
                json.dumps({"rollups": refresh_calendar_rollups(connection, "FBI_UCR")})
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
