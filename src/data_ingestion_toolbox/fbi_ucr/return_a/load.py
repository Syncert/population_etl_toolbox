"""Load the Return A master files into the FBI silver tables, one year at a time.

The weekly ``fbi_ucr_ingest`` run calls ``load`` for every registered year
(``registry.registered_years``: 2015 through last year), so a year never
loaded is loaded by the next scheduled run (LOADER RULE). An operator can
still run it by hand inside the Airflow image::

    docker exec docker-airflow-scheduler-1 \\
        python -m data_ingestion_toolbox.fbi_ucr.return_a.load --years 2025-2015

What a run does:

1. Downloads each requested year's zip and commits it to ``raw_capture``
   before anything is parsed (the payload store keeps one copy of identical
   bytes). Last year's file may not be published yet: a "not found" for it
   is recorded ``not_published``, never a failure.
2. Loads into one standing release per product: the newest Return A release
   already in the warehouse, or, on the first load, a release keyed by the
   newest file's ``Last-Modified`` date. A changed file revises that release
   in place; it never starts a second copy of all eleven years (CHURN RULE).
3. Skips, without reading it, a year whose ledger row in
   ``control.fbi_return_a_year`` holds the same bytes under the same parser
   contract with no county added to the geography reference since (the
   repeats gate of ``utility.unchanged``). So an unchanged week writes
   nothing, and a run that stops part way resumes where it stopped.
4. Otherwise builds the year's rows (county crosswalk, agencies,
   agency-county links, monthly participation and offense rows for the two
   products below) in staging and writes only the difference, in one
   transaction per year: a new row is inserted, a row whose values differ is
   updated, a row the file no longer holds is deleted, and an identical row
   is not touched -- it keeps naming the line it was first read from. Every
   line a written fact names is recorded in ``silver_fbi.return_a_source_row``
   in the same transaction, and a source row no fact names any more is
   removed.
5. Publishes the release once every requested year is in, and only when a
   row changed or it was not yet published, then refreshes the county rates.

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
from collections.abc import Callable, Iterable, Mapping, Sequence
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
from data_ingestion_toolbox.fbi_ucr.gold_fbi.publisher import refresh_county_agency_rate

from .fetch import (
    NOT_PUBLISHED_STATUSES,
    FetchedFile,
    ReturnAFetchError,
    fetch_year,
    unzip_text,
)
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
    #: Fact rows this load wrote: inserted, updated, deleted (CHURN RULE:
    #: an identical row is none of these).
    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    error: str | None = None


class YearsFailed(RuntimeError):
    """Some requested years could not be fetched or loaded; the rest were."""


class YearNotPublished(RuntimeError):
    """The newest registered year's file is not on the CDE yet."""


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


def _agencies(records: Iterable[ReturnARecord]) -> list[ReturnARecord]:
    """One record per ORI, the file's last, in ORI order: the rows a year loads."""
    return sorted(
        {record.ori: record for record in records}.values(),
        key=lambda record: record.ori,
    )


def line_checksums(text: str, line_numbers: Iterable[int]) -> dict[int, str]:
    """SHA-256 of each named line of a year's file, as ``iter_records`` read it."""
    lines = text.split("\n")
    return {
        number: hashlib.sha256(
            lines[number - 1].rstrip("\r").encode("latin-1")
        ).hexdigest()
        for number in line_numbers
    }


def _write_source_rows(
    cursor: Any,
    *,
    run_id: UUID,
    release_key: str,
    year: int,
    capture_id: UUID,
    agencies: Sequence[ReturnARecord],
    checksums: Mapping[int, str],
    only_written: bool = False,
) -> int:
    """Record the line every fact of these agencies was built from.

    Inserts only lines not already recorded, so a rerun writes nothing. With
    ``only_written``, only the lines a fact written in this transaction names
    (``return_a_written_line``): an unchanged fact keeps naming its earlier
    line, so recording the new file's copy of that line would store a row
    nothing reads.
    """
    cursor.execute(
        """
        CREATE TEMP TABLE return_a_source_row_stage
            (LIKE silver_fbi.return_a_source_row INCLUDING DEFAULTS) ON COMMIT DROP
        """
    )
    _copy(
        cursor,
        "return_a_source_row_stage",
        (
            "capture_id",
            "source_row_index",
            "run_id",
            "release_key",
            "data_year",
            "ori",
            "line_checksum",
            "parser_contract_version",
        ),
        (
            (
                str(capture_id),
                record.line_number,
                str(run_id),
                release_key,
                year,
                record.ori,
                checksums[record.line_number],
                PARSER_VERSION,
            )
            for record in agencies
        ),
    )
    cursor.execute(
        """
        INSERT INTO silver_fbi.return_a_source_row
        SELECT * FROM return_a_source_row_stage AS stage
        """
        + (
            """
        WHERE stage.source_row_index IN (
            SELECT source_row_index FROM return_a_written_line)
        """
            if only_written
            else ""
        )
        + """
        ORDER BY capture_id, source_row_index
        ON CONFLICT (capture_id, source_row_index) DO NOTHING
        """
    )
    inserted = cursor.rowcount
    cursor.execute("DROP TABLE return_a_source_row_stage")
    return inserted


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
    *,
    unpublished_ok: bool = False,
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
    except ReturnAFetchError as error:
        if unpublished_ok and error.status_code in NOT_PUBLISHED_STATUSES:
            # Not on the CDE yet: answered, empty, and asked again next week.
            control.finish_request(request.request_id, status="empty", error=error)
            raise YearNotPublished(fetched_key) from error
        control.finish_request(request.request_id, status="failed", error=error)
        raise
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


@dataclass(frozen=True)
class _Table:
    """How one silver table's rows for a year are diffed against staging.

    ``key`` identifies a row; ``compare`` are its values (an update happens
    only when one of them differs); ``lineage`` names what the row was read
    from and is rewritten only together with a changed value, so an
    identical row keeps naming the line it was first read from.
    """

    name: str
    stage: str
    key: tuple[str, ...]
    compare: tuple[str, ...]
    lineage: tuple[str, ...] = ()
    #: Extra ``SET`` text for an update, e.g. ``updated_at = NOW()``.
    touch: str = ""
    #: Record each written row's ``source_row_index`` in
    #: ``return_a_written_line`` (facts only).
    names_line: bool = False

    @property
    def columns(self) -> tuple[str, ...]:
        seen = dict.fromkeys(self.key + self.compare + self.lineage)
        return tuple(seen)


_LINEAGE = ("source_run_id", "capture_id", "source_row_index", "transformation_version")

CROSSWALK = _Table(
    "silver_fbi.return_a_county_crosswalk",
    "return_a_crosswalk_stage",
    key=("release_key", "data_year", "state_code", "fbi_county_code"),
    compare=("geo_id", "geo_sk", "method", "status", "reason_code", "evidence"),
)
RELATIONSHIP = _Table(
    "silver_fbi.agency_geography_relationship",
    "return_a_relationship_stage",
    # The table's own unique key, which has no release_key.
    key=(
        "product_id",
        "ori",
        "relationship_type",
        "source_label",
        "geography_vintage",
        "effective_start",
    ),
    compare=(
        "release_key",
        "geo_id",
        "geo_sk",
        "resolution_method",
        "resolution_status",
        "confidence_class",
        "reason_code",
        "effective_end",
        "evidence_source",
    ),
    lineage=("evidence_capture_id",),
    touch="updated_at = NOW()",
)
PARTICIPATION = _Table(
    "silver_fbi.fact_reporting_participation",
    "return_a_participation_stage",
    key=("product_id", "release_key", "subject_type", "subject_code", "period"),
    compare=(
        "ucr_program",
        "subject_label",
        "source_geo_level",
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
    ),
    lineage=_LINEAGE,
    names_line=True,
)
OBSERVATION = _Table(
    "silver_fbi.fact_crime_observation",
    "return_a_observation_stage",
    key=("product_id", "release_key", "source_record_id"),
    compare=(
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
    ),
    lineage=_LINEAGE,
    names_line=True,
)


def _stage(
    cursor: Any,
    table: _Table,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
) -> int:
    """Copy a year's rows for ``table`` into an empty temp table of its shape.

    ``columns`` orders the values of each row and must name exactly the
    table's diffed columns. Built from the column list, not ``LIKE``, so a
    serial key's default is not copied and staging draws no sequence value.
    """
    if sorted(columns) != sorted(table.columns):
        raise ValueError(f"{table.name}: staged columns differ from the diffed ones")
    cursor.execute(
        f"CREATE TEMP TABLE {table.stage} ON COMMIT DROP AS "
        f"SELECT {', '.join(columns)} FROM {table.name} WITH NO DATA"
    )
    count = _copy(cursor, table.stage, columns, rows)
    cursor.execute(f"ANALYZE {table.stage}")
    return count


def _matches(table: _Table, left: str, right: str) -> str:
    return " AND ".join(f"{left}.{column} = {right}.{column}" for column in table.key)


def _delete_missing(
    cursor: Any, table: _Table, scope: str, parameters: Sequence[Any]
) -> int:
    """Delete the year's stored rows the staged file no longer holds."""
    cursor.execute(
        f"""
        DELETE FROM {table.name} AS stored
        WHERE {scope}
          AND NOT EXISTS (
              SELECT 1 FROM {table.stage} AS staged
              WHERE {_matches(table, "staged", "stored")})
        """,
        tuple(parameters),
    )
    return int(cursor.rowcount)


def _written(table: _Table, statement: str, returning: str) -> str:
    """Wrap a write so each row's line lands in ``return_a_written_line``."""
    if not table.names_line:
        return statement
    return (
        f"WITH written AS ({statement} RETURNING {returning}) "
        "INSERT INTO return_a_written_line SELECT source_row_index FROM written"
    )


def _update_changed(
    cursor: Any, table: _Table, scope: str, parameters: Sequence[Any]
) -> int:
    """Update only stored rows whose values differ from the staged file's."""
    assignments = [f"{column} = staged.{column}" for column in table.compare]
    assignments += [f"{column} = staged.{column}" for column in table.lineage]
    if table.touch:
        assignments.append(table.touch)
    stored_values = ", ".join(f"stored.{column}" for column in table.compare)
    staged_values = ", ".join(f"staged.{column}" for column in table.compare)
    cursor.execute(
        _written(
            table,
            f"""
            UPDATE {table.name} AS stored
               SET {", ".join(assignments)}
              FROM {table.stage} AS staged
             WHERE {_matches(table, "staged", "stored")}
               AND {scope}
               AND ({stored_values}) IS DISTINCT FROM ({staged_values})
            """,
            "staged.source_row_index",
        ),
        tuple(parameters),
    )
    return int(cursor.rowcount)


def _insert_new(cursor: Any, table: _Table) -> int:
    """Insert staged rows with no stored row of the same key."""
    columns = ", ".join(table.columns)
    cursor.execute(
        _written(
            table,
            f"""
            INSERT INTO {table.name} ({columns})
            SELECT {", ".join(f"staged.{column}" for column in table.columns)}
            FROM {table.stage} AS staged
            WHERE NOT EXISTS (
                SELECT 1 FROM {table.name} AS stored
                WHERE {_matches(table, "stored", "staged")})
            ORDER BY {", ".join(f"staged.{column}" for column in table.key)}
            """,
            "source_row_index",
        )
    )
    return int(cursor.rowcount)


def _remove_unnamed_source_rows(cursor: Any, release_key: str, year: int) -> int:
    """Remove the year's source rows no fact names any more.

    A changed or removed agency's earlier line is left behind by the diff.
    Matched by release and year, not by capture, so a row some fact of any
    capture still names stays.
    """
    cursor.execute(
        """
        DELETE FROM silver_fbi.return_a_source_row AS source_row
        WHERE source_row.release_key = %s AND source_row.data_year = %s
          AND NOT EXISTS (
              SELECT 1 FROM silver_fbi.fact_crime_observation AS fact
              WHERE fact.capture_id = source_row.capture_id
                AND fact.source_row_index = source_row.source_row_index)
          AND NOT EXISTS (
              SELECT 1 FROM silver_fbi.fact_reporting_participation AS fact
              WHERE fact.capture_id = source_row.capture_id
                AND fact.source_row_index = source_row.source_row_index)
        """,
        (release_key, year),
    )
    return int(cursor.rowcount)


def _load_year(
    cursor: Any,
    *,
    run_id: UUID,
    release_key: str,
    year: int,
    capture_id: UUID,
    records: list[ReturnARecord],
    counties: list[CountyRef],
    checksums: Mapping[int, str],
) -> YearResult:
    cursor.execute(
        "CREATE TEMP TABLE return_a_written_line (source_row_index BIGINT) "
        "ON COMMIT DROP"
    )
    crosswalk = build_crosswalk(records, counties)
    _stage(
        cursor,
        CROSSWALK,
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

    agencies = _agencies(records)
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
           SET last_seen_release = EXCLUDED.last_seen_release,
               updated_at = NOW()
         WHERE silver_fbi.dim_agency.last_seen_release < EXCLUDED.last_seen_release
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
    _stage(
        cursor,
        RELATIONSHIP,
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
    participation = _stage(
        cursor,
        PARTICIPATION,
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
    observations = _stage(
        cursor,
        OBSERVATION,
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

    # Write only the difference (CHURN RULE). Facts that went away first, the
    # offense rows before the participation rows they reference; then each
    # table's changed rows and new rows, participation before offenses.
    products = [product.product_id for product in PRODUCTS]
    fact_scope = (
        "stored.product_id = ANY(%s) AND stored.release_key = %s"
        " AND stored.period_start BETWEEN %s AND %s"
    )
    fact_parameters = (products, release_key, start, end)
    deleted = _delete_missing(cursor, OBSERVATION, fact_scope, fact_parameters)
    deleted += _delete_missing(cursor, PARTICIPATION, fact_scope, fact_parameters)
    relationship_scope = (
        "stored.product_id = ANY(%s) AND stored.release_key = %s"
        " AND stored.effective_start = %s"
    )
    relationship_parameters = (products, release_key, start)
    _delete_missing(cursor, RELATIONSHIP, relationship_scope, relationship_parameters)
    crosswalk_scope = "stored.release_key = %s AND stored.data_year = %s"
    crosswalk_parameters = (release_key, year)
    _delete_missing(cursor, CROSSWALK, crosswalk_scope, crosswalk_parameters)
    _update_changed(cursor, CROSSWALK, crosswalk_scope, crosswalk_parameters)
    _insert_new(cursor, CROSSWALK)
    _update_changed(cursor, RELATIONSHIP, relationship_scope, relationship_parameters)
    _insert_new(cursor, RELATIONSHIP)
    updated = _update_changed(cursor, PARTICIPATION, fact_scope, fact_parameters)
    inserted = _insert_new(cursor, PARTICIPATION)
    updated += _update_changed(cursor, OBSERVATION, fact_scope, fact_parameters)
    inserted += _insert_new(cursor, OBSERVATION)

    # Every line a written fact names, in the same transaction as the facts;
    # then the lines no fact names any more.
    _write_source_rows(
        cursor,
        run_id=run_id,
        release_key=release_key,
        year=year,
        capture_id=capture_id,
        agencies=agencies,
        checksums=checksums,
        only_written=True,
    )
    if updated or deleted:
        _remove_unnamed_source_rows(cursor, release_key, year)
    unresolved = sum(1 for row in crosswalk if row.status != "resolved")
    return YearResult(
        year,
        "loaded",
        len(records),
        observations,
        participation,
        unresolved,
        inserted=inserted,
        updated=updated,
        deleted=deleted,
    )


@dataclass(frozen=True)
class _LedgerYear:
    payload_checksum: str
    parser_contract: str | None
    loaded_at: datetime


def _standing_release(cursor: Any) -> tuple[str, date, str] | None:
    """The newest Return A release: (release_key, refresh_date, status).

    Every load revises this one release in place. Keyed afresh per changed
    file, each new key would copy all eleven years again as new facts, and
    its agency links would collide with the earlier key's on the link table's
    release-free unique key.
    """
    cursor.execute(
        """
        SELECT release_key, refresh_date,
               CASE WHEN bool_and(status = 'published') THEN 'published'
                    ELSE 'replaying' END
        FROM silver_fbi.dim_ucr_dataset_release
        WHERE product_id = ANY(%s)
        GROUP BY release_key, refresh_date
        ORDER BY refresh_date DESC, release_key DESC
        LIMIT 1
        """,
        ([product.product_id for product in PRODUCTS],),
    )
    row = cursor.fetchone()
    return None if row is None else (str(row[0]), row[1], str(row[2]))


def _ledger(cursor: Any, release_key: str) -> dict[int, _LedgerYear]:
    """Each loaded year's bytes, parser contract and load time."""
    cursor.execute(
        """
        SELECT loaded.data_year, loaded.payload_checksum,
               capture.payload_schema_version, loaded.loaded_at
        FROM control.fbi_return_a_year AS loaded
        LEFT JOIN raw_capture.response_capture AS capture
          ON capture.capture_id = loaded.capture_id
        WHERE loaded.release_key = %s
        """,
        (release_key,),
    )
    return {
        int(year): _LedgerYear(checksum, contract, loaded_at)
        for year, checksum, contract, loaded_at in cursor.fetchall()
    }


def _newest_county_added(cursor: Any) -> datetime | None:
    cursor.execute(
        "SELECT MAX(created_at) FROM silver_ref.dim_geo_entity WHERE geo_type = 'county'"
    )
    return cursor.fetchone()[0]


def unchanged(
    ledger: _LedgerYear | None,
    payload_checksum: str,
    newest_county_added: datetime | None,
) -> bool:
    """Whether loading these bytes would only repeat the year's stored rows.

    The same bytes under the same parser contract, with no county entering
    the geography reference since the year was loaded (a new county could
    resolve an FBI county code the earlier load left unresolved). The gate of
    ``utility.unchanged.repeats_last_publication``, narrowed to counties
    because Return A resolves no other geography: an agency ORI added for
    another FBI product never forces a re-read.
    """
    if ledger is None:
        return False
    if ledger.payload_checksum != payload_checksum:
        return False
    if ledger.parser_contract != PARSER_VERSION:
        return False
    return newest_county_added is None or newest_county_added <= ledger.loaded_at


def _record_ledger(
    cursor: Any,
    release_key: str,
    year: int,
    capture_id: UUID,
    checksum: str,
    result: YearResult,
) -> None:
    cursor.execute(
        """
        INSERT INTO control.fbi_return_a_year (
            release_key, data_year, capture_id, payload_checksum, record_count,
            observation_count, participation_count, unresolved_county_codes
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
            year,
            str(capture_id),
            checksum,
            result.records,
            result.observations,
            result.participation,
            result.unresolved_county_codes,
        ),
    )


def publish(cursor: Any, release_key: str, refresh_date: date | None = None) -> None:
    """Mark the release reconciled and published, with its true extent.

    ``refresh_date`` (the newest file's ``Last-Modified``) moves the
    release's refresh date forward, never back.
    """
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
                   refresh_date = GREATEST(
                       release.refresh_date,
                       COALESCE(%(refresh)s::date, release.refresh_date)),
                   status = 'published', reconciled_at = NOW(),
                   published_at = NOW(), updated_at = NOW()
              FROM extent
             WHERE release.product_id = %(product)s
               AND release.release_key = %(release)s
               AND extent.last_month IS NOT NULL
            """,
            {
                "product": product.product_id,
                "release": release_key,
                "refresh": refresh_date,
            },
        )
        refresh_county_agency_rate(cursor, product.product_id)


def load(
    connection_factory: Callable[[], Any],
    years: Sequence[int],
    *,
    fetch: Callable[[int], FetchedFile] | None = None,
    publish_release: bool = True,
    may_be_unpublished: int | None = None,
) -> dict[str, Any]:
    """Capture, load and publish the requested years; resumable and idempotent.

    ``may_be_unpublished`` names the one year (the scheduled run's newest
    registered year) whose "not found" means "not published yet" while it
    has never been loaded: it is recorded ``not_published`` and does not hold
    the release back. Any other year's refusal is a failure.
    """
    # Resolved per call, not bound as a default, so a test boundary stub of
    # ``fetch_year`` reaches the scheduled task too.
    fetch = fetch or fetch_year
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
        database = connection_factory()
        try:
            with database.cursor() as cursor:
                standing = _standing_release(cursor)
                ledger = _ledger(cursor, standing[0]) if standing else {}
                newest_county = _newest_county_added(cursor)
            database.commit()

            captured = []
            for year in years:
                # A year whose signed URL or download fails is recorded (its
                # request is marked failed in the capture ledger) and the run
                # moves on to the next year; a rerun picks it up.
                try:
                    captured.append(
                        _capture_year(
                            connection_factory,
                            control,
                            run_id,
                            year,
                            fetch,
                            unpublished_ok=(
                                year == may_be_unpublished and year not in ledger
                            ),
                        )
                    )
                except YearNotPublished:
                    logger.info("Return A %s is not published yet", year)
                    results.append(YearResult(year, "not_published"))
                except Exception as error:
                    logger.warning("Return A %s could not be fetched: %s", year, error)
                    failed.append(YearResult(year, "fetch_failed", error=repr(error)))
            if not captured and failed:
                raise YearsFailed(f"no Return A year could be fetched: {failed}")
            modified = [
                file.last_modified for file, _, _ in captured if file.last_modified
            ]
            newest_modified = max(modified) if modified else None
            if standing:
                release_key = standing[0]
            elif captured:
                release_key = (newest_modified or datetime.now(UTC).date()).isoformat()

            changed = False
            if captured:
                with database.cursor() as cursor:
                    _ensure_release(cursor, release_key, captured[0][1], run_id)
                    counties = _county_reference(cursor)
                database.commit()
            for fetched, capture_id, checksum in captured:
                if unchanged(ledger.get(fetched.data_year), checksum, newest_county):
                    results.append(YearResult(fetched.data_year, "already_loaded"))
                    continue
                try:
                    text = unzip_text(fetched.payload)
                    records = list(iter_records(text))
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
                            checksums=line_checksums(
                                text, (r.line_number for r in _agencies(records))
                            ),
                        )
                        _record_ledger(
                            cursor,
                            release_key,
                            fetched.data_year,
                            capture_id,
                            checksum,
                            result,
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
                changed = changed or bool(
                    result.inserted or result.updated or result.deleted
                )
            # The release publishes only once every requested year is in, and
            # only when a row changed or it was not published yet: an
            # unchanged week rewrites nothing, not even the release row.
            # A release already serving is revised in place, so a year that
            # failed keeps its earlier rows and the rows that did change must
            # reach the county rates now; a first release waits for every year.
            serving = standing is not None and standing[2] == "published"
            if (
                publish_release
                and captured
                and (changed or not serving)
                and (serving or not failed)
            ):
                with database.cursor() as cursor:
                    publish(cursor, release_key, newest_modified)
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


def load_registered_years(connection_factory: Callable[[], Any]) -> dict[str, Any]:
    """The scheduled run: every registered year, the newest allowed to be absent."""
    from .registry import registered_years

    years = registered_years()
    return load(connection_factory, years, may_be_unpublished=years[0])


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
