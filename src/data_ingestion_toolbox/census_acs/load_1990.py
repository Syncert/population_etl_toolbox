"""Capture and replay the 1990 census from the Bureau's archived discs.

For each 1990 file (STF1A, STF3A) and state, every disc holding the state
is read: its `tables.dbf` says which segment table carries each cell the
1990 crosswalk sums, the geographic header and those segments are captured
byte for byte, and `stf1990.revision_values` replays them into
`silver_census.observation_revision` as year 1990. A state split across
discs repeats its state, counties and places on each; a geography an earlier
disc already gave is not replayed again (decennial-2000-and-1990-history).

The transform and serving that follow are the ACS adapter's own:
`transform_census_to_silver(years=[1990])` and the chunked serving refresh.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID, uuid4

from data_ingestion_toolbox.census_acs.silver_census.replay import (
    insert_revision_rows,
)
from data_ingestion_toolbox.census_acs.stf1990 import (
    DATASETS_1990,
    Stf1990FormatError,
    discs_1990,
    is_dbf,
    parse_header,
    parse_segment,
    read_dbf,
    requested_cells,
    revision_values,
    segment_tracts,
)


class RejectedDownload(Stf1990FormatError):
    """The archive answered a file request with something that is not the file.

    www2.census.gov's firewall answers some requests with a "Request
    Rejected" HTML page under HTTP 200; on 2026-10-30 it refused Minnesota's
    second STF3A header (`CD90_3A_31/stf300mn.dbf`) every time.
    """


logger = logging.getLogger(__name__)

SOURCE_CODE = "CENSUS_ACS"
ARCHIVE_ROOT = "https://www2.census.gov/census_1990"
PARSER_VERSION = "census-stf1990-v1"

#: The file-name stem of each dataset's geographic header table.
HEADER_STEM = {"dec1990stf1": "stf1a0", "dec1990stf3": "stf300"}


#: What each table the 1990 crosswalks read is titled in `tables.dbf`. A
#: cell's name says only its table and position, and Puerto Rico's STF3A
#: numbers its tables differently (its P049 is period of military service,
#: the states' is means of transportation to work), so a disc is read only
#: when every table carries the title the crosswalk was reviewed against.
TABLE_TITLES: dict[str, str] = {
    "P001": "PERSONS",
    "P003": "HOUSEHOLDS",
    "P008": "PERSONS OF HISPANIC ORIGIN",
    "P010": "HISPANIC ORIGIN BY RACE",
    "H001": "HOUSING UNITS",
    "H002": "OCCUPANCY STATUS",
    "H003": "TENURE",
    "P049": "MEANS OF TRANSPORTATION TO WORK",
    "P080A": "MEDIAN HOUSEHOLD INCOME IN 1989",
    "P114A": "PER CAPITA INCOME IN 1989",
    "P117": "POVERTY STATUS IN 1989 BY AGE",
    "H043A": "MEDIAN GROSS RENT",
    "H061A": "MEDIAN VALUE",
}


class LayoutMismatch(Stf1990FormatError):
    """A disc's tables are not the ones the crosswalk was reviewed against."""


def table_of(cell: str) -> str:
    """The table a cell belongs to: `P0490001` is P049, `P080A001` is P080A."""
    return cell[:5] if cell[4].isalpha() else cell[:4]


def segment_files(tables_payload: bytes, cells: set[str]) -> dict[str, set[str]]:
    """Which segment table carries each requested cell, by file-name stem.

    `tables.dbf` lists every cell with the segment it lives in (`STF314`,
    `STF1A0`); the file is that name in lower case plus the state's postal
    code. Each requested table's title must be the one in `TABLE_TITLES`, and
    every requested cell must be listed; otherwise the disc is refused.
    """
    _, rows = read_dbf(tables_payload)
    titles: dict[str, str] = {}
    for row in rows:
        if not row.get("FIELD") and row.get("SEGMENT"):
            titles.setdefault(row["TABLE"], row["TEXT"].upper())
    for table in sorted({table_of(cell) for cell in cells}):
        expected = TABLE_TITLES.get(table)
        if expected is None or not titles.get(table, "").startswith(expected):
            raise LayoutMismatch(
                f"table {table} is titled {titles.get(table)!r}, not {expected!r}"
            )
    segment_of = {row["FIELD"]: row["SEGMENT"] for row in rows if row.get("FIELD")}
    missing = cells - set(segment_of)
    if missing:
        raise LayoutMismatch(f"tables.dbf does not list {sorted(missing)}")
    stems: dict[str, set[str]] = {}
    for cell in cells:
        stems.setdefault(segment_of[cell].lower(), set()).add(cell)
    return stems


def geography_key(row: dict[str, object]) -> tuple:
    return (
        row["geo_level"],
        row["state_fips_source"],
        row["county_fips_source"],
        row["place_fips_source"],
        row["tract_code_source"],
    )


@dataclass(frozen=True)
class CapturedFile:
    capture_id: UUID
    payload: bytes


#: `capture(url, parameters) -> CapturedFile`: download one file and store it
#: in raw capture before anything parses it.
Capture = Callable[[str, dict[str, object]], CapturedFile]
#: `insert(capture_id, rows) -> count`: write revision rows.
Insert = Callable[[UUID, list[dict[str, object]]], int]


def load_state(
    dataset: str, state: str, *, capture: Capture, insert: Insert
) -> dict[str, int]:
    """Capture and replay one state's 1990 file from every disc holding it."""
    if dataset not in DATASETS_1990:
        raise ValueError(f"{dataset!r} is not a 1990 file")
    discs = discs_1990()[dataset].get(state)
    if not discs:
        raise ValueError(f"no archived disc holds {dataset} for {state!r}")
    cells = requested_cells(dataset)
    seen: set[tuple] = set()
    totals = {"discs": 0, "rows": 0, "repeated": 0}
    for disc in discs:
        base = f"{ARCHIVE_ROOT}/{disc}"

        def fetch(name: str) -> CapturedFile:
            return capture(
                f"{base}/{name}",
                {"dataset": dataset, "state": state, "disc": disc, "file": name},
            )

        stems = segment_files(fetch("tables.dbf").payload, cells)
        try:
            header_file: CapturedFile | None = fetch(
                f"{HEADER_STEM[dataset]}{state}.dbf"
            )
        except RejectedDownload:
            # A disc after the first adds tracts only (its state, counties
            # and places are on every disc), and each segment carries its
            # tracts' keys, so the tracts are read from the segments -- when
            # no requested cell lives in the header itself (STF3A's do not).
            if disc == discs[0] or HEADER_STEM[dataset] in stems:
                raise
            header_file = None
        by_record: dict[str, dict[str, str]] = {}
        segment_capture = None
        header: list = []
        for stem, stem_cells in sorted(stems.items()):
            if header_file is not None and stem == HEADER_STEM[dataset]:
                segment = header_file
            else:
                try:
                    segment = fetch(f"{stem}{state}.dbf")
                except RejectedDownload:
                    # The archive refuses some segments outright (PA's
                    # CD90_3A_50/stf330pa.dbf, median gross rent). Its cells
                    # stay absent for this disc's geographies; the rest of
                    # the state still loads.
                    totals["segments_refused"] = totals.get("segments_refused", 0) + 1
                    continue
            if header_file is None and segment_capture is None:
                segment_capture = segment
                header = segment_tracts(segment.payload)
            for record, values in parse_segment(segment.payload, stem_cells).items():
                by_record.setdefault(record, {}).update(values)
        if header_file is not None:
            header = parse_header(header_file.payload)
            row_capture = header_file
        else:
            totals["header_fallbacks"] = totals.get("header_fallbacks", 0) + 1
            row_capture = segment_capture
        rows = revision_values(dataset, header, by_record)
        fresh = [row for row in rows if geography_key(row) not in seen]
        totals["repeated"] += len(rows) - len(fresh)
        seen.update(geography_key(row) for row in fresh)
        totals["rows"] += insert(row_capture.capture_id, fresh)
        totals["discs"] += 1
    return totals


def states(dataset: str) -> list[str]:
    """Every state's postal code the archive holds the file for."""
    return sorted(discs_1990()[dataset])


# ------------------------------------------------------------ the warehouse


def warehouse_capture(connection_factory: Callable, run_id: UUID) -> Capture:
    """A `Capture` that downloads with retries and persists to raw capture."""
    import httpx

    from data_ingestion_toolbox.capture import (
        CaptureControl,
        ResponseCapture,
        persist_response_capture,
    )

    control = CaptureControl(connection_factory, source_code=SOURCE_CODE)
    client = httpx.Client(
        follow_redirects=True,
        timeout=300,
        headers={"User-Agent": "population-etl-toolbox (census 1990 archive load)"},
    )

    def capture(url: str, parameters: dict[str, object]) -> CapturedFile:
        request = control.start_request(
            run_id=run_id, endpoint=url, parameters=parameters, max_attempts=4
        )
        try:
            response = None
            for attempt in range(1, 5):
                try:
                    response = client.get(url)
                    response.raise_for_status()
                    # A rejection page is not the file: never store it as one.
                    if not is_dbf(response.content):
                        raise RejectedDownload(
                            f"{url} answered {response.headers.get('content-type')!r}, "
                            "not a dBase table"
                        )
                    break
                except (httpx.HTTPError, RejectedDownload) as exc:
                    if attempt == 4:
                        raise
                    control.record_request_retry(request.request_id, error=exc)
                    time.sleep(15 * attempt)
            assert response is not None
            capture_id = uuid4()
            persist_response_capture(
                connection_factory,
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
                    media_type="application/x-dbase",
                    payload=response.content,
                    payload_schema_version=PARSER_VERSION,
                    source_revision="1990",
                ),
            )
        except BaseException as exc:
            control.finish_request(request.request_id, status="failed", error=exc)
            raise
        control.finish_request(request.request_id, status="captured")
        return CapturedFile(capture_id, response.content)

    return capture


def loaded_states(connection_factory: Callable, dataset: str) -> set[str]:
    """States whose every disc already has replayed revision rows.

    A rerun after a failure resumes at the first state not finished; a split
    state with any disc missing is read again in full, since its discs share
    geographies and are deduplicated together.
    """
    discs = discs_1990()[dataset]
    database = connection_factory()
    try:
        with database.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT capture.request_parameters->>'state',
                                capture.request_parameters->>'disc'
                FROM raw_capture.response_capture AS capture
                WHERE capture.source_code = %s
                  AND capture.request_parameters->>'dataset' = %s
                  AND EXISTS (
                      SELECT 1 FROM silver_census.observation_revision AS r
                      WHERE r.capture_id = capture.capture_id
                  )
                """,
                (SOURCE_CODE, dataset),
            )
            replayed = cursor.fetchall()
    finally:
        database.close()
    by_state: dict[str, set[str]] = {}
    for state, disc in replayed:
        by_state.setdefault(state, set()).add(disc)
    return {
        state for state, have in by_state.items() if set(discs.get(state, [])) <= have
    }


def load(
    connection_factory: Callable,
    datasets: Iterable[str] = DATASETS_1990,
    only_states: Iterable[str] | None = None,
) -> dict[str, dict[str, int]]:
    """Capture and replay the 1990 files into the revision table, one run."""
    from data_ingestion_toolbox.capture import CaptureControl

    control = CaptureControl(connection_factory, source_code=SOURCE_CODE)
    run_id = control.start_run(watermark={"dataset": "dec1990", "year": 1990})
    capture = warehouse_capture(connection_factory, run_id)

    def insert(capture_id: UUID, rows: list[dict[str, object]]) -> int:
        return insert_revision_rows(connection_factory, capture_id, rows)

    results: dict[str, dict[str, int]] = {}
    try:
        for dataset in datasets:
            done = loaded_states(connection_factory, dataset)
            for state in only_states or states(dataset):
                if state not in discs_1990()[dataset]:
                    continue
                if state in done:
                    results[f"{dataset}/{state}"] = {"skipped": 1}
                    continue
                try:
                    results[f"{dataset}/{state}"] = load_state(
                        dataset, state, capture=capture, insert=insert
                    )
                except LayoutMismatch as exc:
                    # Recorded and left out, never read under the wrong table.
                    results[f"{dataset}/{state}"] = {"layout_refused": 1}
                    logger.warning(
                        "[CENSUS_1990] %s %s refused: %s", dataset, state, exc
                    )
    except BaseException as exc:
        control.finish_run(run_id, status="failed", error=exc)
        raise
    control.finish_run(run_id, status="success")
    return results


def main(argv: list[str] | None = None) -> int:
    """Load the 1990 census end to end inside the Airflow image.

        docker exec docker-airflow-scheduler-1 \
            python -m data_ingestion_toolbox.census_acs.load_1990 [--states vt,nh]

    Captures and replays the files, then transforms year 1990, builds its
    gold table and variable rows and serves it with the ACS adapter's own
    steps. Run `silver_ref` first so the 1990
    places and tracts are entities, and apply migration 039.
    """
    import argparse
    import json

    from data_ingestion_toolbox.census_acs.gold_census.transform import (
        refresh_acs_elements,
    )
    from data_ingestion_toolbox.census_acs.silver_census.transform import (
        _get_hook,
        transform_census_to_silver,
    )
    from data_ingestion_toolbox.utility.gold_schema import (
        refresh_serving_layer_in_year_chunks,
    )
    from data_ingestion_toolbox.utility.serving_reserve import ACS_CHUNK_CONFIG

    parser = argparse.ArgumentParser(description="Load the 1990 census.")
    parser.add_argument("--states", help="comma-separated postal codes; all by default")
    parser.add_argument("--datasets", default=",".join(DATASETS_1990))
    parser.add_argument("--skip-serving", action="store_true")
    args = parser.parse_args(argv)

    hook = _get_hook()
    only = [s.strip().lower() for s in args.states.split(",")] if args.states else None
    results = load(hook.get_conn, datasets=args.datasets.split(","), only_states=only)
    print(json.dumps(results, indent=2))
    transformed = transform_census_to_silver(resume_key="load_1990", years=[1990])
    print(json.dumps({"transformed": transformed}))
    # The serving join reads 1990's gold table and variable rows; without
    # them a serve publishes nothing.
    elements = refresh_acs_elements(hook=hook, years=[1990])
    print(json.dumps({"elements": elements}))
    if not args.skip_serving:
        served = refresh_serving_layer_in_year_chunks(
            hook=hook, config=ACS_CHUNK_CONFIG
        )
        print(json.dumps({"served": served}, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
