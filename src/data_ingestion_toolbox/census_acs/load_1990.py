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
    parse_header,
    parse_segment,
    read_dbf,
    requested_cells,
    revision_values,
)

SOURCE_CODE = "CENSUS_ACS"
ARCHIVE_ROOT = "https://www2.census.gov/census_1990"
PARSER_VERSION = "census-stf1990-v1"

#: The file-name stem of each dataset's geographic header table.
HEADER_STEM = {"dec1990stf1": "stf1a0", "dec1990stf3": "stf300"}


def segment_files(tables_payload: bytes, cells: set[str]) -> dict[str, set[str]]:
    """Which segment table carries each requested cell, by file-name stem.

    `tables.dbf` lists every cell with the segment it lives in (`STF314`,
    `STF1A0`); the file is that name in lower case plus the state's postal
    code. A requested cell `tables.dbf` does not list refuses.
    """
    _, rows = read_dbf(tables_payload)
    segment_of = {row["FIELD"]: row["SEGMENT"] for row in rows if row.get("FIELD")}
    missing = cells - set(segment_of)
    if missing:
        raise Stf1990FormatError(f"tables.dbf does not list {sorted(missing)}")
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
        header_file = fetch(f"{HEADER_STEM[dataset]}{state}.dbf")
        header = parse_header(header_file.payload)
        by_record: dict[str, dict[str, str]] = {}
        for stem, stem_cells in sorted(stems.items()):
            payload = (
                header_file.payload
                if stem == HEADER_STEM[dataset]
                else fetch(f"{stem}{state}.dbf").payload
            )
            for record, values in parse_segment(payload, stem_cells).items():
                by_record.setdefault(record, {}).update(values)
        rows = revision_values(dataset, header, by_record)
        fresh = [row for row in rows if geography_key(row) not in seen]
        totals["repeated"] += len(rows) - len(fresh)
        seen.update(geography_key(row) for row in fresh)
        totals["rows"] += insert(header_file.capture_id, fresh)
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
    client = httpx.Client(follow_redirects=True, timeout=300)

    def capture(url: str, parameters: dict[str, object]) -> CapturedFile:
        request = control.start_request(
            run_id=run_id, endpoint=url, parameters=parameters, max_attempts=3
        )
        try:
            response = None
            for attempt in range(1, 4):
                try:
                    response = client.get(url)
                    response.raise_for_status()
                    break
                except httpx.HTTPError as exc:
                    if attempt == 3:
                        raise
                    control.record_request_retry(request.request_id, error=exc)
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
            for state in only_states or states(dataset):
                if state not in discs_1990()[dataset]:
                    continue
                results[f"{dataset}/{state}"] = load_state(
                    dataset, state, capture=capture, insert=insert
                )
    except BaseException as exc:
        control.finish_run(run_id, status="failed", error=exc)
        raise
    control.finish_run(run_id, status="success")
    return results
