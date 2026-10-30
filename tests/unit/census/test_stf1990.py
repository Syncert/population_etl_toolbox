"""Reading the 1990 Summary Tape Files from the Bureau's archive.

Covers: ETL-095
"""

from __future__ import annotations

import struct

import pytest

from data_ingestion_toolbox.census_acs.stf1990 import (
    Stf1990FormatError,
    parse_header,
    parse_segment,
    read_dbf,
    tract_code_1990,
)

pytestmark = pytest.mark.unit


def _dbf(fields: list[tuple[str, int]], rows: list[list[str]], deleted=()) -> bytes:
    """A dBase III table as the 1990 discs carry it (character fields only)."""
    record_length = 1 + sum(length for _, length in fields)
    header_length = 32 + 32 * len(fields) + 1
    head = struct.pack(
        "<BBBBIHH", 0x03, 90, 1, 1, len(rows), header_length, record_length
    )
    head += b"\0" * 20
    for name, length in fields:
        head += name.encode("ascii").ljust(11, b"\0") + b"C" + b"\0" * 4
        head += bytes([length, 0]) + b"\0" * 14
    head += b"\x0d"
    body = b""
    for index, row in enumerate(rows):
        flag = b"*" if index in deleted else b" "
        body += flag + b"".join(
            value.encode("latin-1").ljust(length)[:length]
            for value, (_, length) in zip(row, fields)
        )
    return head + body + b"\x1a"


HEADER_FIELDS = [
    ("SUMLEV", 3),
    ("GEOCOMP", 2),
    ("LOGRECNU", 6),
    ("STATEFP", 2),
    ("CNTY", 3),
    ("PLACEFP", 5),
    ("TRACTBNA", 6),
    ("AREALAND", 10),
    ("AREAWAT", 10),
    ("ANPSADPI", 66),
]


def _header_rows() -> list[list[str]]:
    return [
        [
            "040",
            "00",
            "000001",
            "50",
            "",
            "",
            "",
            "0024000000",
            "0000900000",
            "Vermont",
        ],
        ["040", "40", "000002", "50", "", "", "", "", "", "Vermont (part)"],
        [
            "050",
            "00",
            "000003",
            "50",
            "001",
            "",
            "",
            "0001000000",
            "0000000000",
            "Addison County",
        ],
        [
            "160",
            "00",
            "000004",
            "50",
            "",
            "00400",
            "",
            "0000003269",
            "0000000000",
            "Albany village",
        ],
        [
            "140",
            "00",
            "000005",
            "50",
            "001",
            "",
            "9601",
            "0000211886",
            "0000000000",
            "BNA 9601",
        ],
        ["150", "00", "000006", "50", "001", "", "9601", "", "", "Block group 1"],
    ]


def test_a_state_header_keeps_its_served_whole_geographies() -> None:
    """Covers: ETL-095 -- state, county, place and tract; components and block groups drop."""
    geographies = parse_header(_dbf(HEADER_FIELDS, _header_rows()))
    by_level = {geography.geo_level: geography for geography in geographies}
    assert set(by_level) == {"state", "county", "place", "tract"}
    assert len(geographies) == 4
    assert by_level["place"].place_fips == "00400"
    assert by_level["place"].name == "Albany village"
    # Areas are thousandths of a square kilometre: 3,269 is 3.269 km².
    assert by_level["place"].land_area_m2 == 3_269_000
    assert by_level["tract"].tract_code == "960100"
    assert by_level["tract"].county_fips == "001"


def test_a_segment_returns_its_named_cells_by_logical_record() -> None:
    """Covers: ETL-095 -- the cells requested, keyed to join the header."""
    segment = _dbf(
        [("LOGRECNU", 6), ("P0010001", 9), ("P0020001", 9)],
        [["000001", "000562758", "000146370"], ["000004", "000000896", "000000210"]],
        deleted={1},
    )
    assert parse_segment(segment, {"P0010001"}) == {"000001": {"P0010001": "000562758"}}
    with pytest.raises(Stf1990FormatError, match="P0990001"):
        parse_segment(segment, {"P0990001"})


@pytest.mark.parametrize(
    "payload",
    [
        b"not a table",
        _dbf([("LOGRECNU", 6)], [["000001"]])[:-8],
    ],
)
def test_a_damaged_table_refuses(payload: bytes) -> None:
    """Covers: ETL-095 -- not dBase, or shorter than its record count says."""
    with pytest.raises(Stf1990FormatError):
        read_dbf(payload)


def test_a_header_without_its_keys_or_with_a_repeat_refuses() -> None:
    """Covers: ETL-095 -- a segment file mistaken for a header, or a repeated record."""
    with pytest.raises(Stf1990FormatError, match="lacks"):
        parse_header(_dbf([("LOGRECNU", 6)], [["000001"]]))
    rows = _header_rows()
    rows.append(list(rows[0]))
    with pytest.raises(Stf1990FormatError, match="repeats"):
        parse_header(_dbf(HEADER_FIELDS, rows))
    with pytest.raises(Stf1990FormatError):
        tract_code_1990("960")


def test_the_1990_reference_comes_from_each_states_stf1a_header() -> None:
    """Covers: ETL-097 -- places and tracts of the 1990s resolve to Census 1990."""
    from data_ingestion_toolbox.silver_ref.geography_pipeline import (
        STF1990_ROOT,
        historical_sub_county_assets,
        history_vintage,
        parse_stf1990_header_capture,
    )

    assert history_vintage(1990, "place") == 1990
    assert history_vintage(1999, "tract") == 1990
    assert history_vintage(2000, "tract") == 2000
    assets = historical_sub_county_assets(1990, "tract")
    assert len(assets) == 51
    assert (
        "attributes",
        "tract",
        f"{STF1990_ROOT}/CD90_1A_1/stf1a0vt.dbf",
    ) in assets

    header = _dbf(HEADER_FIELDS, _header_rows())
    places = parse_stf1990_header_capture(header, geo_type="place")
    tracts = parse_stf1990_header_capture(header, geo_type="tract")
    assert [(p.geo_id, p.name, p.census_geoid) for p in places] == [
        ("state:50|place:00400", "Albany village", "5000400")
    ]
    assert [(t.geo_id, t.census_geoid, t.geography_vintage) for t in tracts] == [
        ("state:50|county:001|tract:960100", "50001960100", 1990)
    ]
    # The same place on two records collapses to one.
    rows = _header_rows()
    rows.append(list(rows[3]))
    rows[-1][2] = "000099"
    same = parse_stf1990_header_capture(_dbf(HEADER_FIELDS, rows), geo_type="place")
    assert [p.name for p in same] == ["Albany village"]
    # One code named two ways (as Pennsylvania's 69336 is in the Bureau's
    # file) is left unpublished rather than chosen between.
    rows[-1][9] = "Alburg village"
    assert (
        parse_stf1990_header_capture(_dbf(HEADER_FIELDS, rows), geo_type="place") == []
    )
