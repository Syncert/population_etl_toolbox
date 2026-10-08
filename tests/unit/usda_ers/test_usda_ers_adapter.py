"""Offline contracts for the USDA ERS county codes and atlas adapter.

Covers: ETL-066
"""

from __future__ import annotations

import importlib
import io
import sys
import zipfile
from decimal import Decimal
from pathlib import Path

import pytest

from data_ingestion_toolbox.usda_ers.client import ErsPayloadError, csv_text
from data_ingestion_toolbox.usda_ers.registry import get_file, registered_files
from data_ingestion_toolbox.usda_ers.silver_usda_ers.parse import parse_file

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "usda_ers"
RUCC = get_file("rucc:2023")
TYPOLOGY = get_file("typology:2025")
ATLAS = get_file("fea:2025-07")


def _fixture(item) -> bytes:  # noqa: ANN001
    return (FIXTURES / item.path.rsplit("/", 1)[1]).read_bytes()


def _values(raw: bytes, item) -> dict[tuple[str, str], object]:  # noqa: ANN001
    return {
        (obs.fips_code, obs.attribute): obs
        for obs in parse_file(raw, item=item).observations
    }


def _atlas_with(rows: list[bytes]) -> bytes:
    """The Atlas fixture with rows appended to its county table."""
    source = zipfile.ZipFile(io.BytesIO(_fixture(ATLAS)))
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as target:
        for info in source.infolist():
            data = source.read(info)
            if info.filename == ATLAS.member:
                data = data + b"".join(row + b"\r\n" for row in rows)
            target.writestr(info, data)
    return out.getvalue()


def test_configuration_imports_without_io_and_holds_no_credential() -> None:
    """Covers: ETL-066 — the files need no key, so there is none to leak."""
    for name in list(sys.modules):
        if name.startswith("data_ingestion_toolbox.usda_ers"):
            del sys.modules[name]
    module = importlib.import_module("data_ingestion_toolbox.usda_ers.config")
    assert not {
        field
        for field in module.ErsConfig.model_fields
        if "key" in field or "token" in field
    }
    assert [item.key for item in registered_files()] == [
        "rucc:2023",
        "typology:2025",
        "fea:2025-07",
    ]


def test_rucc_codes_keep_their_label_and_territories() -> None:
    """Covers: ETL-066 — a code with ERS's label; a planning region and a territory resolve by FIPS."""
    rows = _values(_fixture(RUCC), RUCC)
    kent = rows[("10001", "RUCC_2023")]
    assert (kent.value, kent.code_label, kent.measure, kent.year) == (
        Decimal("3"),
        "Metro - Counties in metro areas of fewer than 250,000 population",
        "rural_urban_continuum_code",
        2023,
    )
    assert rows[("09110", "RUCC_2023")].geo_id == "state:09|county:110"
    assert ("72001", "RUCC_2023") in rows
    # Rose Island has a population of zero and no code: no RUCC row, not a 0.
    assert ("60030", "RUCC_2023") not in rows
    assert rows[("60030", "Population_2020")].value == Decimal("0")
    assert rows[("60030", "Population_2020")].measure is None


def test_typology_flags_and_the_marks_ers_did_not_set() -> None:
    """Covers: ETL-066 — 0 is a real 'not flagged'; 99 and -1 are not set, never 0."""
    rows = _values(_fixture(TYPOLOGY), TYPOLOGY)
    assert rows[("10001", "High_Farming_2025")].value == Decimal("0")
    assert rows[("10001", "High_Farming_2025")].value_status == "valid"
    region = rows[("09110", "High_Farming_2025")]
    assert (region.value, region.value_status, region.missing_reason) == (
        None,
        "not_applicable",
        "not_computed_for_geography",
    )
    legacy = rows[("09001", "Housing_Stress_2025")]
    assert (legacy.value, legacy.missing_reason) == (None, "not_computed_for_geography")
    chugach = rows[("02063", "Persistent_Poverty_1721")]
    assert (chugach.value, chugach.value_status, chugach.missing_reason) == (
        None,
        "not_applicable",
        "not_determined",
    )
    assert rows[("10001", "Industry_Dependence_2025")].measure == "industry_dependence"


def test_atlas_sentinels_each_keep_their_reason() -> None:
    """Covers: ETL-066 — -9999, -8888, N/A and blank are four reasons, none a zero."""
    raw = _atlas_with(
        [
            b"10003,DE,New Castle,SNAPS23,N/A",
            b"10005,DE,Sussex,SNAPS23,",
        ]
    )
    parsed = parse_file(raw, item=ATLAS)
    rows = {(obs.fips_code, obs.attribute): obs for obs in parsed.observations}
    assert rows[("02063", "LACCESS_SNAP15")].missing_reason == "county_did_not_exist"
    assert rows[("02063", "LACCESS_SNAP19")].missing_reason == "not_available"
    # The appended rows repeat counties the fixture already has: a repeat is
    # quarantined, so their own reasons are checked on a file without them.
    assert {q.error_code for q in parsed.quarantined} == {"duplicate_row"}
    alone = _atlas_with(
        [b"10099,DE,Nowhere,SNAPS23,N/A", b"10097,DE,Elsewhere,SNAPS23,"]
    )
    reasons = {
        obs.fips_code: (obs.value, obs.value_status, obs.missing_reason)
        for obs in parse_file(alone, item=ATLAS).observations
        if obs.fips_code in ("10099", "10097")
    }
    assert reasons == {
        "10099": (None, "missing", "incomplete_data"),
        "10097": (None, "missing", "blank"),
    }
    kent = rows[("10001", "SNAPS17")]
    assert (kent.value, kent.measure, kent.year) == (
        Decimal("128.6666667"),
        "snap_authorized_stores",
        2017,
    )
    assert parsed.in_scope_row_count == 42 and parsed.row_count == 1517


def test_malformed_rows_and_files_are_quarantined() -> None:
    """Covers: ETL-066 — bad FIPS, out-of-domain codes and text values; a wrong file is refused."""
    text = _fixture(TYPOLOGY).decode("utf-8")
    damaged = (
        text
        + "N/A,AK,Somewhere,0,High_Farming_2025,0,x,y\r\n"
        + "99001,ZZ,Nowhere,0,High_Farming_2025,0,x,y\r\n"
        + "10099,DE,Nowhere,0,High_Mining_2025,2,x,y\r\n"
        + "10097,DE,Elsewhere,0,Industry_Dependence_2025,7,x,y\r\n"
        + "10095,DE,Other,0,High_Recreation_2025,yes,x,y\r\n"
        + "10093,DE,Short,0,High_Recreation_2025\r\n"
    )
    parsed = parse_file(damaged.encode("utf-8"), item=TYPOLOGY)
    assert sorted(q.error_code for q in parsed.quarantined) == [
        "out_of_domain",
        "out_of_domain",
        "ragged_row",
        "unreadable_fips",
        "unreadable_fips",
        "unreadable_value",
    ]
    rucc = parse_file(
        _fixture(RUCC).replace(b",RUCC_2023,3", b",RUCC_2023,12", 1), item=RUCC
    )
    assert [q.error_code for q in rucc.quarantined] == ["out_of_domain"]
    with pytest.raises(ErsPayloadError, match="unexpected_header"):
        csv_text(b"GEOID,Attribute,Value\r\n", RUCC)
    with pytest.raises(ErsPayloadError, match="member_missing"):
        csv_text(_fixture(RUCC), ATLAS)
    with pytest.raises(ErsPayloadError, match="undecodable"):
        csv_text(
            _fixture(RUCC).replace(b"FIPS", "FÏPS".encode("cp1252"), 1) + b"\xff\xfe",
            TYPOLOGY,
        )
    refused = parse_file(b"<html>moved</html>", item=TYPOLOGY)
    assert [(q.source_row_index, q.error_code) for q in refused.quarantined] == [
        (0, "unexpected_header")
    ]
