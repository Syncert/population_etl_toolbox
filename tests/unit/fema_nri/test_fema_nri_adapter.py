"""Offline contracts for the FEMA National Risk Index and declarations adapter.

Covers: ETL-067
"""

from __future__ import annotations

import importlib
import json
import sys
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from data_ingestion_toolbox.fema_nri.client import (
    FemaPayloadError,
    fetch_page,
    read_page,
)
from data_ingestion_toolbox.fema_nri.config import FemaConfig
from data_ingestion_toolbox.fema_nri.registry import (
    DECLARATIONS,
    NRI,
    NRI_FIELDS,
    nri_out_fields,
)
from data_ingestion_toolbox.fema_nri.silver_fema_nri.parse import (
    parse_declarations,
    parse_nri,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "fema_nri"


def _records(stream: str) -> tuple:
    name = "nri_counties.json" if stream == NRI else "declarations.json"
    records, _more = read_page(
        stream, (FIXTURES / name).read_bytes(), "fixture", page_size=10**6
    )
    return records


def test_configuration_imports_without_io_and_holds_no_credential() -> None:
    """Covers: ETL-067 — neither FEMA service needs a key, so there is none to leak."""
    for name in list(sys.modules):
        if name.startswith("data_ingestion_toolbox.fema_nri"):
            del sys.modules[name]
    module = importlib.import_module("data_ingestion_toolbox.fema_nri.config")
    assert not {
        field
        for field in module.FemaConfig.model_fields
        if "key" in field or "token" in field
    }
    assert "not endorsed by FEMA" in module.FEMA_NOTICE
    assert len(NRI_FIELDS) == 1 + 18 + 5
    assert "RISK_SCORE" not in nri_out_fields() and "SOVI_SCORE" not in nri_out_fields()


def test_nri_values_keep_their_rating_status() -> None:
    """Covers: ETL-067 — a Not Applicable hazard carries no number, never zero."""
    observations, quarantined = parse_nri(_records(NRI))
    assert quarantined == []
    rows = {(obs.stcofips, obs.field): obs for obs in observations}
    kent = rows[("10001", "EAL_VALT")]
    assert (
        kent.value_status == "valid"
        and kent.value > 0
        and kent.nri_version == "December 2025"
    )
    adjuntas = rows[("72001", "TSUN_EALT")]
    assert (
        adjuntas.value,
        adjuntas.value_status,
        adjuntas.missing_reason,
        adjuntas.rating,
    ) == (
        None,
        "not_applicable",
        "hazard_not_applicable",
        "Not Applicable",
    )
    assert rows[("09110", "EAL_VALT")].geo_id == "state:09|county:110"
    assert all(obs.value is None for obs in observations if obs.value_status != "valid")


def test_nri_quarantines_bad_rows_and_counts_a_repeat_once() -> None:
    """Covers: ETL-067 — a bad FIPS, a text value or a repeated county is quarantined."""
    records = list(_records(NRI))
    bad_code = dict(records[0], STCOFIPS="99001")
    text_value = dict(records[1], STCOFIPS="10099", EAL_VALT="lots")
    observations, quarantined = parse_nri((*records, records[0], bad_code, text_value))
    assert sorted(q.error_code for q in quarantined) == [
        "duplicate_row",
        "unreadable_fips",
        "unreadable_value",
    ]
    assert len(observations) == len(records) * len(NRI_FIELDS)
    blank = dict(records[0], STCOFIPS="10097", EAL_VALT=None, EAL_RATNG="Very Low")
    (row,) = [obs for obs in parse_nri((blank,))[0] if obs.field == "EAL_VALT"]
    assert (row.value, row.value_status, row.missing_reason) == (
        None,
        "missing",
        "blank",
    )


def test_declarations_keep_areas_apart_from_counties() -> None:
    """Covers: ETL-067 — a 000 county code is an area, never a county; counties are FIPS."""
    rows, quarantined = parse_declarations(_records(DECLARATIONS))
    assert quarantined == []
    areas = [row for row in rows if row.county_fips == "000"]
    assert areas and all(row.geo_id is None for row in areas)
    assert any(row.designated_area.startswith("Mashantucket Pequot") for row in areas)
    legacy = [row for row in rows if row.geo_id == "state:09|county:009"]
    assert legacy, "Connecticut declarations still name legacy counties"
    assert {row.declaration_type for row in rows} == {"DR", "EM", "FM"}
    bad = dict(_records(DECLARATIONS)[0], declarationType="XX", id="x")
    no_hash = dict(_records(DECLARATIONS)[0], hash="", id="y")
    _rows, rejected = parse_declarations((bad, no_hash))
    assert sorted(q.error_code for q in rejected) == [
        "unknown_declaration_type",
        "unreadable_identity",
    ]


def test_pages_follow_the_service_until_it_says_stop() -> None:
    """Covers: ETL-067 — offsets advance by the page size; an error body is refused."""
    records = _records(DECLARATIONS)
    seen: list[dict[str, list[str]]] = []

    class Client:
        def get(
            self, url: str, *, params: dict[str, str], headers: dict[str, str]
        ) -> httpx.Response:
            seen.append(params)
            skip, top = int(params["$skip"]), int(params["$top"])
            body = {"DisasterDeclarationsSummaries": list(records[skip : skip + top])}
            return httpx.Response(
                200,
                content=json.dumps(body).encode(),
                request=httpx.Request("GET", url),
            )

    config = FemaConfig(declaration_page_size=30)
    first = fetch_page(DECLARATIONS, 0, config=config, client=Client())
    last = fetch_page(DECLARATIONS, 2, config=config, client=Client())
    assert (len(first.records), first.more) == (30, True)
    assert (len(last.records), last.more) == (len(records) - 60, False)
    assert [params["$skip"] for params in seen] == ["0", "60"]
    nri = fetch_page(NRI, 1, config=FemaConfig(nri_page_size=5), client=_Echo())
    assert (
        parse_qs(urlparse(nri.endpoint).query) == {}
        and nri.parameters["resultOffset"] == "5"
    )
    with pytest.raises(FemaPayloadError, match="service_error"):
        read_page(NRI, b'{"error": {"code": 400}}', "nri:page:0", page_size=5)
    with pytest.raises(FemaPayloadError, match="not_json"):
        read_page(DECLARATIONS, b"<html>", "declarations:page:0", page_size=5)


class _Echo:
    def get(
        self, url: str, *, params: dict[str, str], headers: dict[str, str]
    ) -> httpx.Response:
        body = {
            "features": [{"attributes": {"STCOFIPS": "10001"}}],
            "exceededTransferLimit": False,
        }
        return httpx.Response(
            200, content=json.dumps(body).encode(), request=httpx.Request("GET", url)
        )


def test_a_value_is_the_service_number_as_written() -> None:
    """Covers: ETL-067 — the JSON number is kept verbatim and read exactly."""
    record = dict(_records(NRI)[0], STCOFIPS="10095", EAL_VALT=1234.5)
    (row,) = [obs for obs in parse_nri((record,))[0] if obs.field == "EAL_VALT"]
    assert (row.value_source, row.value) == ("1234.5", Decimal("1234.5"))
