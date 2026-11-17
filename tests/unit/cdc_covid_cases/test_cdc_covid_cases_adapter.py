"""CDC COVID-19 case count adapter contracts: requests, index and slice parsing.

Covers: ETL-135
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from data_ingestion_toolbox.cdc_covid_cases.capture import (
    CdcCovidSliceError,
    index_slices,
)
from data_ingestion_toolbox.cdc_covid_cases.client import (
    CdcCovidPayloadError,
    index_parameters,
    slice_parameters,
)
from data_ingestion_toolbox.cdc_covid_cases.registry import AGE_KEYS, age_key
from data_ingestion_toolbox.cdc_covid_cases.silver_cdc_covid_cases.parse import (
    parse_slice,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "cdc_covid_cases"


def test_requests_group_by_the_registered_columns_and_never_ask_for_race() -> None:
    """Covers: ETL-135 -- one state-month, grouped; race and ethnicity never requested."""
    params = slice_parameters(res_state="WI", case_month="2021-11", limit=50_000)
    assert params == {
        "$select": "county_fips_code, age_group, hosp_yn, death_yn, count(*) AS n",
        "$where": "res_state='WI' AND case_month='2021-11'",
        "$group": "county_fips_code, age_group, hosp_yn, death_yn",
        "$order": "county_fips_code, age_group, hosp_yn, death_yn",
        "$limit": 50_000,
    }
    index = index_parameters(limit=50_000)
    for text in (*params.values(), *index.values()):
        assert "race" not in str(text) and "ethnicity" not in str(text)
    assert slice_parameters(res_state=None, case_month="O'2", limit=1)["$where"] == (
        "res_state IS NULL AND case_month='O''2'"
    )


def test_the_index_names_each_slice_once_and_refuses_a_full_page() -> None:
    """Covers: ETL-135 -- the index is read whole or refused."""
    raw = (FIXTURES / "n8mc_index.json").read_bytes()
    assert index_slices(raw, group_limit=50_000) == [
        ("CA", "2022-01", 2_808_221),
        ("WI", "2021-11", 82_251),
    ]
    with pytest.raises(CdcCovidSliceError, match="index_filled_limit"):
        index_slices(raw, group_limit=2)
    repeated = json.dumps([{"res_state": "WI", "case_month": "2021-11", "n": "1"}] * 2)
    with pytest.raises(CdcCovidPayloadError):
        index_slices(repeated.encode(), group_limit=50_000)


def test_a_captured_slice_parses_every_group_with_withheld_counties_kept() -> None:
    """Covers: ETL-135 -- every Wisconsin 2021-11 group is read; NA counties stay NA."""
    parsed = parse_slice(
        (FIXTURES / "n8mc_wi_2021-11.json").read_bytes(),
        res_state="WI",
        case_month="2021-11",
    )
    assert parsed.quarantined == ()
    assert parsed.case_month == date(2021, 11, 1)
    assert parsed.state_fips == "55"
    assert len(parsed.groups) == parsed.group_count == 1506
    assert sum(group.record_count for group in parsed.groups) == 82_251
    withheld = [group for group in parsed.groups if group.county_fips_code == "NA"]
    assert sum(group.record_count for group in withheld) == 6_163


def test_an_unreadable_group_is_set_aside_alone_with_its_records() -> None:
    """Covers: ETL-135 -- a group that cannot be read is quarantined, never served."""
    payload = json.dumps(
        [
            {"county_fips_code": "55025", "age_group": "65+ years", "n": "5"},
            {"county_fips_code": "5502", "age_group": "65+ years", "n": "2"},
            {"county_fips_code": "55025", "age_group": "80+ years", "n": "3"},
            {"county_fips_code": "55025", "age_group": "NA", "n": "x"},
            {"age_group": "Missing", "hosp_yn": "Yes", "n": "4"},
        ]
    ).encode()
    parsed = parse_slice(payload, res_state="WI", case_month="2021-11")
    assert [(g.county_fips_code, g.age_key, g.record_count) for g in parsed.groups] == [
        ("55025", "age_65_plus", 5),
        (None, "age_unknown", 4),
    ]
    assert [
        (q.source_row_index, q.error_code, q.record_count) for q in parsed.quarantined
    ] == [
        (2, "unreadable_county_code", 2),
        (3, "unregistered_age_group", 3),
        (4, "unreadable_count", None),
    ]
    assert (
        parse_slice(b"not json", res_state="WI", case_month="2021-11")
        .quarantined[0]
        .error_code
        == "invalid_json"
    )
    assert parse_slice(b"[]", res_state="WI", case_month="2021-13").case_month is None


def test_every_registered_age_group_has_one_key() -> None:
    """Covers: ETL-135 -- CDC's age texts map to six served keys."""
    assert AGE_KEYS == (
        "all_ages",
        "age_0_17",
        "age_18_49",
        "age_50_64",
        "age_65_plus",
        "age_unknown",
    )
    assert age_key("0 - 17 years") == "age_0_17"
    assert age_key(None) == "age_unknown"
    assert age_key("Missing") == "age_unknown"
    assert age_key("80+ years") is None
