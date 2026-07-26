import polars as pl
import pytest

from data_ingestion_toolbox.bls.geography import (
    LAUS_AREA_CODE_LENGTH,
    LAUS_SERIES_ID_LENGTH,
    build_laus_series_id,
    get_laus_area_codes,
    parse_laus_series_id,
)
from data_ingestion_toolbox.bls.ingest import enrich_with_geography, expand_laus_series_ids
from data_ingestion_toolbox.bls.metadata import process_series_data


OFFICIAL_EXAMPLES = [
    (
        "national",
        "000000000000000",
        "LAU00000000000000003",
        {"geo_level": "us", "geo_id": "us:1", "state_fips": None, "county_fips": None},
    ),
    (
        "state",
        "ST0100000000000",
        "LAUST010000000000003",
        {"geo_level": "state", "geo_id": "state:01", "state_fips": "01", "county_fips": None},
    ),
    (
        "county",
        "CN1703100000000",
        "LAUCN170310000000003",
        {
            "geo_level": "county",
            "geo_id": "state:17|county:031",
            "state_fips": "17",
            "county_fips": "031",
        },
    ),
    (
        "metro",
        "MT1716980000000",
        "LAUMT171698000000003",
        {"geo_level": "metro", "geo_id": "metro:16980", "state_fips": "17", "county_fips": None},
    ),
    (
        "city",
        "CT4805000000000",
        "LAUCT480500000000003",
        {
            "geo_level": "city",
            "geo_id": "state:48|city:05000",
            "state_fips": "48",
            "county_fips": None,
        },
    ),
]


@pytest.mark.parametrize(("name", "area_code", "series_id", "geography"), OFFICIAL_EXAMPLES)
def test_build_and_parse_official_laus_examples(name, area_code, series_id, geography):
    assert len(area_code) == LAUS_AREA_CODE_LENGTH, name
    assert len(series_id) == LAUS_SERIES_ID_LENGTH, name
    assert build_laus_series_id(area_code, "03", "U") == series_id

    parsed = parse_laus_series_id(series_id)
    assert parsed["program"] == "LA"
    assert parsed["seasonal"] == "U"
    assert parsed["area_code"] == area_code
    assert parsed["measure_code"] == "03"
    for key, expected in geography.items():
        assert parsed[key] == expected


@pytest.mark.parametrize(
    "area_code",
    [
        "CN170310000",       # legacy 10-character code
        "CT480500000000",    # 14 characters
        "ct4805000000000",   # lower case is not canonical
        "CT48050000000000",  # 16 characters
    ],
)
def test_generator_rejects_noncanonical_area_codes(area_code):
    with pytest.raises(ValueError, match="15 uppercase alphanumeric"):
        build_laus_series_id(area_code, "03")


def test_expansion_uses_canonical_generator(monkeypatch):
    monkeypatch.setattr(
        "data_ingestion_toolbox.bls.geography.get_laus_area_codes",
        lambda geo_level, state_fips=None: ["CN1703100000000"],
    )
    assert expand_laus_series_ids(["03", "06"], "county", "17") == [
        "LAUCN170310000000003",
        "LAUCN170310000000006",
    ]


def test_city_lookup_uses_official_ct_prefix(monkeypatch):
    seen = []
    monkeypatch.setattr(
        "data_ingestion_toolbox.bls.geography._get_area_codes_from_db",
        lambda prefix: seen.append(prefix) or [],
    )
    assert get_laus_area_codes("city") == []
    assert seen == ["CT"]


def test_raw_enrichment_uses_canonical_parser():
    df = pl.DataFrame({"series_id": [example[2] for example in OFFICIAL_EXAMPLES]})
    enriched = enrich_with_geography(df, "la")
    assert enriched.get_column("geo_level").to_list() == [
        "us",
        "state",
        "county",
        "metro",
        "city",
    ]
    assert enriched.get_column("county_fips").to_list() == [None, None, "031", None, None]


def test_laus_metadata_normalizes_official_fields_for_all_geographies():
    df = pl.DataFrame(
        {
            "series_id": [f" {example[2]} " for example in OFFICIAL_EXAMPLES],
            "area_code": [f" {example[1]} " for example in OFFICIAL_EXAMPLES],
            "measure_code": ["03"] * len(OFFICIAL_EXAMPLES),
            "seasonal": ["U"] * len(OFFICIAL_EXAMPLES),
        }
    )
    normalized = process_series_data(df, "la")
    assert [record["series_id"] for record in normalized] == [
        example[2] for example in OFFICIAL_EXAMPLES
    ]
    assert [record["area_code"] for record in normalized] == [
        example[1] for example in OFFICIAL_EXAMPLES
    ]


def test_laus_metadata_rejects_inconsistent_series_id():
    df = pl.DataFrame(
        {
            "series_id": ["LAUCN170310000000003"],
            "area_code": ["CN1703100000000"],
            "measure_code": ["06"],
            "seasonal": ["U"],
        }
    )
    with pytest.raises(ValueError, match="fields rebuild"):
        process_series_data(df, "la")
