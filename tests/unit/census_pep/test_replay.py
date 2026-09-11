"""Offline replay contracts for captured Census PEP bulk CSV bytes."""

from __future__ import annotations

from pathlib import Path

import pytest

from data_ingestion_toolbox.census_pep.config import CONFIG, PEPRelease
from data_ingestion_toolbox.census_pep.silver_pep.replay import (
    PepCapturePayloadError,
    parse_captured_pep_values,
    validate_release_completeness,
)

pytestmark = pytest.mark.unit

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "census_pep"


def _release(dataset_code: str, vintage_year: int) -> PEPRelease:
    return next(
        release
        for release in CONFIG.releases
        if release.dataset_code == dataset_code and release.vintage_year == vintage_year
    )


def _fixture(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


def test_current_and_prior_replay_keep_distinct_revision_keys() -> None:
    """Covers: ETL-004 — PEP replay separates vintage and observation year."""
    current = parse_captured_pep_values(
        _fixture("nst_2025.csv"),
        release=_release("pep_nst_alldata", 2025),
    )
    prior = parse_captured_pep_values(
        _fixture("nst_2024.csv"),
        release=_release("pep_nst_alldata", 2024),
    )

    current_2024 = next(
        row
        for row in current
        if row["metric_code"] == "POPESTIMATE" and row["observation_year"] == 2024
    )
    prior_2024 = next(
        row
        for row in prior
        if row["metric_code"] == "POPESTIMATE" and row["observation_year"] == 2024
    )

    assert current_2024["release_vintage"] == 2025
    assert prior_2024["release_vintage"] == 2024
    assert current_2024["value_source"] == "340003797"
    assert prior_2024["value_source"] == "340110988"
    assert current_2024["value"] != prior_2024["value"]
    assert current_2024["unit"] == "persons"


def test_subcounty_replay_retains_authoritative_place_codes() -> None:
    """Covers: ETL-004 — PEP replay retains incorporated-place identity."""
    rows = parse_captured_pep_values(
        _fixture("subcounty_2025.csv"),
        release=_release("pep_subcounty", 2025),
    )
    estimate = next(
        row
        for row in rows
        if row["metric_code"] == "POPESTIMATE" and row["observation_year"] == 2025
    )

    assert estimate["summary_level"] == "162"
    assert estimate["state_fips_source"] == "01"
    assert estimate["place_fips_source"] == "00124"
    assert estimate["name_source"] == "Abbeville city"
    assert estimate["value_source"] == "2378"


def test_rate_metrics_have_explicit_rate_unit() -> None:
    """Covers: ETL-004 — PEP rate fields cannot masquerade as counts."""
    rows = parse_captured_pep_values(
        _fixture("nst_2025.csv"),
        release=_release("pep_nst_alldata", 2025),
    )
    rate = next(
        row
        for row in rows
        if row["metric_code"] == "RNETMIG" and row["observation_year"] == 2025
    )

    assert rate["unit"] == "per_1000_population"
    assert str(rate["value"]) == "3.7026195511"


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"", "empty"),
        (b"SUMLEV,SUMLEV,POPESTIMATE2025\n010,010,1\n", "duplicate"),
        (b"SUMLEV,NAME,POPESTIMATE2025\n010,United States,1,extra\n", "row length"),
        (
            b"SUMLEV,REGION,DIVISION,STATE,NAME\n010,0,0,00,United States\n",
            "metric column",
        ),
    ],
)
def test_malformed_bulk_csv_is_rejected(payload: bytes, message: str) -> None:
    """Covers: ETL-005 — Malformed PEP CSV fails deterministically."""
    with pytest.raises(PepCapturePayloadError, match=message):
        parse_captured_pep_values(
            payload,
            release=_release("pep_nst_alldata", 2025),
        )


def test_sentinel_and_invalid_values_are_not_coerced_to_zero() -> None:
    """Covers: ETL-006 — PEP missing and invalid values retain status."""
    payload = (
        b"SUMLEV,REGION,DIVISION,STATE,NAME,POPESTIMATE2025,BIRTHS2025\n"
        b"010,0,0,00,United States,-999999999,not-a-number\n"
    )

    rows = parse_captured_pep_values(
        payload,
        release=_release("pep_nst_alldata", 2025),
    )
    by_metric = {row["metric_code"]: row for row in rows}

    assert by_metric["POPESTIMATE"]["value"] is None
    assert by_metric["POPESTIMATE"]["value_status"] == "sentinel"
    assert by_metric["BIRTHS"]["value"] is None
    assert by_metric["BIRTHS"]["value_status"] == "invalid"


def test_unregistered_summary_level_is_schema_drift() -> None:
    """Covers: ETL-005 — new source geography layouts cannot load silently."""
    payload = (
        b"SUMLEV,REGION,DIVISION,STATE,NAME,POPESTIMATE2025\n999,0,0,00,Unknown,1\n"
    )
    with pytest.raises(PepCapturePayloadError, match="unregistered summary level"):
        parse_captured_pep_values(
            payload,
            release=_release("pep_nst_alldata", 2025),
        )


def test_production_completeness_rejects_partial_fixture() -> None:
    """Covers: ETL-019 — a missing state slice cannot publish as complete."""
    release = _release("pep_nst_alldata", 2025)
    values = parse_captured_pep_values(_fixture("nst_2025.csv"), release=release)

    with pytest.raises(PepCapturePayloadError, match="release is incomplete"):
        validate_release_completeness(values, release=release)


# ---------------------------------------------------------------------------
# ETL-045 — one parser, every decade's spelling
# ---------------------------------------------------------------------------

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "census_pep"


def _release(dataset_code: str, vintage_year: int) -> PEPRelease:
    return next(
        item
        for item in CONFIG.releases
        if item.dataset_code == dataset_code and item.vintage_year == vintage_year
    )


def _parsed(dataset_code: str, vintage_year: int, fixture: str) -> list[dict]:
    return parse_captured_pep_values(
        (FIXTURES / fixture).read_bytes(),
        release=_release(dataset_code, vintage_year),
    )


def test_closed_decade_component_spelling_maps_onto_one_measure() -> None:
    """Covers: ETL-045 — NATURALINC and NATURALCHG are the same measure.

    The Bureau renamed the family when it opened the 2020s series. Reading
    the older spelling as its own metric would split one county's natural
    change into two series that meet at 2020 and neither of which is whole.
    """
    rows = _parsed("pep_county_alldata_2010s", 2020, "co_2010s.csv")
    natural = [row for row in rows if row["metric_code"] == "NATURALCHG"]

    assert natural, "the 2010s file publishes natural change"
    assert {row["source_header"][: len("NATURALINC")] for row in natural} == {
        "NATURALINC"
    }
    # Nothing is published under the source spelling.
    assert not [row for row in rows if row["metric_code"] == "NATURALINC"]


def test_decennial_count_is_read_as_its_own_measure() -> None:
    """Covers: ETL-045 — the census column carries its year inside its name."""
    rows = _parsed("pep_county_alldata_2010s", 2020, "co_2010s.csv")
    counts = [row for row in rows if row["metric_code"] == "CENSUSPOP"]

    assert {row["source_header"] for row in counts} == {"CENSUS2010POP"}
    assert {row["observation_year"] for row in counts} == {2010}
    # It is not folded into the July estimate for the same year.
    july = [
        row
        for row in rows
        if row["metric_code"] == "POPESTIMATE" and row["observation_year"] == 2010
    ]
    assert july and july[0]["value_source"] != counts[0]["value_source"]


def test_current_decade_files_publish_no_census_count() -> None:
    """Covers: ETL-045 — a family a file does not carry yields no rows."""
    rows = _parsed("pep_nst_alldata", 2025, "nst_2025.csv")
    assert not [row for row in rows if row["metric_code"] == "CENSUSPOP"]


def test_not_applicable_is_recorded_as_a_sentinel_not_a_parse_failure() -> None:
    """Covers: ETL-045 — 'X' is a published non-value.

    The Bureau marks the decennial count of a geography that did not exist at
    that census with X. Reading it as an unparseable value would report a
    defect in this pipeline for something the source states deliberately.
    """
    header = "SUMLEV,STATE,COUNTY,STNAME,CTYNAME,CENSUS2010POP,POPESTIMATE2010"
    row = "050,08,014,Colorado,Broomfield County,X,58298"
    payload = (header + "\n" + row + "\n").encode("cp1252")

    parsed = parse_captured_pep_values(
        payload, release=_release("pep_county_alldata_2010s", 2020)
    )
    by_metric = {item["metric_code"]: item for item in parsed}

    assert by_metric["CENSUSPOP"]["value_status"] == "sentinel"
    assert by_metric["CENSUSPOP"]["value"] is None
    # The exact source text is retained either way.
    assert by_metric["CENSUSPOP"]["value_source"] == "X"
    assert by_metric["POPESTIMATE"]["value_status"] == "valid"


def test_metric_column_outside_the_release_range_is_refused() -> None:
    """Covers: ETL-045 — a file must cover the decade its release declares."""
    header = "SUMLEV,STATE,COUNTY,STNAME,CTYNAME,POPESTIMATE2024"
    row = "050,01,001,Alabama,Autauga County,60000"
    payload = (header + "\n" + row + "\n").encode("cp1252")

    with pytest.raises(PepCapturePayloadError, match="outside release range"):
        parse_captured_pep_values(
            payload, release=_release("pep_county_alldata_2010s", 2020)
        )


def test_completeness_follows_each_product_own_principal_grain() -> None:
    """Covers: ETL-045 — completeness is declared by the registry.

    The thresholds named the three 2020s products, so a newly registered one
    could never be complete and its rows would never reach the gold views.
    """
    rows = _parsed("pep_county_alldata_2010s", 2020, "co_2010s.csv")

    # A two-row fixture is a real file's shape but not a complete release.
    with pytest.raises(PepCapturePayloadError, match="incomplete"):
        validate_release_completeness(
            rows, release=_release("pep_county_alldata_2010s", 2020)
        )
