"""Static guards on the county crime rate computed the FBI's way (ADR-0012).

Covers: ETL-106 — a county rate divides the reported offenses of the agencies
serving only that county by those agencies' own FBI populations, publishes
its coverage, and refuses rather than reads thin coverage as low crime. The
warehouse tier proves the arithmetic on the reviewed Wisconsin fixture in
``tests/integration/database/test_fbi_ucr_pipeline.py``.
"""

from __future__ import annotations

import re
import textwrap
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
GOLD_DDL = (
    REPOSITORY_ROOT / "src/data_ingestion_toolbox/fbi_ucr/gold_fbi/DDL/gold_fbi.sql"
)


def _body(name: str) -> str:
    sql = GOLD_DDL.read_text(encoding="utf-8")
    match = re.search(
        rf"CREATE OR REPLACE VIEW gold_fbi\.{name} AS(?P<body>.*?);\s*\n",
        sql,
        re.DOTALL,
    )
    assert match is not None, f"gold_fbi.{name} is not defined"
    body = match.group("body")
    if name == "county_agency_rate":
        # The served view reads rows computed at publication; the arithmetic
        # lives in the function that computes them, and both are the rate.
        return _rate_rows_body() + body
    return body


def _rate_rows_body() -> str:
    sql = GOLD_DDL.read_text(encoding="utf-8")
    match = re.search(
        r"FUNCTION gold_fbi\.county_agency_rate_rows\(.*?\$rate\$\n(?P<body>.*?)\$rate\$",
        sql,
        re.DOTALL,
    )
    assert match is not None, "gold_fbi.county_agency_rate_rows is not defined"
    return textwrap.dedent(match.group("body"))


def test_the_rate_is_computed_at_publication_not_per_read() -> None:
    """Covers: ETL-106 — a county read is an index lookup, never a national sum.

    Computed on read, the rate summed every agency-month in the country
    before a county filter applied: with every Return A agency loaded, one
    county card took over two minutes.
    """
    sql = GOLD_DDL.read_text(encoding="utf-8")
    view = re.search(
        r"CREATE OR REPLACE VIEW gold_fbi\.county_agency_rate AS(?P<body>.*?);\s*\n",
        sql,
        re.DOTALL,
    ).group("body")
    assert "FROM gold_fbi.county_agency_rate_month AS served" in view
    assert "GROUP BY" not in view and "SUM(" not in view
    rows = _rate_rows_body()
    assert "WHERE observation.product_id = p_product_id" in rows
    for index in (
        "fbi_county_agency_rate_geo_idx\n"
        "    ON gold_fbi.county_agency_rate_month (\n"
        "        geo_id, product_id, measure_id, period_start DESC",
        "fbi_county_agency_rate_state_idx\n"
        "    ON gold_fbi.county_agency_rate_month (\n"
        "        state_fips, product_id, measure_id, period_start DESC",
    ):
        assert index in sql
    # Both ways a release is published recompute that product's rows in the
    # publishing transaction.
    package = GOLD_DDL.parents[2]
    for path in (package / "gold_fbi/publisher.py", package / "return_a/load.py"):
        assert "refresh_county_agency_rate(cursor," in path.read_text(encoding="utf-8")


def test_the_rate_reads_only_fbi_figures() -> None:
    """Covers: ETL-106 — no Census population enters the rate (ADR-0009 rule 2)."""
    body = _body("county_agency_rate")

    assert "FROM gold_fbi.crime_observation AS observation" in body
    assert "observation.population" in body
    for census in ("silver_pep", "gold_census", "gold_acs", "population_denominator"):
        assert census not in body
    # Names only: the county's identity, never its Census population.
    assert "silver_ref.dim_geo_entity AS county" in body


def test_the_rate_divides_offenses_by_the_same_agencies_population() -> None:
    """Covers: ETL-106 — both sides filter on one agency set."""
    body = _body("county_agency_rate")
    contributing = (
        "WHERE classified.is_single_county AND classified.is_reported\n"
        "                 AND classified.has_population"
    )
    assert f"SUM(classified.value) FILTER (\n               {contributing}" in body
    assert f"SUM(classified.population) FILTER (\n               {contributing}" in body
    assert "assessed.offenses * 100000 / assessed.covered_population" in body
    assert "observation.measure_form = 'absolute_total'" in body
    assert "observation.counted_entity_basis = 'offense'" in body


def test_multi_county_agencies_are_left_out_and_listed() -> None:
    """Covers: ETL-106 — no split of an agency's count is invented."""
    body = _body("county_agency_rate")

    assert re.search(r"COUNT\(DISTINCT sibling\.geo_id\) > 1", body)
    assert "NOT agency_month.is_multi_county AS is_single_county" in body
    assert "AS excluded_multi_county_oris" in body


def test_coverage_is_published_and_thin_coverage_is_refused() -> None:
    """Covers: ETL-106 — refusal reasons, never a zero, under the 50 % floor."""
    body = _body("county_agency_rate")

    for column in (
        "AS covered_population",
        "AS linked_population",
        "AS coverage_share",
        "AS contributing_oris",
        "AS non_reporting_oris",
        "AS unknown_population_oris",
        "0.5::NUMERIC AS coverage_floor",
    ):
        assert column in body
    for reason in (
        "'no_reporting_agency'",
        "'only_multi_county_agencies'",
        "'coverage_below_floor'",
    ):
        assert reason in body
    assert "summed.covered_population < 0.5 * summed.linked_population" in body
    assert "CASE WHEN assessed.unavailable_reason IS NULL" in body
    assert "COALESCE(assessed.offenses" not in body
    assert "COALESCE(classified.value" not in body


def test_the_rate_declares_itself_derived_and_monthly() -> None:
    """Covers: ETL-106 — never shown as an FBI-published rate, never annualised."""
    body = _body("county_agency_rate")

    assert "TRUE AS derived" in body
    assert "'agency_reported_offenses_per_covered_population'::TEXT" in body
    assert (
        "'per 100,000 people covered by reporting agencies in this county'::TEXT"
        in body
    )
    assert "date_trunc" not in body.lower()
    assert "assessed.period" in body
    latest = _body("latest_county_agency_rate")
    assert "FROM gold_fbi.county_agency_rate AS rate" in latest
    assert "MAX(refresh_date)" in latest
