"""Offline contracts for the Census government finance adapter.

Covers: ETL-118
"""

from __future__ import annotations

import importlib
import io
import json
import sys
import zipfile
from datetime import date
from decimal import Decimal

import httpx
import pytest

from data_ingestion_toolbox.census_govfin import plan
from data_ingestion_toolbox.census_govfin.client import (
    GovFinFetchError,
    GovFinPayloadError,
    fetch_api_slice,
    fetch_archive,
)
from data_ingestion_toolbox.census_govfin.config import GovFinConfig
from data_ingestion_toolbox.census_govfin.registry import (
    CENSUS_EDITIONS,
    CENSUS_STATE_TO_FIPS,
    GOVS_VARIABLES,
    MEASURES,
    SPENDING_LINES,
    SPENDING_MIX_LINES,
    SPENDING_TOTAL,
    UNIT_LAYOUT_2002,
    USPS_TO_FIPS,
    get_edition,
    govs_measure,
    measures_for_item,
    registered_periods,
    slice_keys,
)
from data_ingestion_toolbox.census_govfin.silver_census_govfin.parse import (
    decimal_or_none,
    parse_archive,
    parse_directory_lines,
    parse_state_total_lines,
    parse_unit_lines,
)
from tests.support import census_govfin

pytestmark = pytest.mark.unit

KEY = "unit-test-census-key-0123456789"


class _ScriptedClient:
    def __init__(self, outcomes: list[httpx.Response | BaseException]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, dict[str, str] | None]] = []

    def get(self, url: str, *, params=None, headers=None) -> httpx.Response:
        self.calls.append((url, dict(params) if params else None))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def close(self) -> None:
        return None


def _config(**overrides: object) -> GovFinConfig:
    values: dict[str, object] = {
        "census_api_key": KEY,
        "min_spacing_seconds": 0,
        "max_attempts": 2,
    }
    values.update(overrides)
    return GovFinConfig(**values)


def test_configuration_and_registry_import_without_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-118 -- importing reads no key, file or network."""
    monkeypatch.delenv("CENSUS_API_KEY", raising=False)
    for name in list(sys.modules):
        if name.startswith("data_ingestion_toolbox.census_govfin"):
            monkeypatch.delitem(sys.modules, name)
    module = importlib.import_module("data_ingestion_toolbox.census_govfin.config")
    importlib.import_module("data_ingestion_toolbox.census_govfin.registry")
    with pytest.raises(ValueError, match="CENSUS_API_KEY is required"):
        module.GovFinConfig.from_environment().require_api_key()


def test_census_state_codes_are_alphabetical_not_fips() -> None:
    """Covers: ETL-118 -- the government code's state is Census alphabetical (Arizona 03, FIPS 04)."""
    assert len(CENSUS_STATE_TO_FIPS) == 51
    assert CENSUS_STATE_TO_FIPS["03"] == "04"
    assert CENSUS_STATE_TO_FIPS["09"] == "11"
    assert CENSUS_STATE_TO_FIPS["50"] == "55"
    assert sorted(CENSUS_STATE_TO_FIPS.values()) == sorted(USPS_TO_FIPS.values())


@pytest.mark.parametrize(
    ("item", "expected"),
    [
        ("T01", {"taxes_total", "tax_property", "own_source_general_revenue"}),
        ("T13", {"taxes_total", "tax_selective_sales", "own_source_general_revenue"}),
        ("T53", {"taxes_total", "tax_other", "own_source_general_revenue"}),
        ("A61", {"current_charges", "own_source_general_revenue"}),
        ("A91", set()),
        ("U20", {"misc_general_revenue", "own_source_general_revenue"}),
        ("B46", {"intergovernmental_federal", "intergovernmental_total"}),
        ("C21", {"intergovernmental_state", "intergovernmental_total"}),
        ("D21", {"intergovernmental_local", "intergovernmental_total"}),
        ("44T", {"debt_outstanding"}),
        ("49U", {"debt_outstanding"}),
        ("44X", set()),
        ("41A", set()),
        ("64V", {"debt_outstanding"}),
        ("19T", set()),
        ("E44", {"spend_highways", "spend_general_total"}),
        (
            "E12",
            {
                "spend_elementary_secondary_education",
                "spend_education",
                "spend_general_total",
            },
        ),
        ("E92", {"spend_utility_electric", "spend_utility_total"}),
        ("I89", {"spend_interest_general_debt", "spend_general_total"}),
        ("M89", set()),
        ("X40", {"spend_insurance_trust"}),
    ],
)
def test_item_codes_count_toward_their_measures(item: str, expected: set[str]) -> None:
    """Covers: ETL-118 -- each Census item code reaches exactly the measures it belongs to."""
    assert set(measures_for_item(item)) == expected


def test_tax_types_partition_total_taxes() -> None:
    """Covers: ETL-118 -- every tax item is in the total and in exactly one tax type."""
    types = {
        "tax_property", "tax_general_sales", "tax_selective_sales",
        "tax_individual_income", "tax_corporate_income", "tax_other",
    }  # fmt: skip
    for number in range(100):
        item = f"T{number:02d}"
        found = set(measures_for_item(item))
        assert "taxes_total" in found
        assert len(found & types) == 1, item
    assert {measure.measure_id for measure in MEASURES} >= types


def test_spending_lines_partition_direct_general_spending() -> None:
    """Covers: ETL-118 -- every item of Census line 66 is in exactly one spending-mix line."""
    codes = {line[0]: set(line[5]) for line in SPENDING_LINES}
    general = codes[SPENDING_TOTAL]
    seen: list[str] = [
        code for measure in SPENDING_MIX_LINES for code in codes[measure]
    ]
    assert len(seen) == len(set(seen))
    assert set(seen) == general
    assert codes["spend_education"] == (
        codes["spend_higher_education"]
        | codes["spend_elementary_secondary_education"]
        | codes["spend_other_education"]
    )
    # Payments to other governments (M, Q, S items) are never direct spending.
    every = set().union(*codes.values())
    assert not any(code[0] in "MQS" for code in every)
    # Each line names the govslocalfin aggregate Census labels the same way.
    assert len({line[2] for line in SPENDING_LINES}) == len(SPENDING_LINES)
    assert all(govs_measure(line[2]) == line[0] for line in SPENDING_LINES)


def test_registered_periods_and_slices() -> None:
    """Covers: ETL-118 -- editions declare their zips, the API its grains, QTAX runs to this year."""
    assert registered_periods("cog_units") == tuple(
        e.census_year for e in CENSUS_EDITIONS
    )
    assert slice_keys("cog_units", 2002) == ("units", "directory")
    assert slice_keys("asslgf", 2020) == ("us", "state")
    assert registered_periods("qtax", date(2027, 3, 1))[-1] == 2027
    assert registered_periods("qtax", date(2027, 3, 1))[0] == 1994


def test_first_run_plans_everything_then_only_the_window() -> None:
    """Covers: ETL-118 -- the loader rule: unanswered periods are planned without a manual run."""
    today = date(2026, 11, 5)
    years = list(registered_periods("qtax", today))
    assert plan.periods_to_capture("qtax", {}, set(), today=today) == years
    assert plan.periods_to_capture("qtax", None, set(years), today=today) == [
        2025,
        2026,
    ]
    gap = set(years) - {2001, 2002}
    assert plan.periods_to_capture("qtax", None, gap, today=today) == [
        2001,
        2002,
        2025,
        2026,
    ]
    editions = list(registered_periods("cog_units"))
    assert plan.periods_to_capture("cog_units", None, set(editions)) == []
    assert (
        plan.periods_to_capture("cog_units", {"history": True}, set(editions))
        == editions
    )


def test_answered_needs_every_declared_slice() -> None:
    """Covers: ETL-118 -- an edition with only its units zip published is asked for again."""

    class _Cursor:
        def __init__(self, rows):
            self.rows = rows

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, params):
            assert "control.census_govfin_slice" in sql
            assert params == ("cog_units", ["published", "empty"])

        def fetchall(self):
            return self.rows

    class _Connection:
        def __init__(self, rows):
            self.rows = rows

        def cursor(self):
            return _Cursor(self.rows)

    assert plan.answered(_Connection([(2002, "units")]), "cog_units") == set()
    assert plan.answered(
        _Connection([(2002, "units"), (2002, "directory")]), "cog_units"
    ) == {2002}


def test_key_rides_only_on_the_request_and_never_in_the_slice_or_error() -> None:
    """Covers: ETL-118 -- the Census key is never captured, fingerprinted or raised."""
    payload = json.dumps([[*GOVS_VARIABLES, "time", "state"]]).encode()
    client = _ScriptedClient([httpx.Response(200, content=payload)])
    response = fetch_api_slice("asslgf", 2022, "state", config=_config(), client=client)
    assert client.calls[0][1]["key"] == KEY
    assert "key" not in response.request_parameters
    assert KEY not in json.dumps(dict(response.request_parameters))

    failing = _ScriptedClient(
        [httpx.ConnectError(f"https://api.census.gov/data?key={KEY}")] * 2
    )
    with pytest.raises(GovFinFetchError) as raised:
        fetch_api_slice("asslgf", 2022, "state", config=_config(), client=failing)
    assert KEY not in str(raised.value)
    assert raised.value.code == "retry_exhausted"


def test_no_content_is_an_empty_slice_and_a_client_error_is_not_retried() -> None:
    """Covers: ETL-118 -- 204 is a published absence; 400 fails at once."""
    empty = fetch_api_slice(
        "qtax",
        1994,
        "us",
        config=_config(),
        client=_ScriptedClient([httpx.Response(204)]),
    )
    assert not empty.published and empty.raw_bytes == b""
    client = _ScriptedClient([httpx.Response(400), httpx.Response(200)])
    with pytest.raises(GovFinFetchError) as raised:
        fetch_api_slice("qtax", 1994, "us", config=_config(), client=client)
    assert raised.value.code == "non_retryable_http"
    assert len(client.calls) == 1


def test_archive_must_be_a_zip_holding_the_registered_member() -> None:
    """Covers: ETL-118 -- an HTML error page or a zip without the unit file is refused."""
    archive = get_edition(census_govfin.UNIT_YEAR).archives[0]
    assert archive.path == census_govfin.UNIT_ARCHIVE_PATH
    with pytest.raises(GovFinPayloadError) as raised:
        fetch_archive(
            archive, 2022, config=_config(),
            client=_ScriptedClient([httpx.Response(200, content=b"<html>moved</html>")]),
        )  # fmt: skip
    assert raised.value.code == "not_a_zip"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as bundle:
        bundle.writestr("readme.pdf", b"layout")
    with pytest.raises(GovFinPayloadError) as raised:
        fetch_archive(
            archive, 2022, config=_config(),
            client=_ScriptedClient([httpx.Response(200, content=buffer.getvalue())]),
        )  # fmt: skip
    assert raised.value.code == "missing_member"
    good = census_govfin.unit_archives()[archive.path]
    response = fetch_archive(
        archive,
        2022,
        config=_config(),
        client=_ScriptedClient([httpx.Response(200, content=good)]),
    )
    assert response.raw_bytes == good
    assert response.media_type == "application/zip"
    assert dict(response.request_parameters) == {
        "census_year": "2022",
        "slice": "bundle",
    }


def test_unit_fixture_parses_with_quarantine_reasons() -> None:
    """Covers: ETL-118 -- each record is read by position; a bad one is set aside with a reason."""
    edition = get_edition(census_govfin.UNIT_YEAR)
    archive = edition.archives[0]
    parsed = parse_archive(edition, archive, census_govfin.unit_bundle())
    assert parsed.row_count >= 30
    unit_errors = {
        (item.error_code, item.source_row_index)
        for item in parsed.quarantined
        if item.member.endswith("2022FinEstDAT_07152026modp.txt")
    }
    assert unit_errors == {("unreadable_government", 37), ("short_record", 38)}
    amounts = [item for item in parsed.amounts if item.fips_state == "55"]
    assert len(amounts) == 33
    dane_property = next(
        item for item in amounts
        if (item.government_code, item.item_code) == ("551025000001", "T01")
    )  # fmt: skip
    assert (dane_property.fips_county, dane_property.amount) == ("025", Decimal(150000))
    assert (dane_property.data_year, dane_property.imputation_flag) == (2022, "R")

    by_code = {item.government_code: item for item in parsed.governments}
    madison = by_code["552025000002"]
    assert (madison.fips_state, madison.fips_county, madison.government_type) == (
        "55", "025", "municipal",
    )  # fmt: skip
    assert madison.population == 272903
    assert by_code["550000000001"].fips_county is None
    assert by_code["554000000005"].fips_county is None
    levels = {(item.fips_state, item.level_code) for item in parsed.state_totals}
    assert {("55", "1"), ("55", "2"), ("55", "3")} <= levels
    assert all(item.level_code in {"1", "2", "3"} for item in parsed.state_totals)


def _fixed(width: int, **fields: tuple[int, str]) -> str:
    """A fixed-width line with each (start, text) placed at its 1-based column."""
    line = [" "] * width
    for start, text in fields.values():
        line[start - 1 : start - 1 + len(text)] = list(text)
    return "".join(line)


def test_2012_alphabetical_records_map_to_fips() -> None:
    """Covers: ETL-118 -- 2012 codes use Census alphabetical states; the directory gives FIPS."""
    edition = get_edition(2012)
    amounts, quarantined, _ = parse_unit_lines(
        ["0100000000000019T      7352842012R"], edition.data_layout
    )
    assert not quarantined
    assert (amounts[0].fips_state, amounts[0].item_code, amounts[0].amount) == (
        "01", "19T", Decimal(735284),
    )  # fmt: skip
    # Wisconsin is 50 in the alphabetical scheme and 55 in FIPS.
    directory_line = _fixed(
        153,
        code=(1, "50201300100000"), name=(15, "MADISON CITY"), county=(79, "Dane"),
        state=(114, "55"), fips_county=(116, "025"), population=(124, "   233209"),
        fiscal=(148, "1231"),
    )  # fmt: skip
    governments, quarantined, _ = parse_directory_lines(
        [directory_line], edition.directory_layout, edition.data_layout.state_scheme
    )
    assert not quarantined
    assert (governments[0].fips_state, governments[0].fips_county) == ("55", "025")
    assert governments[0].population == 233209

    disagreeing = directory_line[:113] + "17" + directory_line[115:]
    _, quarantined, _ = parse_directory_lines(
        [disagreeing], edition.directory_layout, edition.data_layout.state_scheme
    )
    assert [item.error_code for item in quarantined] == ["state_code_disagrees"]

    state_line = _fixed(32, state=(1, "50"), level=(3, "3"), item=(5, "T01"),
                        amount=(9, "      795000"))  # fmt: skip
    totals, quarantined, _ = parse_state_total_lines(
        [state_line], edition.state_total_layout
    )
    assert not quarantined
    assert (totals[0].fips_state, totals[0].level_code, totals[0].amount) == (
        "55", "3", Decimal(795000),
    )  # fmt: skip


def test_2002_is_reported_only_and_reads_its_longer_directory() -> None:
    """Covers: ETL-118 -- 2002 has no imputation flag, no state totals, and a 161-character directory."""
    edition = get_edition(2002)
    assert edition.record_coverage == "reported_only"
    assert edition.state_total_layout is None
    assert [archive.slice_key for archive in edition.archives] == ["units", "directory"]
    amounts, quarantined, _ = parse_unit_lines(
        ["50201300100000T01      123456"], edition.data_layout
    )
    assert not quarantined
    assert (amounts[0].fips_state, amounts[0].imputation_flag, amounts[0].data_year) == (
        "55", None, None,
    )  # fmt: skip
    directory_line = _fixed(
        161,
        code=(1, "01100100100000"), name=(15, "AUTAUGA COUNTY"), county=(79, "AUTAUGA"),
        state=(114, "01"), fips_county=(116, "001"), population=(124, "    43671"),
        fiscal=(155, "0930"),
    )  # fmt: skip
    governments, quarantined, _ = parse_directory_lines(
        [directory_line], edition.directory_layout, edition.data_layout.state_scheme
    )
    assert not quarantined
    assert (governments[0].fips_county, governments[0].fiscal_year_ending) == (
        "001",
        "0930",
    )


def test_a_non_numeric_amount_is_missing_never_zero() -> None:
    """Covers: ETL-118 -- Census text that is not a number is kept as missing."""
    line = "50201300100000T01" + "(X)".rjust(12) + " " * 6
    amounts, quarantined, count = parse_unit_lines([line], UNIT_LAYOUT_2002)
    assert count == 1 and not quarantined
    assert amounts[0].amount is None and amounts[0].value_status == "missing"
    assert amounts[0].amount_source == "(X)"
    assert decimal_or_none("1,234") == Decimal(1234)
    assert decimal_or_none("NaN") is None


def test_rollup_is_planned_from_the_runs_own_statistics_with_nested_loops_off() -> None:
    """Covers: ETL-118 -- the roll-up analyses the run's uncommitted records first.

    The first live 2002 replay was planned from statistics that could not see
    the run's records (rows=1), and its directory join scanned every
    government for each of 1.4 million records for hours.
    """
    from uuid import uuid4

    from data_ingestion_toolbox.census_govfin.silver_census_govfin import load

    class _Cursor:
        def __init__(self) -> None:
            self.statements: list[tuple[str, object]] = []
            self.rowcount = 0

        def execute(self, sql, params=None):
            self.statements.append((sql, params))
            if sql is load._ROLLUP_SQL:
                self.rowcount = 42

        def fetchone(self):
            return ("on",)

    cursor = _Cursor()
    assert load._rollup(cursor, uuid4(), 2002) == 42
    sqls = [sql for sql, _params in cursor.statements]
    rollup_at = sqls.index(load._ROLLUP_SQL)
    analysed = [sql for sql in sqls[:rollup_at] if sql.startswith("ANALYZE ")]
    assert analysed == [f"ANALYZE {name}" for name in load.ROLLUP_ANALYZED_RELATIONS]
    assert "silver_census_govfin.unit_amount" in load.ROLLUP_ANALYZED_RELATIONS
    assert "silver_census_govfin.unit_government" in load.ROLLUP_ANALYZED_RELATIONS
    assert "SELECT set_config('enable_nestloop', 'off', true)" in sqls[:rollup_at]
    restore_sql, restore_params = cursor.statements[rollup_at + 1]
    assert restore_sql == "SELECT set_config('enable_nestloop', %s, true)"
    assert restore_params == ("on",)
