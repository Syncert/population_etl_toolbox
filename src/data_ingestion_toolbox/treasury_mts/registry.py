"""The registered Monthly Treasury Statement table and its lines.

Read on OFFICE_LAB on 2026-11-12 from the Fiscal Data API
(``/services/api/fiscal_service/v1/accounting/mts/mts_table_9``, no key):

* Table 9, "Summary of Receipts and Outlays of the U.S. Government by
  Source/Function", one row per statement month (``record_date``, the last
  day of the month) and line. Record dates run from 2015-03-31 to
  2026-09-30; 33 lines a month, about 4,600 rows in all (77 pages of 60).
* Each row carries the current month, the current fiscal year to date and
  the prior fiscal year to date, in dollars and cents, as text; a heading
  line (Receipts, Net Outlays, Social Insurance and Retirement Receipts)
  carries the text ``null`` in all three.
* ``line_code_nbr`` names the line: receipts by source 20 to 120 (120 is
  their total), outlays by budget function 140 to 330 and their total 340.
* The federal fiscal year ends on September 30, so the September statement's
  fiscal-year-to-date column is the fiscal year's total as Treasury
  publishes it. It is read as published, never added up from months.

No credential is needed.
"""

from __future__ import annotations

from dataclasses import dataclass

FISCAL_DATA_BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
TABLE_PATH = "/v1/accounting/mts/mts_table_9"
FIRST_RECORD_DATE = "2015-03-31"

#: The fields the client asks for, in the API's own names.
FIELDS: tuple[str, ...] = (
    "record_date",
    "parent_id",
    "classification_id",
    "classification_desc",
    "current_month_rcpt_outly_amt",
    "current_fytd_rcpt_outly_amt",
    "prior_fytd_rcpt_outly_amt",
    "table_nbr",
    "src_line_nbr",
    "print_order_nbr",
    "line_code_nbr",
    "data_type_cd",
    "record_type_cd",
    "sequence_level_nbr",
    "sequence_number_cd",
    "record_fiscal_year",
    "record_fiscal_quarter",
    "record_calendar_month",
)

#: The statement months re-read by an ordinary run, counted back from the
#: newest month already published: Treasury can restate a recent month.
REVISION_WINDOW_MONTHS = 3


@dataclass(frozen=True)
class StatementLine:
    line_code: str
    measure_id: str
    label: str
    #: ``receipts`` (by source) or ``outlays`` (by budget function).
    kind: str
    is_total: bool = False


LINES: tuple[StatementLine, ...] = (
    StatementLine(
        "20", "receipts_individual_income_taxes", "Individual income taxes", "receipts"
    ),
    StatementLine(
        "30",
        "receipts_corporation_income_taxes",
        "Corporation income taxes",
        "receipts",
    ),
    StatementLine(
        "50",
        "receipts_employment_general_retirement",
        "Social insurance: employment and general retirement",
        "receipts",
    ),
    StatementLine(
        "60",
        "receipts_unemployment_insurance",
        "Social insurance: unemployment insurance",
        "receipts",
    ),
    StatementLine(
        "70",
        "receipts_other_retirement",
        "Social insurance: other retirement",
        "receipts",
    ),
    StatementLine("80", "receipts_excise_taxes", "Excise taxes", "receipts"),
    StatementLine(
        "90", "receipts_estate_gift_taxes", "Estate and gift taxes", "receipts"
    ),
    StatementLine("100", "receipts_customs_duties", "Customs duties", "receipts"),
    StatementLine(
        "110", "receipts_miscellaneous", "Miscellaneous receipts", "receipts"
    ),
    StatementLine("120", "receipts_total", "Total receipts", "receipts", True),
    StatementLine("140", "outlays_national_defense", "National defense", "outlays"),
    StatementLine(
        "150", "outlays_international_affairs", "International affairs", "outlays"
    ),
    StatementLine(
        "160",
        "outlays_science_space_technology",
        "General science, space, and technology",
        "outlays",
    ),
    StatementLine("170", "outlays_energy", "Energy", "outlays"),
    StatementLine(
        "180",
        "outlays_natural_resources_environment",
        "Natural resources and environment",
        "outlays",
    ),
    StatementLine("190", "outlays_agriculture", "Agriculture", "outlays"),
    StatementLine(
        "200",
        "outlays_commerce_housing_credit",
        "Commerce and housing credit",
        "outlays",
    ),
    StatementLine("210", "outlays_transportation", "Transportation", "outlays"),
    StatementLine(
        "220",
        "outlays_community_regional_development",
        "Community and regional development",
        "outlays",
    ),
    StatementLine(
        "230",
        "outlays_education_training_employment_social",
        "Education, training, employment, and social services",
        "outlays",
    ),
    StatementLine("240", "outlays_health", "Health", "outlays"),
    StatementLine("250", "outlays_medicare", "Medicare", "outlays"),
    StatementLine("260", "outlays_income_security", "Income security", "outlays"),
    StatementLine("270", "outlays_social_security", "Social Security", "outlays"),
    StatementLine(
        "280", "outlays_veterans_benefits", "Veterans benefits and services", "outlays"
    ),
    StatementLine(
        "290",
        "outlays_administration_of_justice",
        "Administration of justice",
        "outlays",
    ),
    StatementLine("300", "outlays_general_government", "General government", "outlays"),
    StatementLine("320", "outlays_net_interest", "Net interest", "outlays"),
    StatementLine(
        "330",
        "outlays_undistributed_offsetting_receipts",
        "Undistributed offsetting receipts",
        "outlays",
    ),
    StatementLine("340", "outlays_total", "Total outlays", "outlays", True),
)
LINES_BY_CODE: dict[str, StatementLine] = {line.line_code: line for line in LINES}

#: The heading lines Treasury prints with no amounts.
HEADING_LINE_CODES = frozenset({"10", "40", "130"})


def outlay_function_lines() -> tuple[StatementLine, ...]:
    """The budget-function lines whose shares of total outlays are served."""
    return tuple(line for line in LINES if line.kind == "outlays" and not line.is_total)
