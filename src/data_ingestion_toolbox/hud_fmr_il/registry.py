"""The registered HUD Fair Market Rent and income-limit workbooks and measures.

Read from HUD User's FMR and income-limit dataset pages and the workbooks
themselves on 2026-10-07. Each fiscal year's county-level file is a static
download under ``https://www.huduser.gov/portal/datasets/``; a fiscal year
HUD reissues within the year has a second, ``_revised`` workbook beside the
first, and both are registered so both editions are kept.

Every workbook has one data sheet named after the file and a
``Field_Descriptions`` sheet. The data sheet's first row is its header. Rows
are HUD's area values repeated once per county -- or, in New England, once
per town: ``fips`` is ten digits (state, county, county subdivision), and a
county subdivision of ``99999`` means the whole county. Column names embed a
year (``pop2023``, ``median2026``), so each edition names its own columns.

The values are a HUD FMR or income-limit *area's* figures (``hud_area_code``:
a metro area, a HUD metro subdivision, or a nonmetropolitan county), never a
county-specific estimate. FMRs take effect at the start of the federal
fiscal year (October 1) unless reissued; income limits take effect on their
own date.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

HUD_BASE_URL = "https://www.huduser.gov/portal/datasets"

FMR = "fmr"
INCOME_LIMITS = "il"
ORIGINAL = "original"
REVISED = "revised"

#: The whole-county subdivision code in a ten-digit ``fips``.
WHOLE_COUNTY = "99999"


@dataclass(frozen=True)
class HudFile:
    dataset: str
    fiscal_year: int
    edition: str
    path: str
    sheet: str
    #: When the values take effect, as HUD's dataset page states.
    effective_date: date

    @property
    def key(self) -> str:
        return f"{self.dataset}:fy{self.fiscal_year}:{self.edition}"

    @property
    def value_columns(self) -> tuple[tuple[str, str], ...]:
        """(measure, column) pairs this edition's sheet carries."""
        if self.dataset == FMR:
            return tuple((f"fmr_{rooms}br", f"fmr_{rooms}") for rooms in range(5))
        columns = [("median_family_income", f"median{self.fiscal_year}")]
        for measure, prefix in (
            ("income_limit_50", "l50"),
            ("income_limit_30", "ELI"),
            ("income_limit_80", "l80"),
        ):
            columns.extend(
                (f"{measure}_{size}p", f"{prefix}_{size}") for size in range(1, 9)
            )
        return tuple(columns)

    @property
    def required_columns(self) -> frozenset[str]:
        identity = {"fips", "hud_area_code", "hud_area_name", "metro"}
        return frozenset(identity | {column for _measure, column in self.value_columns})


REGISTERED_FILES: tuple[HudFile, ...] = (
    # FY 2026 FMRs, effective October 1, 2025, and their reissue effective
    # May 21, 2026 (91 FR 21301).
    HudFile(
        FMR,
        2026,
        ORIGINAL,
        "/fmr/fmr2026/FY26_FMRs.xlsx",
        "FY26_FMRs",
        date(2025, 10, 1),
    ),
    HudFile(
        FMR,
        2026,
        REVISED,
        "/fmr/fmr2026/FY26_FMRs_revised.xlsx",
        "FY26_FMRs_revised",
        date(2026, 5, 21),
    ),
    # FY 2027 FMRs (91 FR 56156), effective October 1, 2026.
    HudFile(
        FMR,
        2027,
        ORIGINAL,
        "/fmr/fmr2027/FY27_FMRs.xlsx",
        "FY27_FMRs",
        date(2026, 10, 1),
    ),
    # FY 2026 income limits, effective May 1, 2026.
    HudFile(
        INCOME_LIMITS,
        2026,
        ORIGINAL,
        "/il/il26/Section8-FY26.xlsx",
        "Section8-FY26",
        date(2026, 5, 1),
    ),
)


def registered_files() -> tuple[HudFile, ...]:
    return REGISTERED_FILES


def get_file(key: str) -> HudFile:
    for item in REGISTERED_FILES:
        if item.key == key:
            return item
    raise KeyError(f"{key} is not a registered HUD workbook")
