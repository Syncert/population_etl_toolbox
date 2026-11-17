"""Taxes and public money for one place (taxes-and-public-money T-6).

Two reads, both scoped to one place:

* ``public_money`` answers the place page's "Taxes and public money" group
  for a county, a state or the nation, beside its state and the nation. Every
  share is an approved pairing (ADR-0009) computed by the derived-ratio
  service's own matching rules over rows read for exactly these geographies;
  the state and local tax burden is read from ``gold_census_govfin.tax_burden``,
  where it is computed at publish.
* ``governor_terms`` serves ``gold_elections.governor_term_fiscal`` and
  ``governor_term_tax_mix`` for one state.

ADR-0014's fiscal section applies throughout: each measure on its own, no
score or rank, fiscal years dated by the calendar year they end in, taxes
"collected in" a place, payments read one way round. A missing value is
null with a reason, never zero.
"""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Iterable, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.schemas.derived import DerivedRatioInput, DerivedRatioObservation
from apps.api.schemas.public_money import (
    FiscalMeasureDefinition,
    GovernorTenure,
    GovernorTermsResponse,
    IncomeSource,
    IncomeSources,
    LevelSeries,
    MeasureSeries,
    PaymentsByProgram,
    PeriodValues,
    PointValue,
    ProgramShare,
    PublicMoneyLevel,
    PublicMoneyResponse,
    SeriesPoint,
    TaxMixShare,
    TenureFiscalChange,
)
from apps.api.schemas.observations import NeutralObservation
from apps.api.services.contracts import require_relation
from apps.api.services.derived_ratio_service import pair_rows
from apps.api.services.neutral_observations_service import (
    NeutralQueryError,
    observations_for_geographies,
)
from data_ingestion_toolbox.semantics.derived_ratios import RatioPairing, pairing_for

NATION = "us:1"
STATE_GEO_ID = re.compile(r"^state:(\d{2})$")
COUNTY_GEO_ID = re.compile(r"^state:(\d{2})\|county:\d{3}$")
QUANTUM = Decimal("0.01")

#: Program groups of the payments bar, in BEA's order. Each names one or more
#: BEA programs; `other` is the rest of BEA's total (veterans' benefits,
#: education and training, and the smaller programs), never an estimate.
#: A program is read through its approved pairing for the place's level
#: (`payment_ratio`).
PROGRAM_GROUPS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("social_security", "Social Security", ("social_security",)),
    ("medicare", "Medicare", ("medicare",)),
    ("medicaid", "Medicaid and other public medical care", ("medicaid",)),
    ("income_support", "SSI, EITC and SNAP", ("ssi", "eitc", "snap")),
    ("unemployment", "Unemployment insurance", ("unemployment_insurance",)),
)
OTHER_PROGRAMS = ("other", "Veterans' benefits and everything else")
PAYMENTS_TOTAL = "all"


def payment_ratio(program: str, geo_id: str) -> str:
    """The approved pairing that reads one BEA program for this place.

    A county divides CAINC35 by CAINC1 personal income. A state and the nation
    divide SAINC35 by SAINC50 personal income: BEA publishes the state table a
    year or more ahead of the county one (SAINC35 ran to 2025 on 2026-11-06,
    CAINC35 to 2022), and CAINC35's state and nation rows never share a
    release with CAINC1's, so reading them left the state and nation empty.
    """
    level = "county" if COUNTY_GEO_ID.match(geo_id) else "state"
    return f"transfer_share_{program}_{level}"


INCOME_PARTS = (
    (
        "dividends_interest_rent",
        "Dividends, interest and rent",
        "dividends_interest_rent_share",
    ),
    ("payments", "Payments from government and others", "transfer_receipts_share"),
)
EARNINGS = ("earnings", "Earnings from work, and the rest")

FEDERAL_TAX_RATE = "income_tax_share_of_agi"
PROPERTY_TAX = "property_tax_to_home_value:acs5"

#: ADR-0014's fiscal wording, stated with every answer.
FISCAL_CAVEATS: tuple[str, ...] = (
    "Each measure stands on its own; nothing here is a score or a ranking.",
    "Where a tax is collected is not who bears it: tourists pay sales taxes, "
    "and business property and severance taxes reach owners and buyers elsewhere.",
    "Payments respond to a place's age and income; a high share describes "
    "who lives there, it does not cause it.",
    "A fiscal year is shown as the calendar year it ends in; 46 states end "
    "theirs on June 30.",
)

TAX_MIX_LABELS = {
    "property": "Property",
    "general_sales": "General sales",
    "income": "Individual and corporate income",
    "other": "Other taxes",
}
TAX_MIX_ORDER = tuple(TAX_MIX_LABELS)

#: Said once with every governor table (ADR-0014 rule 7 and fiscal section).
GOVERNOR_CAVEATS: tuple[str, ...] = (
    "These are comparisons, not causes: a governor shares credit and blame "
    "with the legislature, federal policy, the Federal Reserve and national trends.",
    "Each number is the state's change over the years compared minus the "
    "nation's change over the same years, in percentage points.",
    "Tax and payment changes are not good or bad in themselves; whether "
    "higher taxes or payments are better is the reader's call.",
    *FISCAL_CAVEATS[1:],
)


# ---------------------------------------------------------------------------
# Small arithmetic over served text values
# ---------------------------------------------------------------------------


def _number(value: Optional[str]) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _text(value: Optional[Decimal]) -> Optional[str]:
    return None if value is None else str(value.quantize(QUANTUM))


def _year(row: DerivedRatioObservation) -> int:
    # An ACS five-year estimate is named by its last year; an annual figure's
    # start and end share a year.
    return int(str(row.period_end or row.period_start)[:4])


def levels_for(geo_id: str) -> list[PublicMoneyLevel]:
    """The place, its state and the nation; only these three grains."""
    if geo_id == NATION:
        return [PublicMoneyLevel(geo_id=NATION, role="nation")]
    state = STATE_GEO_ID.match(geo_id)
    if state:
        return [
            PublicMoneyLevel(geo_id=geo_id, role="state"),
            PublicMoneyLevel(geo_id=NATION, role="nation"),
        ]
    county = COUNTY_GEO_ID.match(geo_id)
    if county:
        return [
            PublicMoneyLevel(geo_id=geo_id, role="this place"),
            PublicMoneyLevel(geo_id=f"state:{county.group(1)}", role="state"),
            PublicMoneyLevel(geo_id=NATION, role="nation"),
        ]
    raise NeutralQueryError(
        "taxes and public money are served for the nation (us:1), a state "
        "(state:NN) or a county (state:NN|county:NNN)"
    )


def ratio_values(
    db: Session, ratio_id: str, geo_ids: Iterable[str], by_year: bool = False
) -> dict[tuple[str, int], DerivedRatioObservation]:
    """One approved pairing's values for these geographies, every period.

    Read exactly as ``/observations/derived`` reads it: the numerator and the
    denominator for these geographies, matched on geography, period and
    release. A pairing the registry does not serve, or a metric the catalog
    does not hold, is simply absent.
    """
    pairing = pairing_for(ratio_id)
    if pairing is None or not pairing.is_served() or not pairing.denominator:
        return {}
    ids = sorted(set(geo_ids))
    numerators = observations_for_geographies(
        db, pairing.numerator, "latest", None, ids
    )
    denominators = observations_for_geographies(
        db, pairing.denominator, "latest", None, ids
    )
    if not numerators or denominators is None:
        return {}
    if by_year:
        return _paired_by_year(pairing, numerators, denominators)
    return {
        (row.geo_id, _year(row)): row
        for row in pair_rows(pairing, numerators, denominators)
    }


def _paired_by_year(
    pairing: RatioPairing,
    numerators: list[NeutralObservation],
    denominators: list[NeutralObservation],
) -> dict[tuple[str, int], DerivedRatioObservation]:
    """Each input's newest release for the year, matched on place and year.

    BEA's county program file (CAINC35) trails its income file (CAINC1) by
    years, so the two never share a release and a county had no payments at
    all. Each derived row keeps both inputs, each with its own release, so
    the page can name the two files (Nick, 2026-11-06).
    """
    bare = {"release": None}
    rows = pair_rows(
        pairing,
        [row.model_copy(update=bare) for row in numerators],
        [row.model_copy(update=bare) for row in denominators],
    )
    released = {
        (row.metric_code, row.geo_id, row.period_start, row.period_end): row.release
        for row in [*numerators, *denominators]
    }

    def restored(
        item: Optional[DerivedRatioInput], geo_id: str
    ) -> Optional[DerivedRatioInput]:
        if item is None:
            return None
        key = (item.metric_code, geo_id, item.period_start, item.period_end)
        return item.model_copy(update={"release": released.get(key)})

    answer = {}
    for row in rows:
        numerator = restored(row.numerator, row.geo_id)
        answer[(row.geo_id, _year(row))] = row.model_copy(
            update={
                "release": numerator.release if numerator else None,
                "numerator": numerator,
                "denominator": restored(row.denominator, row.geo_id),
            }
        )
    return answer


def _value(
    values: dict[tuple[str, int], DerivedRatioObservation], geo_id: str, year: int
) -> Optional[Decimal]:
    row = values.get((geo_id, year))
    return None if row is None else _number(row.value)


def _newest_year(
    values: dict[tuple[str, int], DerivedRatioObservation], geo_id: str
) -> Optional[int]:
    years = [
        year
        for (geo, year), row in values.items()
        if geo == geo_id and row.value is not None
    ]
    return max(years) if years else None


# ---------------------------------------------------------------------------
# The place answer
# ---------------------------------------------------------------------------


def program_values(
    db: Session, program: str, geo_ids: Iterable[str]
) -> dict[tuple[str, int], DerivedRatioObservation]:
    """One BEA program's shares for these places, each through its level's pairing."""
    by_ratio: dict[str, list[str]] = {}
    for geo_id in geo_ids:
        by_ratio.setdefault(payment_ratio(program, geo_id), []).append(geo_id)
    values: dict[tuple[str, int], DerivedRatioObservation] = {}
    for ratio_id, ids in by_ratio.items():
        values.update(
            ratio_values(db, ratio_id, ids, by_year=ratio_id.endswith("_county"))
        )
    return values


def _payments(db: Session, levels: list[PublicMoneyLevel]) -> list[PaymentsByProgram]:
    geo_ids = [level.geo_id for level in levels]
    total = program_values(db, PAYMENTS_TOTAL, geo_ids)
    # The place's newest year; a place with none (a county whose program
    # detail BEA has not published beside its current income) is shown as
    # not published, beside its state and the nation in their newest year.
    year = next(
        (
            newest
            for newest in (_newest_year(total, geo_id) for geo_id in geo_ids)
            if newest is not None
        ),
        None,
    )
    if year is None:
        return []
    parts = {
        program: program_values(db, program, geo_ids)
        for _group, _label, programs in PROGRAM_GROUPS
        for program in programs
    }
    answer = []
    for geo_id in geo_ids:
        whole = _value(total, geo_id, year)
        programs = []
        named: Optional[Decimal] = Decimal(0)
        for group, label, members in PROGRAM_GROUPS:
            shares = [_value(parts[member], geo_id, year) for member in members]
            share = (
                None
                if any(value is None for value in shares)
                else sum(shares, Decimal(0))
            )
            named = None if named is None or share is None else named + share
            programs.append(
                ProgramShare(program=group, label=label, share_pct=_text(share))
            )
        rest = None if whole is None or named is None else whole - named
        if rest is not None and rest < 0:
            # The named programs exceed BEA's total only when their releases
            # differ; the rest is then unknown, never forced to zero.
            rest = None
        programs.append(
            ProgramShare(
                program=OTHER_PROGRAMS[0],
                label=OTHER_PROGRAMS[1],
                share_pct=_text(rest),
            )
        )
        row = total.get((geo_id, year))
        answer.append(
            PaymentsByProgram(
                geo_id=geo_id,
                year=year,
                program_release=row.numerator.release if row else None,
                income_release=row.denominator.release
                if row and row.denominator
                else None,
                total_pct=_text(whole),
                programs=programs,
                unavailable_reason=None
                if whole is not None
                else "not_published_that_year",
            )
        )
    return answer


def _income_sources(db: Session, levels: list[PublicMoneyLevel]) -> list[IncomeSources]:
    geo_ids = [level.geo_id for level in levels]
    values = {
        ratio_id: ratio_values(db, ratio_id, geo_ids)
        for _s, _l, ratio_id in INCOME_PARTS
    }
    years = [
        _newest_year(values[ratio_id], levels[0].geo_id)
        for _s, _l, ratio_id in INCOME_PARTS
    ]
    if any(year is None for year in years):
        return []
    year = min(years)  # the newest year both parts publish
    answer = []
    for geo_id in geo_ids:
        shares = [
            _value(values[ratio_id], geo_id, year) for _s, _l, ratio_id in INCOME_PARTS
        ]
        rest = (
            None
            if any(share is None for share in shares)
            else Decimal(100) - sum(shares)
        )
        parts = [
            IncomeSource(source=EARNINGS[0], label=EARNINGS[1], share_pct=_text(rest))
        ]
        parts += [
            IncomeSource(source=source, label=label, share_pct=_text(share))
            for (source, label, _ratio), share in zip(INCOME_PARTS, shares)
        ]
        answer.append(
            IncomeSources(
                geo_id=geo_id,
                year=year,
                parts=parts,
                unavailable_reason=None
                if rest is not None
                else "not_published_that_year",
            )
        )
    return answer


def _ratio_series(db: Session, ratio_id: str, geo_ids: list[str]) -> list[LevelSeries]:
    values = ratio_values(db, ratio_id, geo_ids)
    series = []
    for geo_id in geo_ids:
        points = sorted(
            (
                SeriesPoint(year=year, value=row.value)
                for (geo, year), row in values.items()
                if geo == geo_id
            ),
            key=lambda point: point.year,
        )
        if points:
            series.append(LevelSeries(geo_id=geo_id, points=points))
    return series


def _federal_tax_rate(db: Session, levels: list[PublicMoneyLevel]) -> MeasureSeries:
    return MeasureSeries(
        measure="federal_tax_rate",
        label="Federal income tax as a share of adjusted gross income",
        unit="percent of adjusted gross income",
        source="IRS Statistics of Income, county and state income data",
        derived_from=FEDERAL_TAX_RATE,
        caveats=[
            "Income tax after credits over adjusted gross income on residents' returns; "
            "people who file no return are in neither.",
            "County files carry income tax from tax year 2010 and the nation's "
            "series starts in 2020; earlier county files publish no tax item.",
            "The 2018 federal tax law changed what counts as income and tax, so "
            "part of any change across 2018 is accounting.",
        ],
        series=_ratio_series(db, FEDERAL_TAX_RATE, [level.geo_id for level in levels]),
    )


def _tax_burden(db: Session, levels: list[PublicMoneyLevel]) -> MeasureSeries:
    """State and local taxes over personal income: states and the nation
    every fiscal year (Census's state and local totals), a county's local
    governments in Census of Governments years only."""
    geo_ids = [level.geo_id for level in levels]
    require_relation(db, "gold_census_govfin.tax_burden")
    rows = (
        db.execute(
            text(
                """
            SELECT geo_id, fiscal_year, dataset_id,
                   taxes_pct_of_personal_income::TEXT AS value
            FROM gold_census_govfin.tax_burden
            WHERE geo_id = ANY(:geo_ids)
              AND ((dataset_id = 'asslgf' AND government_level = 'state_and_local'
                    AND geo_id NOT LIKE '%|county:%')
                   OR (dataset_id = 'cog_units' AND geo_id LIKE '%|county:%'))
            ORDER BY geo_id, fiscal_year
            """
            ),
            {"geo_ids": geo_ids},
        )
        .mappings()
        .all()
    )
    by_geo: dict[str, list[SeriesPoint]] = {}
    for row in rows:
        by_geo.setdefault(row["geo_id"], []).append(
            SeriesPoint(year=int(row["fiscal_year"]), value=row["value"])
        )
    return MeasureSeries(
        measure="tax_burden",
        label="Taxes collected, share of personal income",
        unit="percent of personal income",
        source=(
            "Census Annual Survey of State and Local Government Finances and Census of "
            "Governments; BEA personal income"
        ),
        derived_from="gold_census_govfin.tax_burden",
        caveats=[
            "States and the nation: taxes collected by the state and all its local "
            "governments, each fiscal year.",
            "A county: taxes collected by the local governments in the county area "
            "(county, cities, towns, school and special districts), only in Census of "
            "Governments years, which end in 2 and 7; a government spanning counties "
            "is counted in its home county.",
            FISCAL_CAVEATS[1],
            FISCAL_CAVEATS[3],
        ],
        series=[
            LevelSeries(geo_id=geo_id, points=by_geo[geo_id])
            for geo_id in geo_ids
            if geo_id in by_geo
        ],
    )


def _property_tax(db: Session, levels: list[PublicMoneyLevel]) -> PeriodValues:
    geo_ids = [level.geo_id for level in levels]
    values = ratio_values(db, PROPERTY_TAX, geo_ids)
    year = _newest_year(values, levels[0].geo_id)
    points = []
    period = None
    for geo_id in geo_ids:
        row = values.get((geo_id, year)) if year is not None else None
        if row is not None and period is None:
            period = f"{str(row.period_start)[:4]}–{str(row.period_end)[:4]}"
        points.append(
            PointValue(
                geo_id=geo_id,
                value=None if row is None else row.value,
                margin_of_error=None if row is None else row.margin_of_error,
                unavailable_reason=(
                    "not_published_that_year" if row is None else row.unavailable_reason
                ),
            )
        )
    return PeriodValues(
        measure="property_tax_load",
        label="Median property tax as a percent of median home value",
        unit="percent of median home value",
        source="American Community Survey 5-year estimates, B25103 and B25077",
        period=period,
        caveats=[
            "A ratio of two medians, not any home's own rate.",
            "Home value is the owner's estimate; where values rose faster than "
            "assessments, the ratio falls without any change in tax law.",
        ],
        values=points,
    )


def _release_date(release: Optional[str]) -> Optional[str]:
    """BEA's release date, as people write it: November 16, 2023."""
    if not release:
        return None
    try:
        day = date.fromisoformat(release[:10])
    except ValueError:
        return release
    return f"{day:%B} {day.day}, {day.year}"


def payments_source(payments: list[PaymentsByProgram]) -> str:
    """Which BEA files the payments shares divide, with their release dates.

    A county's program figures (CAINC35) and its income (CAINC1) come from
    files BEA released years apart; the reader is told both.
    """
    county = next(
        (row for row in payments if COUNTY_GEO_ID.match(row.geo_id) and row.total_pct),
        None,
    )
    state_files = "SAINC35 transfer receipts over SAINC50 personal income"
    if county is None:
        return f"BEA {state_files}"
    programs = _release_date(county.program_release)
    income = _release_date(county.income_release)
    county_files = (
        f"CAINC35 transfer receipts (released {programs}) over CAINC1 personal "
        f"income (released {income})"
        if programs and income and programs != income
        else "CAINC35 transfer receipts over CAINC1 personal income"
    )
    return f"BEA: the county, {county_files}; the state and nation, {state_files}"


def public_money(db: Session, geo_id: str) -> PublicMoneyResponse:
    levels = levels_for(geo_id)
    payments = _payments(db, levels)
    return PublicMoneyResponse(
        geo_id=geo_id,
        levels=levels,
        payments=payments,
        payments_source=payments_source(payments),
        income_sources=_income_sources(db, levels),
        federal_tax_rate=_federal_tax_rate(db, levels),
        tax_burden=_tax_burden(db, levels),
        property_tax=_property_tax(db, levels),
        caveats=list(FISCAL_CAVEATS),
    )


# ---------------------------------------------------------------------------
# Governor tenures
# ---------------------------------------------------------------------------


def governor_terms(db: Session, geo_id: str) -> GovernorTermsResponse:
    state = STATE_GEO_ID.match(geo_id)
    if not state:
        raise NeutralQueryError("governor terms are served for a state (state:NN)")
    fips = state.group(1)
    for relation in (
        "gold_elections.governor_term_fiscal",
        "gold_elections.governor_term_tax_mix",
        "gold_elections.governor_fiscal_measure_definition",
    ):
        require_relation(db, relation)
    # The governor views plan as many small index lookups whose summed cost
    # estimate crosses jit_above_cost, so Postgres JIT-compiled about 3,900
    # functions per read: 1.1 s of a 1.17 s read on OFFICE_LAB on
    # 2026-11-06, for a query that runs in about 60 ms. SET LOCAL ends with
    # this request's read-only transaction.
    db.execute(text("SET LOCAL jit = off"))
    definitions = (
        db.execute(
            text(
                """
            SELECT measure, measure_label, change_unit, source_label, observation_basis
            FROM gold_elections.governor_fiscal_measure_definition
            ORDER BY display_order
            """
            )
        )
        .mappings()
        .all()
    )
    rows = (
        db.execute(
            text(
                """
            SELECT tenure_key, state_code, holder_name, party_code, party_text,
                   start_date::TEXT AS start_date, end_date::TEXT AS end_date, so_far,
                   term_count, measure, measure_label, window_start_year, window_end_year,
                   place_change::TEXT AS place_change, nation_change::TEXT AS nation_change,
                   net_change::TEXT AS net_change, unavailable_reason
            FROM gold_elections.governor_term_fiscal
            WHERE state_fips = :fips
            ORDER BY start_date DESC, tenure_key, display_order
            """
            ),
            {"fips": fips},
        )
        .mappings()
        .all()
    )
    mixes = (
        db.execute(
            text(
                """
            SELECT tenure_key, fiscal_year, tax_group, share_of_taxes_pct::TEXT AS share
            FROM gold_elections.governor_term_tax_mix
            WHERE state_fips = :fips
            """
            ),
            {"fips": fips},
        )
        .mappings()
        .all()
    )

    mix_by_tenure: dict[str, tuple[int, dict[str, str]]] = {}
    for mix in mixes:
        year, shares = mix_by_tenure.setdefault(
            mix["tenure_key"], (int(mix["fiscal_year"]), {})
        )
        shares[mix["tax_group"]] = mix["share"]

    tenures: dict[str, GovernorTenure] = {}
    state_code = None
    for row in rows:
        state_code = state_code or row["state_code"]
        tenure = tenures.get(row["tenure_key"])
        if tenure is None:
            year, shares = mix_by_tenure.get(row["tenure_key"], (None, {}))
            tenure = tenures[row["tenure_key"]] = GovernorTenure(
                tenure_key=row["tenure_key"],
                holder_name=row["holder_name"],
                party_code=row["party_code"],
                party_text=row["party_text"],
                start_date=row["start_date"],
                end_date=row["end_date"],
                so_far=bool(row["so_far"]),
                term_count=int(row["term_count"]),
                fiscal=[],
                tax_mix_year=year,
                tax_mix=[
                    TaxMixShare(
                        tax_group=group,
                        label=TAX_MIX_LABELS[group],
                        share_pct=shares[group],
                    )
                    for group in TAX_MIX_ORDER
                    if group in shares
                ],
            )
        tenure.fiscal.append(
            TenureFiscalChange(
                measure=row["measure"],
                label=row["measure_label"],
                window_start_year=int(row["window_start_year"]),
                window_end_year=row["window_end_year"],
                place_change=row["place_change"],
                nation_change=row["nation_change"],
                net_change=row["net_change"],
                unavailable_reason=row["unavailable_reason"],
            )
        )
    return GovernorTermsResponse(
        geo_id=geo_id,
        state_code=state_code,
        measures=[
            FiscalMeasureDefinition(
                measure=row["measure"],
                label=row["measure_label"],
                change_unit=row["change_unit"],
                source=row["source_label"],
                basis=row["observation_basis"],
            )
            for row in definitions
        ],
        caveats=list(GOVERNOR_CAVEATS),
        items=list(tenures.values()),
    )
