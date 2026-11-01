"""Age and sex structure arithmetic (ADR-0013), shared by the API and the
warehouse summary builder.

One place in one release goes in as its published lines; sums, shares,
dependency ratios and the sex ratio come out as numbers with their Census
Bureau margins. Formatting for a response is the caller's: the API renders
these numbers as strings, the summary builder writes them as ``NUMERIC``.
Keeping the arithmetic here is what makes a mapped old-age dependency ratio
the same number the place page prints.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional, Sequence

from data_ingestion_toolbox.semantics.age_bands import SEXES, AgeScheme
from data_ingestion_toolbox.semantics.derived_ratios import derived_margin

#: The z-value of the Bureau's 90 percent margins.
Z90 = Decimal("1.645")
#: A band whose coefficient of variation is above this is marked unreliable.
RELIABLE_CV_PERCENT = Decimal(30)
#: Levels whose total population is a controlled estimate published without
#: a margin; the Bureau's instruction is to treat that margin as zero.
CONTROLLED_TOTAL_LEVELS = frozenset({"STATE", "NATIONAL"})
_LEVEL_ALIASES = {"NATION": "NATIONAL", "US": "NATIONAL"}


def to_number(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def root_sum_squares(margins: Sequence[Optional[Decimal]]) -> Optional[Decimal]:
    """The Bureau's approximate margin of a sum: sqrt of the summed squares."""
    if any(margin is None for margin in margins):
        return None
    return sum((margin * margin for margin in margins), Decimal(0)).sqrt()  # type: ignore[operator]


def ratio_margin(
    numerator: Decimal,
    numerator_moe: Decimal,
    denominator: Decimal,
    denominator_moe: Decimal,
) -> Decimal:
    """The Bureau's margin of ``numerator / denominator`` (unscaled)."""
    margin, _ = derived_margin(
        numerator / denominator, numerator_moe, denominator, denominator_moe, False
    )
    return margin


def proportion_margin(
    part: Decimal, part_moe: Decimal, whole: Decimal, whole_moe: Decimal
) -> Decimal:
    """The Bureau's margin of ``part / whole`` for a part inside the whole."""
    margin, _ = derived_margin(part / whole, part_moe, whole, whole_moe, True)
    return margin


def coefficient_of_variation(
    count: Optional[Decimal], moe: Optional[Decimal]
) -> Optional[Decimal]:
    """``(MOE / 1.645) / count`` as a percent; ``None`` when undefined."""
    if count is None or moe is None or count <= 0:
        return None
    return moe / Z90 / count * 100


@dataclass(frozen=True)
class Line:
    """One published line: its estimate (``None`` when not usable) and MOE."""

    estimate: Optional[Decimal]
    moe: Optional[Decimal]
    suppressed: bool = False


@dataclass
class PlaceRelease:
    """Every scheme line served for one place in one release."""

    geo_id: str
    geo_level: str
    vintage_year: int
    period_start: Optional[str]
    period_end: Optional[str]
    lines: dict[str, Line] = field(default_factory=dict)


@dataclass(frozen=True)
class Summed:
    count: Optional[Decimal]
    moe: Optional[Decimal]
    reason: Optional[str] = None


@dataclass(frozen=True)
class ShareValue:
    """A count, its margin, and its share of the place's total (percent)."""

    count: Optional[Decimal]
    count_moe: Optional[Decimal]
    share: Optional[Decimal] = None
    share_moe: Optional[Decimal] = None
    reason: Optional[str] = None


@dataclass(frozen=True)
class RatioValue:
    """A ratio per ``scale``, its margin, and how the margin was found."""

    value: Optional[Decimal] = None
    moe: Optional[Decimal] = None
    moe_method: str = "none"
    reason: Optional[str] = None


def variable_of(metric_code: str) -> str:
    """``CENSUS_ACS:acs5:B01001_003`` → ``B01001_003`` (no trailing ``E``)."""
    variable = metric_code.rsplit(":", 1)[-1]
    return variable[:-1] if variable.endswith("E") else variable


def iso_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def releases_from_rows(
    rows: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, int], PlaceRelease]:
    """Group served rows by (place, vintage); each line once."""
    out: dict[tuple[str, int], PlaceRelease] = {}
    for row in rows:
        vintage = int(row["vintage_year"])
        key = (str(row["geo_id"]), vintage)
        release = out.get(key)
        if release is None:
            start = row.get("duration_start") or row.get("observation_date")
            release = out[key] = PlaceRelease(
                geo_id=key[0],
                geo_level=str(row.get("geo_level") or ""),
                vintage_year=vintage,
                period_start=iso_text(start),
                period_end=iso_text(row.get("duration_end")),
            )
        status = str(row.get("value_status") or "valid")
        estimate = to_number(row.get("estimate_value"))
        release.lines[variable_of(str(row["metric_code"]))] = Line(
            estimate=estimate if status == "valid" else None,
            moe=None
            if row.get("margin_of_error") is None
            else abs(to_number(row["margin_of_error"]) or Decimal(0)),
            suppressed=status != "valid" or estimate is None,
        )
    return out


def sum_lines(release: PlaceRelease, codes: Sequence[str]) -> Summed:
    estimates: list[Decimal] = []
    margins: list[Optional[Decimal]] = []
    for code in codes:
        line = release.lines.get(code)
        if line is None:
            return Summed(None, None, "line_missing")
        if line.suppressed or line.estimate is None:
            return Summed(None, None, "suppressed_input")
        estimates.append(line.estimate)
        margins.append(line.moe)
    return Summed(sum(estimates, Decimal(0)), root_sum_squares(margins))


def total_of(scheme: AgeScheme, release: PlaceRelease, key: str) -> Summed:
    """``all``, ``men`` or ``women``: the table's own total line."""
    summed = sum_lines(release, [scheme.totals[key]])
    level = str(release.geo_level).strip().upper()
    level = _LEVEL_ALIASES.get(level, level)
    if (
        summed.count is not None
        and summed.moe is None
        and level in CONTROLLED_TOTAL_LEVELS
    ):
        return Summed(summed.count, Decimal(0))
    return summed


def group_sum(scheme: AgeScheme, release: PlaceRelease, group_id: str) -> Summed:
    if group_id in SEXES:
        return total_of(scheme, release, group_id)
    group = scheme.group(group_id)
    return sum_lines(release, [*group.men, *group.women])


def share_value(summed: Summed, total: Summed) -> ShareValue:
    if summed.count is None:
        return ShareValue(None, None, reason=summed.reason)
    if total.count is None or total.count == 0:
        return ShareValue(summed.count, summed.moe, reason="total_missing")
    share = summed.count / total.count * 100
    share_moe = None
    if summed.moe is not None and total.moe is not None:
        share_moe = (
            proportion_margin(summed.count, summed.moe, total.count, total.moe) * 100
        )
    return ShareValue(summed.count, summed.moe, share, share_moe)


def ratio_value(numerator: Summed, denominator: Summed, scale: int) -> RatioValue:
    reason = numerator.reason or denominator.reason
    if reason is not None or numerator.count is None or denominator.count is None:
        return RatioValue(reason=reason or "suppressed_input")
    if denominator.count == 0:
        return RatioValue(reason="denominator_zero")
    value = numerator.count / denominator.count * scale
    if numerator.moe is None or denominator.moe is None:
        return RatioValue(value=value, moe_method="unavailable")
    margin = ratio_margin(
        numerator.count, numerator.moe, denominator.count, denominator.moe
    )
    return RatioValue(value=value, moe=margin * scale, moe_method="ratio")


def indicator_values(
    scheme: AgeScheme, release: PlaceRelease
) -> dict[str, ShareValue | RatioValue]:
    """Each broad group as a share of the total, and each indicator ratio."""
    total = total_of(scheme, release, "all")
    values: dict[str, ShareValue | RatioValue] = {
        group.group_id: share_value(group_sum(scheme, release, group.group_id), total)
        for group in scheme.groups
    }
    for indicator in scheme.indicators:
        parts = [group_sum(scheme, release, part) for part in indicator.numerator]
        missing = next((part for part in parts if part.count is None), None)
        numerator = missing or Summed(
            sum((part.count for part in parts), Decimal(0)),  # type: ignore[misc]
            root_sum_squares([part.moe for part in parts]),
        )
        denominator = group_sum(scheme, release, indicator.denominator)
        values[indicator.indicator_id] = ratio_value(
            numerator, denominator, indicator.scale
        )
    return values
