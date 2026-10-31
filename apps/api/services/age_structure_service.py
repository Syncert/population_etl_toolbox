"""A place's age and sex structure, beside every level it sits in (ADR-0013).

The pyramid, its summary indicators, its history across releases and its
cohorts are all sums and ratios of one table's lines (B01001) for one place
in one release, computed here at request time from served rows (ADR-0009
rule 7, ADR-0013 rule 8). The band scheme -- which lines make which band --
is the reviewed record in ``docs/semantics/age_bands.json``; nothing here
groups lines by reading their labels.

Reads (the prevention list): every statement filters ``metric_code = ANY``
and ``geo_id = ANY`` (or ``=``) as plain equality, so it is served by the
indexes led by ``(metric_code, geo_id)`` on ``gold_census.mv_acs_latest``
and ``gold_census.rpt_acs_observations``; a pinned release also bounds
``observation_date`` so only that year's partition is probed. The enclosing
levels are resolved once per request; one statement reads the place and all
of them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Mapping, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.registry import normalize_geo_level
from apps.api.schemas.age_structure import (
    AgeBandLines,
    AgeBandResult,
    AgeBandSide,
    AgeCohortsResponse,
    AgeCount,
    AgeIndicators,
    AgeRatio,
    AgeScheme as AgeSchemeOut,
    AgeShare,
    AgeStructureHistoryResponse,
    AgeStructurePlace,
    AgeStructureResponse,
    CohortItem,
    CohortPeriod,
    CohortRefusal,
    CohortSide,
    MedianAge,
    MedianAgeValue,
)
from apps.api.services.contracts import require_relation, session_memo
from apps.api.services.derived_ratio_service import derived_margin
from data_ingestion_toolbox.semantics.age_bands import (
    SEXES,
    AgeScheme,
    default_scheme,
)
from data_ingestion_toolbox.sql.catalog_queries import (
    GEOGRAPHY_IDENTITY_QUERY,
    GEOGRAPHY_RELATED_QUERY,
    GEOGRAPHY_RELATION,
    RELATIONSHIP_RELATION,
)

LATEST_RELATION = "gold_census.mv_acs_latest"
AS_RELEASED_RELATION = "gold_census.rpt_acs_observations"
NATION_GEO_ID = "us:1"
#: The z-value of the Bureau's published 90 percent margins.
Z90 = Decimal("1.645")
#: A band whose coefficient of variation exceeds this is marked unreliable.
RELIABLE_CV_PERCENT = Decimal(30)
#: The most extra places one request draws beside the place and its levels.
MOST_COMPARE_GEOGRAPHIES = 3
#: Levels whose published totals carry a controlled margin (no MOE): the
#: Bureau controls state and national totals to its population estimates.
CONTROLLED_TOTAL_LEVELS = frozenset({"STATE", "NATIONAL"})
#: Geography vintages tracts are redrawn at; a cohort across one is refused.
TRACT_REDRAW_YEARS = (2010, 2020)

COHORT_CAVEAT = (
    "A change between two surveys mixes births abroad, deaths, moves and "
    "sampling error; it is not one group of people followed over time."
)

_ROW_COLUMNS = (
    "geo_id, geo_level, metric_code, vintage_year, observation_date, "
    "duration_start, duration_end, estimate_value, margin_of_error, value_status"
)

LATEST_QUERY = text(
    f"""
    SELECT {_ROW_COLUMNS}
    FROM {LATEST_RELATION}
    WHERE metric_code = ANY(:metric_codes)
      AND geo_id = ANY(:geo_ids)
    """
)

PINNED_QUERY = text(
    f"""
    SELECT {_ROW_COLUMNS}
    FROM {AS_RELEASED_RELATION}
    WHERE metric_code = ANY(:metric_codes)
      AND geo_id = ANY(:geo_ids)
      AND vintage_year = ANY(:vintages)
      AND observation_date >= :first_date
      AND observation_date < :after_date
    """
)

HISTORY_QUERY = text(
    f"""
    SELECT {_ROW_COLUMNS}
    FROM {AS_RELEASED_RELATION}
    WHERE metric_code = ANY(:metric_codes)
      AND geo_id = :geo_id
    """
)


class UnknownAgeGeography(LookupError):
    """A requested geography is not in the catalog; carries the parameter."""

    def __init__(self, parameter: str) -> None:
        super().__init__(parameter)
        self.parameter = parameter


class AgeStructureRequestError(ValueError):
    """A request the routes refuse with 422."""


# ---------------------------------------------------------------------------
# Numbers
# ---------------------------------------------------------------------------


def _number(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def _text(value: Optional[Decimal], quantum: str) -> Optional[str]:
    if value is None:
        return None
    return str(value.quantize(Decimal(quantum), rounding=ROUND_HALF_UP))


def _count_text(value: Optional[Decimal]) -> Optional[str]:
    return _text(value, "1")


def root_sum_squares(margins: Sequence[Optional[Decimal]]) -> Optional[Decimal]:
    """The Bureau's approximate margin of a sum: sqrt of the summed squares."""
    if any(margin is None for margin in margins):
        return None
    return sum((margin * margin for margin in margins), Decimal(0)).sqrt()


def ratio_margin(
    numerator: Decimal,
    numerator_moe: Decimal,
    denominator: Decimal,
    denominator_moe: Decimal,
) -> Decimal:
    """The Bureau's margin of ``numerator / denominator`` (unscaled)."""
    estimate = numerator / denominator
    margin, _ = derived_margin(
        estimate, numerator_moe, denominator, denominator_moe, subset=False
    )
    return margin


def proportion_margin(
    part: Decimal, part_moe: Decimal, whole: Decimal, whole_moe: Decimal
) -> Decimal:
    """The Bureau's margin of ``part / whole`` for a part inside the whole."""
    margin, _ = derived_margin(part / whole, part_moe, whole, whole_moe, subset=True)
    return margin


def coefficient_of_variation(
    count: Optional[Decimal], moe: Optional[Decimal]
) -> Optional[Decimal]:
    if count is None or moe is None or count <= 0:
        return None
    return moe / Z90 / count * 100


# ---------------------------------------------------------------------------
# One place in one release: lines in, pyramid out (pure)
# ---------------------------------------------------------------------------


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


def variable_of(metric_code: str) -> str:
    """``CENSUS_ACS:acs5:B01001_003`` → ``B01001_003`` (no trailing ``E``)."""
    variable = metric_code.rsplit(":", 1)[-1]
    return variable[:-1] if variable.endswith("E") else variable


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


def _total(scheme: AgeScheme, release: PlaceRelease, key: str) -> Summed:
    summed = sum_lines(release, [scheme.totals[key]])
    if (
        summed.count is not None
        and summed.moe is None
        and normalize_geo_level(release.geo_level) in CONTROLLED_TOTAL_LEVELS
    ):
        # A controlled total is published without a margin; the Bureau
        # instructs treating it as zero.
        return Summed(summed.count, Decimal(0))
    return summed


def share_of(summed: Summed, total: Summed) -> AgeShare:
    out = AgeShare(
        count=_count_text(summed.count),
        count_moe=_text(summed.moe, "0.01"),
        unavailable_reason=summed.reason,  # type: ignore[arg-type]
    )
    if summed.count is None:
        return out
    if total.count is None or total.count == 0:
        out.unavailable_reason = "total_missing"
        return out
    out.share = _text(summed.count / total.count * 100, "0.01")
    if summed.moe is not None and total.moe is not None:
        margin = proportion_margin(summed.count, summed.moe, total.count, total.moe)
        out.share_moe = _text(margin * 100, "0.01")
    return out


def _band_side(summed: Summed, total: Summed) -> AgeBandSide:
    share = share_of(summed, total)
    cv = coefficient_of_variation(summed.count, summed.moe)
    return AgeBandSide(
        **share.model_dump(),
        cv_percent=_text(cv, "0.1"),
        reliable=cv is not None and cv <= RELIABLE_CV_PERCENT,
    )


def _ratio(numerator: Summed, denominator: Summed, scale: int, unit: str) -> AgeRatio:
    reason = numerator.reason or denominator.reason
    if reason is not None or numerator.count is None or denominator.count is None:
        return AgeRatio(unit=unit, unavailable_reason=reason or "suppressed_input")  # type: ignore[arg-type]
    if denominator.count == 0:
        return AgeRatio(unit=unit, unavailable_reason="denominator_zero")
    value = numerator.count / denominator.count * scale
    if numerator.moe is None or denominator.moe is None:
        return AgeRatio(value=_text(value, "0.1"), unit=unit, moe_method="unavailable")
    margin = ratio_margin(
        numerator.count, numerator.moe, denominator.count, denominator.moe
    )
    return AgeRatio(
        value=_text(value, "0.1"),
        moe=_text(margin * scale, "0.1"),
        moe_method="ratio",
        unit=unit,
    )


def _group_sum(scheme: AgeScheme, release: PlaceRelease, group_id: str) -> Summed:
    if group_id in SEXES:
        return _total(scheme, release, group_id)
    group = scheme.group(group_id)
    return sum_lines(release, [*group.men, *group.women])


def indicators_for(scheme: AgeScheme, release: PlaceRelease) -> AgeIndicators:
    total = _total(scheme, release, "all")
    groups = {
        group.group_id: _group_sum(scheme, release, group.group_id)
        for group in scheme.groups
    }
    values: dict[str, Any] = {
        group_id: share_of(summed, total) for group_id, summed in groups.items()
    }
    for indicator in scheme.indicators:
        parts = [_group_sum(scheme, release, part) for part in indicator.numerator]
        if any(part.count is None for part in parts):
            numerator = next(part for part in parts if part.count is None)
        else:
            numerator = Summed(
                sum((part.count for part in parts), Decimal(0)),  # type: ignore[misc]
                root_sum_squares([part.moe for part in parts]),
            )
        denominator = _group_sum(scheme, release, indicator.denominator)
        values[indicator.indicator_id] = _ratio(
            numerator, denominator, indicator.scale, indicator.unit
        )
    return AgeIndicators(**values)


def _median(scheme: AgeScheme, release: PlaceRelease) -> MedianAge:
    def one(key: str) -> MedianAgeValue:
        line = release.lines.get(scheme.median_age[key])
        if line is None or line.suppressed:
            return MedianAgeValue()
        return MedianAgeValue(
            value=_text(line.estimate, "0.1"), moe=_text(line.moe, "0.1")
        )

    return MedianAge(all=one("all"), men=one("men"), women=one("women"))


def place_structure(
    scheme: AgeScheme,
    release: Optional[PlaceRelease],
    *,
    geo_id: str,
    geo_level: Optional[str],
    geo_name: Optional[str],
    role: str,
    reference_level: Optional[str] = None,
    overlap_weight: Optional[float] = None,
) -> AgeStructurePlace:
    """One place's block: bands, totals, the provider's medians, indicators."""
    base: dict[str, Any] = {
        "geo_id": geo_id,
        "geo_level": geo_level,
        "geo_name": geo_name,
        "role": role,
        "reference_level": reference_level,
        "overlap_weight": overlap_weight,
    }
    if release is None or not any(
        code in release.lines for code in scheme.totals.values()
    ):
        return AgeStructurePlace(**base, unavailable_reason="not_published")
    total = _total(scheme, release, "all")
    bands = [
        AgeBandResult(
            band_id=band.band_id,
            age_start=band.age_start,
            age_end=band.age_end,
            label=band.label,
            lines=AgeBandLines(men=list(band.men), women=list(band.women)),
            men=_band_side(sum_lines(release, band.men), total),
            women=_band_side(sum_lines(release, band.women), total),
        )
        for band in scheme.bands
    ]

    def count(summed: Summed) -> AgeCount:
        return AgeCount(
            count=_count_text(summed.count), count_moe=_text(summed.moe, "0.01")
        )

    return AgeStructurePlace(
        **base,
        release=str(release.vintage_year),
        period_start=release.period_start,
        period_end=release.period_end,
        total=count(total),
        men=count(_total(scheme, release, "men")),
        women=count(_total(scheme, release, "women")),
        bands=bands,
        median_age=_median(scheme, release),
        indicators=indicators_for(scheme, release),
    )


# ---------------------------------------------------------------------------
# Rows → releases
# ---------------------------------------------------------------------------


def _iso(value: Any) -> Optional[str]:
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
                period_start=_iso(start),
                period_end=_iso(row.get("duration_end")),
            )
        status = str(row.get("value_status") or "valid")
        estimate = _number(row.get("estimate_value"))
        release.lines[variable_of(str(row["metric_code"]))] = Line(
            estimate=estimate if status == "valid" else None,
            moe=None
            if row.get("margin_of_error") is None
            else abs(_number(row["margin_of_error"]) or Decimal(0)),
            suppressed=status != "valid" or estimate is None,
        )
    return out


def _newest(
    releases: Mapping[tuple[str, int], PlaceRelease], geo_id: str
) -> Optional[PlaceRelease]:
    found = [release for (place, _), release in releases.items() if place == geo_id]
    return max(found, key=lambda release: release.vintage_year) if found else None


# ---------------------------------------------------------------------------
# Enclosing levels (ADR-0013 rule 4)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Reference:
    geo_id: str
    level: str
    overlap_weight: Optional[float] = None


def _state_of(geo_id: str) -> Optional[str]:
    first = geo_id.split("|", 1)[0]
    return first if first.startswith("state:") else None


def _county_of(geo_id: str) -> Optional[str]:
    parts = geo_id.split("|")
    if (
        len(parts) >= 2
        and parts[0].startswith("state:")
        and parts[1].startswith("county:")
    ):
        return f"{parts[0]}|{parts[1]}"
    return None


def county_for_city(related_rows: Sequence[Mapping[str, Any]]) -> Optional[Reference]:
    """The city's county: the one it is part of, else its largest overlap."""
    counties = [
        row
        for row in related_rows
        if normalize_geo_level(str(row.get("geo_level") or "")) == "COUNTY"
        and row.get("relationship") in ("part_of", "intersects")
    ]
    if not counties:
        return None
    whole = [row for row in counties if row.get("relationship") == "part_of"]
    if whole:
        return Reference(str(whole[0]["geo_id"]), "COUNTY", 1.0)
    best = max(counties, key=lambda row: float(row.get("overlap_weight") or 0))
    weight = best.get("overlap_weight")
    return Reference(
        str(best["geo_id"]), "COUNTY", None if weight is None else float(weight)
    )


def single_state(related_rows: Sequence[Mapping[str, Any]]) -> Optional[str]:
    states = {
        str(row["geo_id"])
        for row in related_rows
        if normalize_geo_level(str(row.get("geo_level") or "")) == "STATE"
        and row.get("relationship") == "part_of"
    }
    return next(iter(states)) if len(states) == 1 else None


def references_for(
    geo_id: str, geo_level: str, related_rows: Sequence[Mapping[str, Any]] = ()
) -> list[Reference]:
    """Every enclosing level the toggle offers, next level up first."""
    level = normalize_geo_level(geo_level)
    nation = Reference(NATION_GEO_ID, "NATIONAL")
    if level == "NATIONAL" or geo_id == NATION_GEO_ID:
        return []
    state = _state_of(geo_id)
    out: list[Reference] = []
    if level == "TRACT":
        county = _county_of(geo_id)
        if county:
            out.append(Reference(county, "COUNTY"))
    elif level == "PLACE":
        county = county_for_city(related_rows)
        if county:
            out.append(county)
    elif level == "METRO":
        out.append(nation)
        only = single_state(related_rows)
        if only:
            out.append(Reference(only, "STATE"))
        return out
    if state and level != "STATE":
        out.append(Reference(state, "STATE"))
    out.append(nation)
    return out


def needs_related(geo_level: str) -> bool:
    return normalize_geo_level(geo_level) in ("PLACE", "METRO")


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------


_MEMO_KEY = "apps.api.age_structure"


def _identities(db: Session, geo_ids: Sequence[str]) -> dict[str, Mapping[str, Any]]:
    require_relation(db, GEOGRAPHY_RELATION)
    rows = (
        db.execute(GEOGRAPHY_IDENTITY_QUERY, {"geo_ids": list(geo_ids)})
        .mappings()
        .all()
    )
    return {str(row["geo_id"]): row for row in rows}


def _related(db: Session, geo_id: str) -> list[Mapping[str, Any]]:
    memo = session_memo(db, _MEMO_KEY)
    key = ("related", geo_id)
    if memo is not None and key in memo:
        return memo[key]
    require_relation(db, RELATIONSHIP_RELATION)
    rows = list(
        db.execute(GEOGRAPHY_RELATED_QUERY, {"geo_id": geo_id}).mappings().all()
    )
    if memo is not None:
        memo[key] = rows
    return rows


def _year_bounds(vintages: Sequence[int]) -> dict[str, Any]:
    return {
        "vintages": sorted(set(vintages)),
        "first_date": date(min(vintages), 1, 1),
        "after_date": date(max(vintages) + 1, 1, 1),
    }


def _read_pinned(
    db: Session, codes: Sequence[str], geo_ids: Sequence[str], vintages: Sequence[int]
) -> dict[tuple[str, int], PlaceRelease]:
    require_relation(db, AS_RELEASED_RELATION)
    rows = (
        db.execute(
            PINNED_QUERY,
            {
                "metric_codes": list(codes),
                "geo_ids": list(geo_ids),
                **_year_bounds(vintages),
            },
        )
        .mappings()
        .all()
    )
    return releases_from_rows(rows)


def _scheme_out(scheme: AgeScheme) -> AgeSchemeOut:
    return AgeSchemeOut(
        scheme_id=scheme.scheme_id, version=scheme.version, status=scheme.status
    )


def _check_dataset(scheme: AgeScheme, dataset: str) -> str:
    dataset = dataset.strip().lower()
    if dataset not in scheme.datasets:
        raise AgeStructureRequestError(
            f"dataset must be one of: {', '.join(scheme.datasets)}"
        )
    return dataset


def parse_release(release: Optional[str]) -> Optional[int]:
    if release is None or release.strip().lower() in ("", "latest"):
        return None
    try:
        year = int(release.strip())
    except ValueError as exc:
        raise AgeStructureRequestError("release must be 'latest' or a year") from exc
    if not 1990 <= year <= 2100:
        raise AgeStructureRequestError("release must be 'latest' or a year")
    return year


def age_structure(
    db: Session,
    geo_id: str,
    dataset: str = "acs5",
    release: Optional[str] = None,
    compare_geo_ids: Sequence[str] = (),
) -> AgeStructureResponse:
    scheme = default_scheme()
    dataset = _check_dataset(scheme, dataset)
    pinned = parse_release(release)
    compare = [
        code for code in dict.fromkeys(compare_geo_ids) if code and code != geo_id
    ]
    if len(compare) > MOST_COMPARE_GEOGRAPHIES:
        raise AgeStructureRequestError(
            f"name at most {MOST_COMPARE_GEOGRAPHIES} compare_geo_ids"
        )

    identity = _identities(db, [geo_id])
    if geo_id not in identity:
        raise UnknownAgeGeography("geo_id")
    place = identity[geo_id]
    level = str(place.get("geo_level") or "")
    related = _related(db, geo_id) if needs_related(level) else []
    references = [
        ref
        for ref in references_for(geo_id, level, related)
        if ref.geo_id not in compare
    ]

    everyone = [geo_id, *[ref.geo_id for ref in references], *compare]
    identities = (
        {**identity, **_identities(db, everyone[1:])} if len(everyone) > 1 else identity
    )
    for code in compare:
        if code not in identities:
            raise UnknownAgeGeography("compare_geo_ids")
    codes = scheme.metric_codes(dataset)

    if pinned is None:
        require_relation(db, LATEST_RELATION)
        rows = (
            db.execute(LATEST_QUERY, {"metric_codes": list(codes), "geo_ids": everyone})
            .mappings()
            .all()
        )
        releases = releases_from_rows(rows)
        own = _newest(releases, geo_id)
        vintage = own.vintage_year if own is not None else None
        if vintage is not None:
            # A level whose newest differs is read at the place's release from
            # the as-released relation; never shown at another release.
            stale = [g for g in everyone[1:] if (g, vintage) not in releases]
            if stale:
                releases.update(_read_pinned(db, codes, stale, [vintage]))
    else:
        vintage = pinned
        releases = _read_pinned(db, codes, everyone, [pinned])

    def block(
        code: str, role: str, ref: Optional[Reference] = None
    ) -> AgeStructurePlace:
        who = identities.get(code, {})
        return place_structure(
            scheme,
            None if vintage is None else releases.get((code, vintage)),
            geo_id=code,
            geo_level=who.get("geo_level") or (ref.level if ref else None),
            geo_name=who.get("geo_name"),
            role=role,
            reference_level=ref.level if ref else None,
            overlap_weight=ref.overlap_weight if ref else None,
        )

    places = [block(geo_id, "place")]
    places += [block(ref.geo_id, "reference", ref) for ref in references]
    places += [block(code, "compare") for code in compare]
    return AgeStructureResponse(
        geo_id=geo_id,
        geo_level=place.get("geo_level"),
        geo_name=place.get("geo_name"),
        dataset=dataset,
        scheme=_scheme_out(scheme),
        default_reference_geo_id=references[0].geo_id if references else None,
        places=places,
    )


def non_overlapping(releases: Sequence[PlaceRelease]) -> list[PlaceRelease]:
    """Newest first, then each release whose period ends before the last kept starts."""
    kept: list[PlaceRelease] = []
    for release in sorted(releases, key=lambda item: item.vintage_year, reverse=True):
        if not kept:
            kept.append(release)
            continue
        last = kept[-1]
        if _period_end_year(release) < _period_start_year(last):
            kept.append(release)
    return kept


def _period_start_year(release: PlaceRelease) -> int:
    return int((release.period_start or f"{release.vintage_year}")[:4])


def _period_end_year(release: PlaceRelease) -> int:
    return int((release.period_end or f"{release.vintage_year}")[:4])


def age_structure_history(
    db: Session,
    geo_id: str,
    dataset: str = "acs5",
    periods: str = "non_overlapping",
    include_bands: bool = False,
) -> AgeStructureHistoryResponse:
    scheme = default_scheme()
    dataset = _check_dataset(scheme, dataset)
    if periods not in ("non_overlapping", "all"):
        raise AgeStructureRequestError("periods must be 'non_overlapping' or 'all'")
    identity = _identities(db, [geo_id])
    if geo_id not in identity:
        raise UnknownAgeGeography("geo_id")
    place = identity[geo_id]
    require_relation(db, AS_RELEASED_RELATION)
    rows = (
        db.execute(
            HISTORY_QUERY,
            {"metric_codes": list(scheme.metric_codes(dataset)), "geo_id": geo_id},
        )
        .mappings()
        .all()
    )
    releases = list(releases_from_rows(rows).values())
    chosen = (
        non_overlapping(releases)
        if periods == "non_overlapping"
        else sorted(releases, key=lambda item: item.vintage_year, reverse=True)
    )
    items = []
    for release in chosen:
        block = place_structure(
            scheme,
            release,
            geo_id=geo_id,
            geo_level=place.get("geo_level"),
            geo_name=place.get("geo_name"),
            role="place",
        )
        if not include_bands:
            block.bands = []
        items.append(block)
    return AgeStructureHistoryResponse(
        geo_id=geo_id,
        geo_level=place.get("geo_level"),
        geo_name=place.get("geo_name"),
        dataset=dataset,
        scheme=_scheme_out(scheme),
        periods=periods,  # type: ignore[arg-type]
        items=items,
    )


def cohort_refusal(
    geo_level: str, earlier: PlaceRelease, later: PlaceRelease
) -> Optional[CohortRefusal]:
    if _period_end_year(earlier) >= _period_start_year(later):
        return CohortRefusal(
            reason="overlapping_periods",
            detail=(
                "These two releases share survey years, so most of their "
                "respondents are the same; compare releases whose periods do "
                "not overlap."
            ),
        )
    if _period_end_year(later) - _period_end_year(earlier) != 5:
        return CohortRefusal(
            reason="period_gap_not_five_years",
            detail="An age group moves up one five-year band only when the periods end five years apart.",
        )
    if normalize_geo_level(geo_level) == "TRACT" and any(
        earlier.vintage_year < year <= later.vintage_year for year in TRACT_REDRAW_YEARS
    ):
        return CohortRefusal(
            reason="boundary_changed",
            detail="Census tracts were redrawn between these releases, so the area is not the same.",
        )
    return None


def _overlap(a: Summed, b: Summed) -> Optional[bool]:
    if None in (a.count, a.moe, b.count, b.moe):
        return None
    return a.count - a.moe <= b.count + b.moe and b.count - b.moe <= a.count + a.moe  # type: ignore[operator]


def cohort_items(
    scheme: AgeScheme, earlier: PlaceRelease, later: PlaceRelease
) -> list[CohortItem]:
    items = []
    for band in scheme.bands:
        following = scheme.next_band(band.band_id)
        if following is None:
            continue
        sides = {}
        for sex in SEXES:
            before = sum_lines(earlier, band.lines(sex))
            after = sum_lines(later, following.lines(sex))
            change = (
                None
                if before.count is None or after.count is None
                else after.count - before.count
            )
            sides[sex] = CohortSide(
                from_count=_count_text(before.count),
                from_count_moe=_text(before.moe, "0.01"),
                to_count=_count_text(after.count),
                to_count_moe=_text(after.moe, "0.01"),
                change=_count_text(change),
                change_moe=None
                if change is None
                else _text(root_sum_squares([before.moe, after.moe]), "0.01"),
                intervals_overlap=_overlap(before, after),
            )
        items.append(
            CohortItem(
                from_band=band.band_id,
                to_band=following.band_id,
                men=sides["men"],
                women=sides["women"],
            )
        )
    return items


def age_cohorts(
    db: Session,
    geo_id: str,
    from_release: str,
    to_release: str,
    dataset: str = "acs5",
) -> AgeCohortsResponse:
    scheme = default_scheme()
    dataset = _check_dataset(scheme, dataset)
    first = parse_release(from_release)
    last = parse_release(to_release)
    if first is None or last is None or first >= last:
        raise AgeStructureRequestError(
            "from_release and to_release must be years, from_release the earlier"
        )
    identity = _identities(db, [geo_id])
    if geo_id not in identity:
        raise UnknownAgeGeography("geo_id")
    place = identity[geo_id]
    level = str(place.get("geo_level") or "")
    releases = _read_pinned(db, scheme.metric_codes(dataset), [geo_id], [first, last])
    earlier = releases.get((geo_id, first))
    later = releases.get((geo_id, last))

    def period(release: Optional[PlaceRelease], year: int) -> CohortPeriod:
        if release is None:
            return CohortPeriod(release=str(year))
        return CohortPeriod(
            release=str(year),
            period_start=release.period_start,
            period_end=release.period_end,
        )

    base: dict[str, Any] = {
        "geo_id": geo_id,
        "geo_level": place.get("geo_level"),
        "geo_name": place.get("geo_name"),
        "dataset": dataset,
        "scheme": _scheme_out(scheme),
        "from": period(earlier, first),
        "to": period(later, last),
        "caveat": COHORT_CAVEAT,
    }
    if earlier is None or later is None:
        missing = first if earlier is None else last
        return AgeCohortsResponse(
            **base,
            comparable=False,
            refusal=CohortRefusal(
                reason="not_published",
                detail=f"There is no {dataset} release {missing} for this place.",
            ),
        )
    base["years_apart"] = _period_end_year(later) - _period_end_year(earlier)
    refusal = cohort_refusal(level, earlier, later)
    if refusal is not None:
        return AgeCohortsResponse(**base, comparable=False, refusal=refusal)
    return AgeCohortsResponse(
        **base, comparable=True, items=cohort_items(scheme, earlier, later)
    )
