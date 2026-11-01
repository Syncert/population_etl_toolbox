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

from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
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
    AgeMapItem,
    AgeMapResponse,
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
from apps.api.services.cross_level_service import place_name
from data_ingestion_toolbox.semantics.age_bands import (
    SEXES,
    AgeScheme,
    default_scheme,
)
from data_ingestion_toolbox.semantics.age_structure import (
    CONTROLLED_TOTAL_LEVELS,
    RELIABLE_CV_PERCENT,
    Z90,
    Line,
    PlaceRelease,
    RatioValue,
    ShareValue,
    Summed,
    coefficient_of_variation,
    group_sum,
    indicator_values,
    iso_text,
    proportion_margin,
    releases_from_rows,
    ratio_margin,
    ratio_value,
    root_sum_squares,
    share_value,
    sum_lines,
    to_number,
    total_of,
    variable_of,
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
#: The most extra places one request draws beside the place and its levels.
MOST_COMPARE_GEOGRAPHIES = 3
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


_number = to_number

#: Re-exported for callers and tests that reach them through this module.
__all__ = [
    "CONTROLLED_TOTAL_LEVELS",
    "RELIABLE_CV_PERCENT",
    "Z90",
    "Line",
    "PlaceRelease",
    "Summed",
    "coefficient_of_variation",
    "proportion_margin",
    "releases_from_rows",
    "ratio_margin",
    "root_sum_squares",
    "sum_lines",
    "variable_of",
]


def _text(value: Optional[Decimal], quantum: str) -> Optional[str]:
    if value is None:
        return None
    return str(value.quantize(Decimal(quantum), rounding=ROUND_HALF_UP))


def _count_text(value: Optional[Decimal]) -> Optional[str]:
    return _text(value, "1")


# ---------------------------------------------------------------------------
# One place in one release: numbers from the shared arithmetic, as strings
# ---------------------------------------------------------------------------


def _share_out(value: ShareValue) -> AgeShare:
    return AgeShare(
        count=_count_text(value.count),
        count_moe=_text(value.count_moe, "0.01"),
        share=_text(value.share, "0.01"),
        share_moe=_text(value.share_moe, "0.01"),
        unavailable_reason=value.reason,  # type: ignore[arg-type]
    )


def share_of(summed: Summed, total: Summed) -> AgeShare:
    return _share_out(share_value(summed, total))


def _band_side(summed: Summed, total: Summed) -> AgeBandSide:
    share = share_of(summed, total)
    cv = coefficient_of_variation(summed.count, summed.moe)
    return AgeBandSide(
        **share.model_dump(),
        cv_percent=_text(cv, "0.1"),
        reliable=cv is not None and cv <= RELIABLE_CV_PERCENT,
    )


def _ratio_out(value: RatioValue, unit: str) -> AgeRatio:
    return AgeRatio(
        value=_text(value.value, "0.1"),
        moe=_text(value.moe, "0.1"),
        moe_method=value.moe_method,  # type: ignore[arg-type]
        unit=unit,
        unavailable_reason=value.reason,  # type: ignore[arg-type]
    )


def _ratio(numerator: Summed, denominator: Summed, scale: int, unit: str) -> AgeRatio:
    return _ratio_out(ratio_value(numerator, denominator, scale), unit)


def _total(scheme: AgeScheme, release: PlaceRelease, key: str) -> Summed:
    return total_of(scheme, release, key)


def _group_sum(scheme: AgeScheme, release: PlaceRelease, group_id: str) -> Summed:
    return group_sum(scheme, release, group_id)


def indicators_for(scheme: AgeScheme, release: PlaceRelease) -> AgeIndicators:
    values = indicator_values(scheme, release)
    units = {indicator.indicator_id: indicator.unit for indicator in scheme.indicators}
    out: dict[str, Any] = {}
    for key, value in values.items():
        if isinstance(value, ShareValue):
            out[key] = _share_out(value)
        else:
            out[key] = _ratio_out(value, units[key])
    return AgeIndicators(**out)


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
    # The catalog's nation row carries its code (``us:1``) as its name; serve
    # the name a reader knows it by, as the cross-level comparison does.
    return {str(row["geo_id"]): {**row, "geo_name": place_name(row)} for row in rows}


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


# ---------------------------------------------------------------------------
# One indicator for every place of a grain (the summary table)
# ---------------------------------------------------------------------------

SUMMARY_RELATION = "gold_census.age_structure_summary"
#: Grains a map of one indicator is drawn at; cities and tracts one state at
#: a time, so a read is never every tract in the country.
MAP_GRAINS = ("STATE", "METRO", "COUNTY", "PLACE", "TRACT")
STATE_SCOPED_GRAINS = frozenset({"PLACE", "TRACT"})
_SHARE_GROUPS = ("under_18", "age_18_64", "age_65_plus", "age_85_plus")
_RATIOS = (
    "child_dependency_ratio",
    "old_age_dependency_ratio",
    "total_dependency_ratio",
    "sex_ratio",
)
_MEDIANS = {
    "median_age": "median_age",
    "median_age_men": "median_age_men",
    "median_age_women": "median_age_women",
}

#: indicator -> (value column, margin column, reason column or None). Column
#: names come from this table only, never from the request.
MAP_COLUMNS: dict[str, tuple[str, str, Optional[str]]] = {
    **{
        group: (
            f"{group}_share",
            f"{group}_share_moe",
            f"{group}_unavailable_reason",
        )
        for group in _SHARE_GROUPS
    },
    **{
        ratio: (ratio, f"{ratio}_moe", f"{ratio}_unavailable_reason")
        for ratio in _RATIOS
    },
    **{name: (column, f"{column}_moe", None) for name, column in _MEDIANS.items()},
}

NEWEST_SUMMARY_RELEASE_QUERY = text(
    f"""
    SELECT MAX(vintage_year) AS vintage_year
    FROM {SUMMARY_RELATION}
    WHERE dataset_code = :dataset AND geo_level = :geo_level
      AND (CAST(:state_fips AS TEXT) IS NULL OR state_fips = :state_fips)
    """
)


def _map_query(indicator: str):
    value, moe, reason = MAP_COLUMNS[indicator]
    reason_sql = reason or "NULL::TEXT"
    return text(
        f"""
        SELECT geo_id, period_start, period_end,
               {value} AS value, {moe} AS moe, {reason_sql} AS unavailable_reason
        FROM {SUMMARY_RELATION}
        WHERE dataset_code = :dataset AND vintage_year = :vintage_year
          AND geo_level = :geo_level
          AND (CAST(:state_fips AS TEXT) IS NULL OR state_fips = :state_fips)
        ORDER BY geo_id
        """
    )


def _indicator_label(scheme: AgeScheme, indicator: str) -> tuple[str, str]:
    for group in scheme.groups:
        if group.group_id == indicator:
            return f"{group.label} (share of everyone)", "percent"
    for item in scheme.indicators:
        if item.indicator_id == indicator:
            return item.label, item.unit
    words = {"median_age": "Median age", "median_age_men": "Median age, men"}
    return words.get(indicator, "Median age, women"), "years"


def age_structure_map(
    db: Session,
    indicator: str,
    geo_level: str,
    state_fips: Optional[str],
    dataset: str,
    release: Optional[str],
) -> AgeMapResponse:
    scheme = default_scheme()
    dataset = _check_dataset(scheme, dataset)
    if indicator not in MAP_COLUMNS:
        raise AgeStructureRequestError(
            f"indicator must be one of {', '.join(MAP_COLUMNS)}"
        )
    level = normalize_geo_level(geo_level)
    if level not in MAP_GRAINS:
        raise AgeStructureRequestError(
            f"geo_level must be one of {', '.join(MAP_GRAINS)}"
        )
    if state_fips is not None and not (len(state_fips) == 2 and state_fips.isdigit()):
        raise AgeStructureRequestError("state_fips must be two digits")
    if level in STATE_SCOPED_GRAINS and state_fips is None:
        raise AgeStructureRequestError(
            f"a {level.lower()} map is drawn one state at a time: send state_fips"
        )
    require_relation(db, SUMMARY_RELATION)
    scope = {"dataset": dataset, "geo_level": level, "state_fips": state_fips}
    vintage = parse_release(release)
    if vintage is None:
        newest = db.execute(NEWEST_SUMMARY_RELEASE_QUERY, scope).scalar()
        vintage = None if newest is None else int(newest)
    label, unit = _indicator_label(scheme, indicator)
    out = AgeMapResponse(
        indicator=indicator,
        label=label,
        unit=unit,
        dataset=dataset,
        geo_level=level,
        state_fips=state_fips,
        release=None if vintage is None else str(vintage),
        scheme=_scheme_out(scheme),
    )
    if vintage is None:
        return out
    rows = (
        db.execute(_map_query(indicator), {**scope, "vintage_year": vintage})
        .mappings()
        .all()
    )
    quantum = "0.01" if unit == "percent" else "0.1"
    for row in rows:
        out.period_start = out.period_start or iso_text(row.get("period_start"))
        out.period_end = out.period_end or iso_text(row.get("period_end"))
        out.items.append(
            AgeMapItem(
                geo_id=str(row["geo_id"]),
                value=_text(_number(row.get("value")), quantum),
                moe=_text(_number(row.get("moe")), quantum),
                unavailable_reason=row.get("unavailable_reason"),
            )
        )
    return out
