"""One measure, two places, at any geographic level (ADR-0010, A-4).

``/comparison`` aligns two measures across every geography of one grain.
This answers the reader's other question -- one measure, two named places --
and the places may sit at different levels: a city and its county, a county
and its state, a metro and the nation.

The rules, in the order the ADR states them:

1. Mixed grains are accepted.
2. Whether the measure compares across levels is its **kind**, read from its
   definition (``docs/semantics/metric_definitions.json``). A median, mean,
   rate, share or index compares; a count does not, because a county has
   more of everything than a city inside it. A count across levels is
   refused with ``counts_differ_in_size`` and the served ADR-0009 pairings
   for that count, which do compare. A metric with no declared kind is a
   count.
3. One program and one period, or said otherwise. A different metric code on
   side b is refused unless it is the same measure from the same program;
   each side's period is the newest both publish, or each side's own newest
   named with ``periods_differ``. A side with no value is "not published",
   never zero.
4. Margins of error decide "different": the answer says whether the two 90
   percent intervals overlap, and prints no verdict.
5. Containment is disclosed from the reference's relationships, and no
   difference is computed between a place and its container.
6. No verdicts: values, periods, intervals and reasons only.

The rows are read through ``observations_for_geographies``, the same partner
read ``/observations/derived`` uses, so the metric's own dispatch, relation
and code conditions apply unchanged, and the geographies are bound
parameters.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional, Sequence

from sqlalchemy.orm import Session

from apps.api.registry import normalize_geo_level
from apps.api.schemas.cross_level import (
    ComparisonAlternative,
    PlacesComparisonResponse,
    PlacesContainment,
    PlacesNote,
    PlacesRefusal,
    PlacesSide,
)
from apps.api.schemas.labels import label_fields
from apps.api.services.comparison_service import _resolved
from apps.api.services.contracts import require_relation
from apps.api.services.derived_ratio_service import UnknownRatio, pair_rows
from apps.api.services.neutral_observations_service import (
    NeutralQueryError,
    observations_for_geographies,
)
from data_ingestion_toolbox.semantics.definitions import definition_for, measure_kind
from data_ingestion_toolbox.semantics.derived_ratios import (
    RatioPairing,
    pairing_for,
    pairings_for_numerator,
)
from data_ingestion_toolbox.sql.catalog_queries import (
    GEOGRAPHY_IDENTITY_QUERY,
    GEOGRAPHY_RELATED_QUERY,
    GEOGRAPHY_RELATION,
    RELATIONSHIP_RELATION,
)

SCOPE = "latest"
#: Kinds that describe a population whatever its size (ADR-0010 rule 2).
COMPARABLE_KINDS = frozenset({"median", "mean", "rate", "share", "index"})
#: An ADR-0009 pairing's kind, as a measure kind.
RATIO_KINDS = {"share": "share", "per_capita": "rate", "provider_rate": "rate"}
VALID_STATUSES = frozenset({None, "", "valid"})

#: Finest to coarsest, for reading containment from the finer side, whose
#: relationships reach upward to its grandparent.
_LEVEL_RANK = {
    "TRACT": 0,
    "PLACE": 1,
    "COUNTY": 2,
    "METRO": 3,
    "STATE": 4,
    "NATIONAL": 5,
}
_FLIP = {"contains": "part_of", "part_of": "contains"}

INTERVAL_DERIVATIONS = ("interval_low", "interval_high", "intervals_overlap")


class UnknownGeography(LookupError):
    """A requested geography is not in the catalog; carries the parameter."""

    def __init__(self, parameter: str) -> None:
        super().__init__(parameter)
        self.parameter = parameter


# ---------------------------------------------------------------------------
# Pure pieces
# ---------------------------------------------------------------------------


def _number(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def _period_key(candidate: Mapping[str, Any]) -> tuple[str, str]:
    return (candidate.get("period_start") or "", candidate.get("period_end") or "")


def _recency(key: tuple[str, str]) -> tuple[str, str]:
    """Newest by the period's end, then its start (a 5-year window ends late)."""
    return (key[1], key[0])


def by_period(candidates: Sequence[Mapping[str, Any]]) -> dict[tuple, Mapping]:
    """One candidate per period: the first in the relation's own served order."""
    periods: dict[tuple, Mapping] = {}
    for candidate in candidates:
        periods.setdefault(_period_key(candidate), candidate)
    return periods


def choose_periods(
    side_a: Mapping[tuple, Mapping],
    side_b: Mapping[tuple, Mapping],
    period_start: Optional[str],
) -> tuple[Optional[Mapping], Optional[Mapping], str]:
    """Each side's row and how it was chosen (ADR-0010 rule 3).

    A requested period is that period on both sides. Otherwise the newest
    period both sides publish; failing that, each side's own newest, which
    the caller names as ``periods_differ``. Nothing is aligned silently.
    """
    if period_start is not None:

        def pick(periods: Mapping[tuple, Mapping]) -> Optional[Mapping]:
            # A year names every period starting in it; a date names one.
            keys = [
                key
                for key in periods
                if key[0] == period_start or key[0].startswith(f"{period_start}-")
            ]
            return periods[max(keys, key=_recency)] if keys else None

        return pick(side_a), pick(side_b), "requested"
    common = set(side_a) & set(side_b)
    if common:
        key = max(common, key=_recency)
        return side_a[key], side_b[key], "newest_common"

    def newest(periods: Mapping[tuple, Mapping]) -> Optional[Mapping]:
        return periods[max(periods, key=_recency)] if periods else None

    return newest(side_a), newest(side_b), "each_newest"


def interval(candidate: Optional[Mapping]) -> Optional[tuple[Decimal, Decimal]]:
    """The 90 percent interval, or ``None`` without a value and a margin."""
    if candidate is None or candidate.get("value_status") not in VALID_STATUSES:
        return None
    value = _number(candidate.get("value"))
    margin = _number(candidate.get("margin_of_error"))
    if value is None or margin is None:
        return None
    return value - abs(margin), value + abs(margin)


def intervals_overlap(
    a: Optional[tuple[Decimal, Decimal]], b: Optional[tuple[Decimal, Decimal]]
) -> Optional[bool]:
    if a is None or b is None:
        return None
    return a[0] <= b[1] and b[0] <= a[1]


def usable_value(candidate: Optional[Mapping]) -> Optional[Decimal]:
    if candidate is None or candidate.get("value_status") not in VALID_STATUSES:
        return None
    return _number(candidate.get("value"))


def place_name(geography: Mapping[str, Any]) -> str:
    """The name a reader knows a place by; the nation is "United States".

    The catalog's nation row carries its code (``us:1``) as its name, which
    is no name to print beside a figure.
    """
    geo_id = str(geography.get("geo_id") or "")
    name = geography.get("geo_name")
    level = normalize_geo_level(str(geography.get("geo_level") or ""))
    if level == "NATIONAL" and (not name or name == geo_id):
        return "United States"
    return str(name or geo_id)


def unpublished_refusal(
    missing: Sequence[tuple[str, Mapping[str, Any], Optional[Mapping[str, Any]]]],
) -> PlacesRefusal:
    """Why a comparison with a side that has no value is not made.

    A side with no value is not published, never zero, and two places are not
    compared when one of them has nothing to compare (ADR-0010 rule 3).
    """
    parts = []
    for _label, geography, chosen in missing:
        name = place_name(geography)
        if chosen is None:
            parts.append(f"there is no published value for {name} in this series")
        else:
            status = chosen.get("value_status") or "no number"
            parts.append(f"the value for {name} is withheld or missing ({status})")
    return PlacesRefusal(
        reason="not_published",
        detail="; ".join(parts) + ", so there is nothing to compare it with",
    )


def containment_between(
    geography_a: Mapping[str, Any],
    geography_b: Mapping[str, Any],
    related_rows: Sequence[Mapping[str, Any]],
    rows_from: str,
) -> PlacesContainment:
    """How b relates to a, from the reference's relationships.

    ``related_rows`` are the finer side's ``/related`` rows and ``rows_from``
    says which side that is. The nation contains every served geography; the
    reference records no edge to it below the grandparent, so it is answered
    by level rather than looked for.
    """
    level_a = normalize_geo_level(str(geography_a.get("geo_level") or ""))
    level_b = normalize_geo_level(str(geography_b.get("geo_level") or ""))
    if level_a == "NATIONAL" or level_b == "NATIONAL":
        a_is_nation = level_a == "NATIONAL"
        return PlacesContainment(
            relationship="contains" if a_is_nation else "part_of",
            smaller_side="b" if a_is_nation else "a",
            share_of_smaller_inside_larger=1.0,
            evidence_source="nation",
        )
    other = geography_b if rows_from == "a" else geography_a
    match = next(
        (row for row in related_rows if row.get("geo_id") == other.get("geo_id")),
        None,
    )
    if match is None or match.get("relationship") not in (
        "contains",
        "part_of",
        "intersects",
        "adjacent",
    ):
        return PlacesContainment(relationship="none_recorded")
    relationship = str(match["relationship"])
    if rows_from == "b":
        relationship = _FLIP.get(relationship, relationship)
    smaller: Optional[str] = None
    share: Optional[float] = None
    if relationship == "contains":
        smaller, share = "b", 1.0
    elif relationship == "part_of":
        smaller, share = "a", 1.0
    elif relationship == "intersects":
        rank_a = _LEVEL_RANK.get(level_a, 99)
        rank_b = _LEVEL_RANK.get(level_b, 99)
        smaller = "a" if rank_a <= rank_b else "b"
        weight = match.get("overlap_weight")
        share = None if weight is None else float(weight)
    return PlacesContainment(
        relationship=relationship,
        smaller_side=smaller,
        share_of_smaller_inside_larger=share,
        geography_vintage=match.get("geography_vintage"),
        evidence_source=match.get("evidence_source"),
    )


def count_alternatives(metric_code: str) -> list[ComparisonAlternative]:
    """The served per-capita and share views of a count (ADR-0009)."""
    return [
        ComparisonAlternative(
            ratio_id=pairing.ratio_id,
            label=pairing.label,
            unit=pairing.unit,
            href=f"/api/v1/comparison/places?ratio_id={pairing.ratio_id}",
        )
        for pairing in pairings_for_numerator(metric_code)
        if pairing.is_served()
    ]


def code_refusal(code_a: str, code_b: str) -> Optional[PlacesRefusal]:
    """Why a different code on side b cannot be compared, or ``None``.

    Two codes of one definition are one measure. They may still come from
    two programs -- the ACS 5-year and 1-year estimates describe different
    periods and populations of different minimum size -- and are refused
    then, pointing at the code both places publish.
    """
    if code_a == code_b:
        return None
    definition_a = definition_for(code_a)
    definition_b = definition_for(code_b)
    program_a = code_a.split(":")[:2]
    program_b = code_b.split(":")[:2]
    if (
        definition_a is not None
        and definition_b is not None
        and definition_a.metric_id == definition_b.metric_id
    ):
        if program_a != program_b:
            return PlacesRefusal(
                reason="programs_differ",
                detail=(
                    f"{code_a} and {code_b} are one measure from two programs "
                    f"({':'.join(program_a)} and {':'.join(program_b)}), which "
                    "describe different periods; compare both places on one "
                    "code (the ACS 5-year estimates cover every place)"
                ),
            )
        return None
    universe_a = definition_a.universe if definition_a else None
    universe_b = definition_b.universe if definition_b else None
    if universe_a and universe_b and universe_a != universe_b:
        return PlacesRefusal(
            reason="universes_differ",
            detail=(
                f"{code_a} counts {universe_a} and {code_b} counts "
                f"{universe_b}; values over different universes are not one "
                "measure in two places"
            ),
        )
    unit_a = definition_a.unit if definition_a else None
    unit_b = definition_b.unit if definition_b else None
    if unit_a and unit_b and unit_a != unit_b:
        return PlacesRefusal(
            reason="units_differ",
            detail=f"{code_a} is in {unit_a} and {code_b} in {unit_b}",
        )
    if program_a[0] != program_b[0]:
        return PlacesRefusal(
            reason="programs_differ",
            detail=(
                f"{code_a} and {code_b} come from two programs, whose methods "
                "and periods differ; compare both places on one code"
            ),
        )
    return PlacesRefusal(
        reason="different_measures",
        detail=(
            f"{code_a} and {code_b} are different measures; this route compares "
            "one measure in two places, /comparison compares two measures"
        ),
    )


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------


def _geographies(db: Session, geo_id_a: str, geo_id_b: str) -> tuple[Mapping, Mapping]:
    require_relation(db, GEOGRAPHY_RELATION)
    rows = {
        row["geo_id"]: row
        for row in db.execute(
            GEOGRAPHY_IDENTITY_QUERY, {"geo_ids": [geo_id_a, geo_id_b]}
        )
        .mappings()
        .all()
    }
    for parameter, geo_id in (("geo_id_a", geo_id_a), ("geo_id_b", geo_id_b)):
        if geo_id not in rows:
            raise UnknownGeography(parameter)
    return rows[geo_id_a], rows[geo_id_b]


def _containment(db: Session, geography_a: Mapping, geography_b: Mapping):
    level_a = normalize_geo_level(str(geography_a.get("geo_level") or ""))
    level_b = normalize_geo_level(str(geography_b.get("geo_level") or ""))
    if "NATIONAL" in (level_a, level_b):
        return containment_between(geography_a, geography_b, [], "a")
    rows_from = (
        "a" if _LEVEL_RANK.get(level_a, 99) <= _LEVEL_RANK.get(level_b, 99) else "b"
    )
    finer = geography_a if rows_from == "a" else geography_b
    require_relation(db, RELATIONSHIP_RELATION)
    rows = (
        db.execute(GEOGRAPHY_RELATED_QUERY, {"geo_id": finer["geo_id"]})
        .mappings()
        .all()
    )
    return containment_between(geography_a, geography_b, rows, rows_from)


def _observation_candidate(row: Any) -> dict[str, Any]:
    uncertainty = row.uncertainty
    return {
        "geo_id": row.geo_id,
        "metric_code": row.metric_code,
        "period_start": row.period_start,
        "period_end": row.period_end,
        "release": row.release,
        "value": row.value,
        "value_status": row.value_status,
        "margin_of_error": None if uncertainty is None else uncertainty.margin_of_error,
    }


def _derived_candidate(row: Any, metric_code: str) -> dict[str, Any]:
    return {
        "geo_id": row.geo_id,
        "metric_code": metric_code,
        "period_start": row.period_start,
        "period_end": row.period_end,
        "release": row.release,
        "value": row.value,
        "value_status": row.unavailable_reason,
        "margin_of_error": row.margin_of_error,
    }


def _read_metric(
    db: Session, metric_code: str, geo_ids: list[str]
) -> list[dict[str, Any]]:
    rows = observations_for_geographies(db, metric_code, SCOPE, None, geo_ids)
    return [_observation_candidate(row) for row in rows or []]


def _read_ratio(
    db: Session, pairing: RatioPairing, geo_ids: list[str]
) -> list[dict[str, Any]]:
    if pairing.kind == "provider_rate":
        return [
            {**candidate, "metric_code": pairing.ratio_id}
            for candidate in _read_metric(db, str(pairing.provider_rate), geo_ids)
        ]
    numerators = observations_for_geographies(
        db, pairing.numerator, SCOPE, None, geo_ids
    )
    denominators = observations_for_geographies(
        db, str(pairing.denominator), SCOPE, None, geo_ids
    )
    if numerators is None or denominators is None:
        raise NeutralQueryError(
            f"pairing '{pairing.ratio_id}' names a metric the catalog does not publish"
        )
    return [
        _derived_candidate(row, pairing.ratio_id)
        for row in pair_rows(pairing, numerators, denominators)
    ]


def _side(
    geography: Mapping[str, Any],
    metric_code: str,
    metric: Optional[Mapping[str, Any]],
    candidate: Optional[Mapping[str, Any]],
    read: bool,
) -> PlacesSide:
    labels = label_fields(metric_code, metric) if metric is not None else {}
    bounds = interval(candidate)
    return PlacesSide(
        geo_id=str(geography["geo_id"]),
        geo_level=geography.get("geo_level"),
        geo_name=place_name(geography),
        metric_code=metric_code,
        **labels,
        published=(candidate is not None) if read else None,
        period_start=None if candidate is None else candidate.get("period_start"),
        period_end=None if candidate is None else candidate.get("period_end"),
        release=None if candidate is None else candidate.get("release"),
        value=None if candidate is None else candidate.get("value"),
        value_status=None if candidate is None else candidate.get("value_status"),
        margin_of_error=(
            None if candidate is None else candidate.get("margin_of_error")
        ),
        interval_low=None if bounds is None else str(bounds[0]),
        interval_high=None if bounds is None else str(bounds[1]),
    )


def compare_places(
    db: Session,
    geo_id_a: str,
    geo_id_b: str,
    metric_code: Optional[str] = None,
    ratio_id: Optional[str] = None,
    metric_code_b: Optional[str] = None,
    period_start: Optional[str] = None,
) -> PlacesComparisonResponse:
    if (metric_code is None) == (ratio_id is None):
        raise NeutralQueryError("give exactly one of metric_code or ratio_id")
    if metric_code_b is not None and ratio_id is not None:
        raise NeutralQueryError("metric_code_b compares two codes, not a ratio")
    if geo_id_a == geo_id_b:
        raise NeutralQueryError("geo_id_a and geo_id_b name the same place")

    code_a: str
    code_b: str
    metric_a: Optional[Mapping[str, Any]] = None
    metric_b: Optional[Mapping[str, Any]] = None
    pairing: Optional[RatioPairing] = None
    if ratio_id is not None:
        pairing = pairing_for(ratio_id)
        if pairing is None:
            raise UnknownRatio(ratio_id)
        refusal_text = pairing.refusal()
        if refusal_text is not None:
            raise NeutralQueryError(refusal_text)
        code_a = code_b = pairing.ratio_id
        kind, kind_status = RATIO_KINDS[pairing.kind], pairing.status
    else:
        code_a = str(metric_code)
        code_b = metric_code_b or code_a
        metric_a = _resolved(db, code_a, "metric_code")
        metric_b = (
            metric_a if code_b == code_a else _resolved(db, code_b, "metric_code_b")
        )
        kind, kind_status = measure_kind(code_a)

    geography_a, geography_b = _geographies(db, geo_id_a, geo_id_b)
    level_a = normalize_geo_level(str(geography_a.get("geo_level") or ""))
    level_b = normalize_geo_level(str(geography_b.get("geo_level") or ""))
    cross_level = level_a != level_b
    containment = _containment(db, geography_a, geography_b)

    refusal: Optional[PlacesRefusal] = None
    if pairing is None:
        refusal = code_refusal(code_a, code_b)
        if refusal is None and cross_level and kind not in COMPARABLE_KINDS:
            alternatives = count_alternatives(code_a)
            steer = (
                "compare its per-capita or share view instead, listed in alternatives"
                if alternatives
                else "no reviewed per-capita or share view of it is served yet "
                "(docs/semantics/derived_ratios.json lists the pairings)"
            )
            refusal = PlacesRefusal(
                reason="counts_differ_in_size",
                detail=(
                    f"{code_a} is a count, and a {level_a.lower()} and a "
                    f"{level_b.lower()} differ in size by construction, so the "
                    f"larger count says nothing about either place; {steer}"
                ),
                alternatives=alternatives,
            )

    response: dict[str, Any] = {
        "metric_code": metric_code,
        "metric_code_b": metric_code_b,
        "ratio_id": ratio_id,
        "measure_kind": kind,
        "measure_kind_status": kind_status,
        "cross_level": cross_level,
        "containment": containment,
        "derived": bool(pairing is not None and pairing.derived),
    }
    if refusal is not None:
        return PlacesComparisonResponse(
            **response,
            comparable=False,
            refusal=refusal,
            side_a=_side(geography_a, code_a, metric_a, None, read=False),
            side_b=_side(geography_b, code_b, metric_b, None, read=False),
            derivations=[],
            notes=[],
            caveats=[],
        )

    geo_a, geo_b = str(geography_a["geo_id"]), str(geography_b["geo_id"])
    if pairing is not None:
        rows = _read_ratio(db, pairing, [geo_a, geo_b])
        rows_a = [row for row in rows if row["geo_id"] == geo_a]
        rows_b = [row for row in rows if row["geo_id"] == geo_b]
    elif code_a == code_b:
        rows = _read_metric(db, code_a, [geo_a, geo_b])
        rows_a = [row for row in rows if row["geo_id"] == geo_a]
        rows_b = [row for row in rows if row["geo_id"] == geo_b]
    else:
        rows_a = _read_metric(db, code_a, [geo_a])
        rows_b = _read_metric(db, code_b, [geo_b])

    chosen_a, chosen_b, basis = choose_periods(
        by_period(rows_a), by_period(rows_b), period_start
    )
    notes: list[PlacesNote] = []
    derivations: list[str] = []

    for label, geography, chosen in (
        ("a", geography_a, chosen_a),
        ("b", geography_b, chosen_b),
    ):
        if chosen is None:
            asked = f" for the period starting {period_start}" if period_start else ""
            notes.append(
                PlacesNote(
                    reason="not_published",
                    detail=(
                        f"side {label} ({place_name(geography)}) "
                        f"has no published value{asked}; it is not zero"
                    ),
                )
            )
        elif usable_value(chosen) is None:
            notes.append(
                PlacesNote(
                    reason="not_published",
                    detail=(
                        f"side {label}'s value for its period is withheld or not "
                        f"numeric ({chosen.get('value_status') or 'no value'}); "
                        "it is not zero"
                    ),
                )
            )

    same_period = (
        chosen_a is not None
        and chosen_b is not None
        and _period_key(chosen_a) == _period_key(chosen_b)
    )
    if chosen_a is not None and chosen_b is not None and not same_period:
        notes.append(
            PlacesNote(
                reason="periods_differ",
                detail=(
                    "the two places publish no common period, so each shows its "
                    f"own newest ({chosen_a.get('period_start')} to "
                    f"{chosen_a.get('period_end')} beside "
                    f"{chosen_b.get('period_start')} to {chosen_b.get('period_end')}); "
                    "no difference is computed across periods"
                ),
            )
        )

    if containment.relationship in ("contains", "part_of"):
        inner = "b" if containment.relationship == "contains" else "a"
        notes.append(
            PlacesNote(
                reason="contained",
                detail=(
                    f"side {inner} lies inside the other, so the larger place's "
                    "value includes the smaller one's; they are shown side by "
                    "side and no difference is computed"
                ),
            )
        )
    elif containment.relationship == "intersects":
        share = containment.share_of_smaller_inside_larger
        share_text = (
            f"{share:.0%} of side {containment.smaller_side}'s area lies inside "
            "the other"
            if share is not None
            else "the two overlap"
        )
        notes.append(
            PlacesNote(
                reason="overlapping_places",
                detail=(
                    f"{share_text}, so the two values partly describe the same "
                    "people; no difference is computed"
                ),
            )
        )

    if not cross_level and kind not in COMPARABLE_KINDS:
        alternatives = count_alternatives(code_a) if pairing is None else []
        notes.append(
            PlacesNote(
                reason="counts_differ_in_size",
                detail=(
                    "a count grows with the place; two places of one level can "
                    "still differ greatly in size"
                    + (
                        f", so read it beside {alternatives[0].ratio_id}"
                        if alternatives
                        else ""
                    )
                ),
            )
        )

    bounds_a, bounds_b = interval(chosen_a), interval(chosen_b)
    overlap: Optional[bool] = None
    value_a, value_b = usable_value(chosen_a), usable_value(chosen_b)
    if value_a is not None and value_b is not None:
        overlap = intervals_overlap(bounds_a, bounds_b) if same_period else None
        if bounds_a is None or bounds_b is None:
            notes.append(
                PlacesNote(
                    reason="no_margin_published",
                    detail=(
                        "a margin of error is not published for "
                        + (
                            "either side"
                            if bounds_a is None and bounds_b is None
                            else f"side {'a' if bounds_a is None else 'b'}"
                        )
                        + ", so whether the two differ beyond sampling or "
                        "measurement error cannot be said"
                    ),
                )
            )
        if overlap is not None:
            derivations.extend(INTERVAL_DERIVATIONS)
        if overlap:
            notes.append(
                PlacesNote(
                    reason="intervals_overlap",
                    detail=(
                        "the two 90 percent intervals overlap: the values are "
                        "not distinguishable at 90 percent confidence"
                    ),
                )
            )

    difference: Optional[str] = None
    if (
        value_a is not None
        and value_b is not None
        and same_period
        and containment.relationship in ("adjacent", "none_recorded")
    ):
        difference = str(value_a - value_b)
        derivations.insert(0, "difference")

    caveats: list[str] = []
    definition = definition_for(code_a) if pairing is None else None
    if definition is not None and definition.caveat:
        caveats.append(definition.caveat)
    if pairing is not None:
        caveats.extend(pairing.limitations)
    if kind_status != "approved":
        caveats.append(
            f"the measure's kind ({kind}) is {kind_status}, not yet reviewed"
            if kind_status != "undeclared"
            else "the measure declares no kind, so it is treated as a count"
        )

    missing = [
        (label, geography, chosen)
        for label, geography, chosen in (
            ("a", geography_a, chosen_a),
            ("b", geography_b, chosen_b),
        )
        if usable_value(chosen) is None
    ]
    if missing:
        return PlacesComparisonResponse(
            **response,
            comparable=False,
            refusal=unpublished_refusal(missing),
            period_basis=basis,
            side_a=_side(geography_a, code_a, metric_a, chosen_a, read=True),
            side_b=_side(geography_b, code_b, metric_b, chosen_b, read=True),
            derivations=[],
            notes=notes,
            caveats=caveats,
        )

    return PlacesComparisonResponse(
        **response,
        comparable=True,
        period_basis=basis,
        side_a=_side(geography_a, code_a, metric_a, chosen_a, read=True),
        side_b=_side(geography_b, code_b, metric_b, chosen_b, read=True),
        difference=difference,
        intervals_overlap=overlap,
        derivations=derivations,
        notes=notes,
        caveats=caveats,
    )
