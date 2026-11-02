"""The age band scheme registry (ADR-0013).

`docs/semantics/age_bands.json` names, for each sex-by-age table the site
draws a pyramid from, which provider lines make each output band, broad group
and indicator. Nothing is grouped by reading labels: a band is exactly the sum
of the lines declared here, and the loader refuses a scheme that uses a line
twice or leaves one of the table's age lines out.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, replace
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Optional

REGISTRY_PATH = (
    Path(__file__).resolve().parents[3] / "docs" / "semantics" / "age_bands.json"
)

STATUSES = frozenset({"draft", "approved", "deprecated"})
SEXES = ("men", "women")
#: The groups every indicator is built from, plus the two sex totals.
GROUP_IDS = ("under_18", "age_18_64", "age_65_plus", "age_85_plus")


class AgeBandRegistryError(ValueError):
    """The registry breaks its own contract."""


@dataclass(frozen=True)
class AgeBand:
    band_id: str
    age_start: int
    age_end: Optional[int]
    label: str
    men: tuple[str, ...]
    women: tuple[str, ...]

    def lines(self, sex: str) -> tuple[str, ...]:
        return self.men if sex == "men" else self.women


@dataclass(frozen=True)
class AgeGroup:
    group_id: str
    label: str
    men: tuple[str, ...]
    women: tuple[str, ...]

    def lines(self, sex: str) -> tuple[str, ...]:
        return self.men if sex == "men" else self.women


@dataclass(frozen=True)
class AgeIndicator:
    indicator_id: str
    label: str
    numerator: tuple[str, ...]
    denominator: str
    scale: int
    unit: str


@dataclass(frozen=True)
class AgeScheme:
    scheme_id: str
    version: int
    status: str
    label: str
    table_id: str
    source_code: str
    datasets: tuple[str, ...]
    totals: dict[str, str]
    median_age: dict[str, str]
    bands: tuple[AgeBand, ...]
    groups: tuple[AgeGroup, ...]
    indicators: tuple[AgeIndicator, ...]

    def group(self, group_id: str) -> AgeGroup:
        for group in self.groups:
            if group.group_id == group_id:
                return group
        raise KeyError(group_id)

    def metric_code(self, dataset: str, variable: str) -> str:
        return f"{self.source_code}:{dataset}:{variable}"

    def variables(self) -> tuple[str, ...]:
        """Every variable the scheme reads, totals and medians included."""
        seen: dict[str, None] = {}
        for code in self.totals.values():
            seen[code] = None
        for band in self.bands:
            for sex in SEXES:
                for line in band.lines(sex):
                    seen[line] = None
        for key in ("all", "men", "women"):
            seen[self.median_age[key]] = None
        return tuple(seen)

    def metric_codes(self, dataset: str) -> tuple[str, ...]:
        return tuple(self.metric_code(dataset, code) for code in self.variables())

    def next_band(self, band_id: str) -> Optional[AgeBand]:
        """The band one step older, or ``None`` past the last closed band."""
        for index, band in enumerate(self.bands):
            if band.band_id == band_id:
                if index + 1 < len(self.bands):
                    following = self.bands[index + 1]
                    # Nothing moves into an open-ended band from a closed one:
                    # 85+ in the later survey also holds the earlier 85+.
                    return following if following.age_end is not None else None
                return None
        raise KeyError(band_id)


def _strings(value: Any, where: str) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(item, str) and item for item in value)
    ):
        raise AgeBandRegistryError(f"{where}: expected a non-empty list of lines")
    return tuple(value)


def _require(entry: dict, key: str, where: str) -> Any:
    if key not in entry:
        raise AgeBandRegistryError(f"{where}: missing {key}")
    return entry[key]


def parse_scheme(entry: dict) -> AgeScheme:
    scheme_id = _require(entry, "scheme_id", "scheme")
    where = f"scheme {scheme_id}"
    status = _require(entry, "status", where)
    if status not in STATUSES:
        raise AgeBandRegistryError(f"{where}: unknown status {status!r}")
    bands = tuple(
        AgeBand(
            band_id=_require(band, "band_id", where),
            age_start=int(_require(band, "age_start", where)),
            age_end=None if band.get("age_end") is None else int(band["age_end"]),
            label=_require(band, "label", where),
            men=_strings(band.get("men"), f"{where} band {band.get('band_id')} men"),
            women=_strings(
                band.get("women"), f"{where} band {band.get('band_id')} women"
            ),
        )
        for band in _require(entry, "bands", where)
    )
    groups = tuple(
        AgeGroup(
            group_id=_require(group, "group_id", where),
            label=_require(group, "label", where),
            men=_strings(group.get("men"), f"{where} group men"),
            women=_strings(group.get("women"), f"{where} group women"),
        )
        for group in _require(entry, "groups", where)
    )
    indicators = []
    for item in _require(entry, "indicators", where):
        numerator = item["numerator"]
        indicators.append(
            AgeIndicator(
                indicator_id=item["indicator_id"],
                label=item["label"],
                numerator=tuple(
                    numerator if isinstance(numerator, list) else [numerator]
                ),
                denominator=item["denominator"],
                scale=int(item["scale"]),
                unit=item["unit"],
            )
        )
    scheme = AgeScheme(
        scheme_id=scheme_id,
        version=int(_require(entry, "version", where)),
        status=status,
        label=_require(entry, "label", where),
        table_id=_require(entry, "table_id", where),
        source_code=_require(entry, "source_code", where),
        datasets=tuple(_require(entry, "datasets", where)),
        totals=dict(_require(entry, "totals", where)),
        median_age=dict(_require(entry, "median_age", where)),
        bands=bands,
        groups=groups,
        indicators=tuple(indicators),
    )
    _validate(scheme)
    return scheme


def _validate(scheme: AgeScheme) -> None:
    where = f"scheme {scheme.scheme_id}"
    if not scheme.bands:
        raise AgeBandRegistryError(f"{where}: no bands")
    previous_end: Optional[int] = None
    for index, band in enumerate(scheme.bands):
        expected = 0 if previous_end is None else previous_end + 1
        if band.age_start != expected:
            raise AgeBandRegistryError(
                f"{where}: band {band.band_id} starts at {band.age_start}, "
                f"expected {expected}"
            )
        if band.age_end is None and index != len(scheme.bands) - 1:
            raise AgeBandRegistryError(f"{where}: only the last band may be open")
        previous_end = band.age_end
    for sex in SEXES:
        used = [line for band in scheme.bands for line in band.lines(sex)]
        if len(used) != len(set(used)):
            raise AgeBandRegistryError(f"{where}: a {sex} line is used twice")
        for group in scheme.groups:
            if not set(group.lines(sex)) <= set(used):
                raise AgeBandRegistryError(
                    f"{where}: group {group.group_id} names a line no band holds"
                )
    men_lines = {line for band in scheme.bands for line in band.men}
    women_lines = {line for band in scheme.bands for line in band.women}
    if men_lines & women_lines:
        raise AgeBandRegistryError(f"{where}: a line is both men's and women's")
    names = {group.group_id for group in scheme.groups} | {"men", "women"}
    for indicator in scheme.indicators:
        for part in (*indicator.numerator, indicator.denominator):
            if part not in names:
                raise AgeBandRegistryError(
                    f"{where}: indicator {indicator.indicator_id} names {part!r}"
                )
    for group_id in GROUP_IDS:
        if group_id not in {group.group_id for group in scheme.groups}:
            raise AgeBandRegistryError(f"{where}: group {group_id} is missing")


def parse_registry(document: dict) -> dict[str, AgeScheme]:
    schemes: dict[str, AgeScheme] = {}
    for entry in document.get("schemes", []):
        scheme = parse_scheme(entry)
        if scheme.scheme_id in schemes:
            raise AgeBandRegistryError(f"duplicate scheme {scheme.scheme_id}")
        schemes[scheme.scheme_id] = scheme
    return schemes


@lru_cache(maxsize=1)
def load_registry(path: Path = REGISTRY_PATH) -> dict[str, AgeScheme]:
    return parse_registry(json.loads(path.read_text(encoding="utf-8")))


# ---------------------------------------------------------------------------
# By-age rate schemes (ADR-0013 consequences; views plan item 6)
# ---------------------------------------------------------------------------

#: The sides a rate is served for: everyone, and each sex where the table
#: publishes one.
RATE_SIDES = ("all", "men", "women")


@dataclass(frozen=True)
class RateLines:
    """The lines summed into one side's numerator and into its universe."""

    numerator: tuple[str, ...]
    denominator: tuple[str, ...]


@dataclass(frozen=True)
class RateEdition:
    """How a group's lines were labelled in releases before ``before_vintage``
    (the Bureau moved an age boundary without renumbering the lines)."""

    before_vintage: int
    label: str
    age_start: int
    age_end: Optional[int]


@dataclass(frozen=True)
class RateGroup:
    group_id: str
    label: str
    age_start: int
    age_end: Optional[int]
    sides: dict[str, RateLines]
    editions: tuple[RateEdition, ...] = ()

    def at(self, vintage: int) -> "RateGroup":
        """The group as a release of ``vintage`` labels it."""
        for edition in sorted(self.editions, key=lambda e: e.before_vintage):
            if vintage < edition.before_vintage:
                return replace(
                    self,
                    label=edition.label,
                    age_start=edition.age_start,
                    age_end=edition.age_end,
                    editions=(),
                )
        return replace(self, editions=())


@dataclass(frozen=True)
class RateScheme:
    """One by-age table read as a rate per age group: a share of the group's
    universe, never a share of the whole population."""

    rate_id: str
    version: int
    status: str
    label: str
    table_id: str
    source_code: str
    datasets: tuple[str, ...]
    by_sex: bool
    universe: str
    numerator_label: str
    unit: str
    total: RateLines
    groups: tuple[RateGroup, ...]
    #: Words a numerator line's Bureau label must hold, and must not.
    numerator_has: str = ""
    numerator_lacks: tuple[str, ...] = ()
    #: The first vintage, per dataset, whose line layout these groups map;
    #: an older release published another layout and is not served.
    served_from: dict[str, int] = field(default_factory=dict)

    def serves(self, dataset: str, vintage: int) -> bool:
        return vintage >= self.served_from.get(dataset, 0)

    def metric_code(self, dataset: str, variable: str) -> str:
        return f"{self.source_code}:{dataset}:{variable}"

    def boundary_vintages(self) -> tuple[int, ...]:
        """One vintage inside each span of releases labelled alike."""
        befores = sorted({e.before_vintage for g in self.groups for e in g.editions})
        return (*(year - 1 for year in befores), 9999)

    def at(self, vintage: int) -> "RateScheme":
        """The scheme's groups as a release of ``vintage`` labels them."""
        return replace(self, groups=tuple(group.at(vintage) for group in self.groups))

    def variables(self) -> tuple[str, ...]:
        seen: dict[str, None] = {}
        for lines in (self.total, *(s for g in self.groups for s in g.sides.values())):
            for code in (*lines.numerator, *lines.denominator):
                seen[code] = None
        return tuple(seen)

    def metric_codes(self, dataset: str) -> tuple[str, ...]:
        return tuple(self.metric_code(dataset, code) for code in self.variables())


def _rate_lines(value: Any, where: str) -> RateLines:
    if not isinstance(value, dict):
        raise AgeBandRegistryError(f"{where}: expected numerator and denominator")
    return RateLines(
        numerator=_strings(value.get("numerator"), f"{where} numerator"),
        denominator=_strings(value.get("denominator"), f"{where} denominator"),
    )


def _group_sides(group: dict, by_sex: bool, where: str) -> dict[str, RateLines]:
    numerator = _require(group, "numerator", where)
    denominator = _require(group, "denominator", where)
    keys = ("men", "women") if by_sex else ("all",)
    for part, name in ((numerator, "numerator"), (denominator, "denominator")):
        if not isinstance(part, dict) or set(part) != set(keys):
            raise AgeBandRegistryError(
                f"{where}: {name} must name exactly {', '.join(keys)}"
            )
    sides = {
        key: RateLines(
            numerator=_strings(numerator[key], f"{where} {key} numerator"),
            denominator=_strings(denominator[key], f"{where} {key} denominator"),
        )
        for key in keys
    }
    if by_sex:
        # Everyone in the group is the two sexes' lines together.
        sides = {
            "all": RateLines(
                numerator=sides["men"].numerator + sides["women"].numerator,
                denominator=sides["men"].denominator + sides["women"].denominator,
            ),
            **sides,
        }
    return sides


def parse_rate_scheme(entry: dict) -> RateScheme:
    rate_id = _require(entry, "rate_id", "rate scheme")
    where = f"rate scheme {rate_id}"
    status = _require(entry, "status", where)
    if status not in STATUSES:
        raise AgeBandRegistryError(f"{where}: unknown status {status!r}")
    by_sex = bool(_require(entry, "by_sex", where))
    groups = tuple(
        RateGroup(
            group_id=_require(group, "group_id", where),
            label=_require(group, "label", where),
            age_start=int(_require(group, "age_start", where)),
            age_end=None if group.get("age_end") is None else int(group["age_end"]),
            sides=_group_sides(group, by_sex, f"{where} group {group.get('group_id')}"),
            editions=tuple(
                RateEdition(
                    before_vintage=int(_require(edition, "before_vintage", where)),
                    label=_require(edition, "label", where),
                    age_start=int(_require(edition, "age_start", where)),
                    age_end=None
                    if edition.get("age_end") is None
                    else int(edition["age_end"]),
                )
                for edition in group.get("earlier_editions") or ()
            ),
        )
        for group in _require(entry, "groups", where)
    )
    scheme = RateScheme(
        rate_id=rate_id,
        version=int(_require(entry, "version", where)),
        status=status,
        label=_require(entry, "label", where),
        table_id=_require(entry, "table_id", where),
        source_code=_require(entry, "source_code", where),
        datasets=tuple(_require(entry, "datasets", where)),
        by_sex=by_sex,
        universe=_require(entry, "universe", where),
        numerator_label=_require(entry, "numerator_label", where),
        unit=_require(entry, "unit", where),
        total=_rate_lines(_require(entry, "total", where), f"{where} total"),
        groups=groups,
        numerator_has=str(_require(entry, "label_check", where).get("numerator_has")),
        numerator_lacks=tuple(entry["label_check"].get("numerator_lacks") or ()),
        served_from={
            str(dataset): int(year)
            for dataset, year in (entry.get("served_from") or {}).items()
        },
    )
    _validate_rate(scheme)
    return scheme


def _validate_rate(scheme: RateScheme) -> None:
    where = f"rate scheme {scheme.rate_id}"
    if scheme.unit != "percent":
        raise AgeBandRegistryError(f"{where}: a rate is served in percent")
    if not scheme.groups:
        raise AgeBandRegistryError(f"{where}: no age groups")
    if not set(scheme.served_from) <= set(scheme.datasets):
        raise AgeBandRegistryError(f"{where}: served_from names an unknown dataset")
    prefix = f"{scheme.table_id}_"
    for code in scheme.variables():
        if not code.startswith(prefix):
            raise AgeBandRegistryError(
                f"{where}: {code} is not a {scheme.table_id} line"
            )
    for vintage in scheme.boundary_vintages():
        previous_end: Optional[int] = None
        for index, group in enumerate(scheme.at(vintage).groups):
            if previous_end is not None and group.age_start != previous_end + 1:
                raise AgeBandRegistryError(
                    f"{where}: group {group.group_id} does not follow the one before"
                )
            if group.age_end is None and index != len(scheme.groups) - 1:
                raise AgeBandRegistryError(f"{where}: only the last group may be open")
            if group.age_end is not None and group.age_end < group.age_start:
                raise AgeBandRegistryError(
                    f"{where}: group {group.group_id} ends early"
                )
            previous_end = group.age_end
    for side in ("men", "women") if scheme.by_sex else ("all",):
        numerators = [c for g in scheme.groups for c in g.sides[side].numerator]
        denominators = [c for g in scheme.groups for c in g.sides[side].denominator]
        if len(numerators) != len(set(numerators)):
            raise AgeBandRegistryError(
                f"{where}: a {side} numerator line is used twice"
            )
        if len(denominators) != len(set(denominators)):
            raise AgeBandRegistryError(
                f"{where}: a {side} denominator line is used twice"
            )
        for group in scheme.groups:
            lines = group.sides[side]
            if set(lines.numerator) == set(lines.denominator):
                raise AgeBandRegistryError(
                    f"{where}: group {group.group_id} divides a count by itself"
                )
    if scheme.by_sex:
        men = {c for g in scheme.groups for c in g.sides["men"].numerator}
        women = {c for g in scheme.groups for c in g.sides["women"].numerator}
        if men & women:
            raise AgeBandRegistryError(f"{where}: a line is both men's and women's")


def parse_rate_registry(document: dict) -> dict[str, RateScheme]:
    schemes: dict[str, RateScheme] = {}
    for entry in document.get("rate_schemes", []):
        scheme = parse_rate_scheme(entry)
        if scheme.rate_id in schemes:
            raise AgeBandRegistryError(f"duplicate rate scheme {scheme.rate_id}")
        schemes[scheme.rate_id] = scheme
    return schemes


@lru_cache(maxsize=1)
def load_rate_registry(path: Path = REGISTRY_PATH) -> dict[str, RateScheme]:
    return parse_rate_registry(json.loads(path.read_text(encoding="utf-8")))


#: The scheme the pyramid routes read.
DEFAULT_SCHEME_ID = "acs_b01001_five_year"


def default_scheme() -> AgeScheme:
    return load_registry()[DEFAULT_SCHEME_ID]


_AGE_NUMBER = re.compile(r"\d+")


def label_age_range(label: str) -> Optional[tuple[int, Optional[int]]]:
    """The ages a Bureau label's age segment covers (``Under 5 years`` →
    (0, 4), ``18 and 19 years`` → (18, 19), ``75 years and over`` → (75,
    None)), or ``None`` when no segment names an age."""
    for segment in reversed(label.replace(":", "").split("!!")):
        if "year" not in segment:
            continue
        numbers = [int(n) for n in _AGE_NUMBER.findall(segment)]
        if not numbers:
            continue
        if segment.strip().lower().startswith("under"):
            return (0, numbers[0] - 1)
        if "and over" in segment or "or older" in segment:
            return (numbers[0], None)
        return (numbers[0], numbers[-1])
    return None


def rate_label_problems(
    scheme: RateScheme, labels: Mapping[str, Optional[str]], vintage: int = 9999
) -> list[str]:
    """Every way the Bureau's own labels disagree with a rate scheme.

    ``labels`` maps a variable (``B17001_004``) to its published label. A
    numerator line must name what the scheme counts and not what it leaves
    out; each line of a by-sex group names its sex; and each age group's
    lines name ages inside the group's range. Run against the warehouse's
    ``dim_acs_variable`` before a scheme is approved, for every loaded
    ``vintage``: a group is checked as that release labels it.
    """
    problems: list[str] = []
    scheme = scheme.at(vintage)

    def check(code: str, group: RateGroup, side: str, role: str) -> None:
        label = labels.get(code)
        if not label:
            problems.append(f"{scheme.rate_id} {code}: no published label")
            return
        if role == "numerator":
            if scheme.numerator_has.lower() not in label.lower():
                problems.append(f"{code} ({label}) lacks {scheme.numerator_has!r}")
            for word in scheme.numerator_lacks:
                if word.lower() in label.lower():
                    problems.append(f"{code} ({label}) holds {word!r}")
        segments = {part.strip().rstrip(":") for part in label.split("!!")}
        if side == "men" and "Male" not in segments:
            problems.append(f"{code} ({label}) is not a men's line")
        if side == "women" and "Female" not in segments:
            problems.append(f"{code} ({label}) is not a women's line")
        ages = label_age_range(label)
        if ages is None:
            problems.append(f"{code} ({label}) names no age")
            return
        start, end = ages
        inside = start >= group.age_start and (
            group.age_end is None or (end is not None and end <= group.age_end)
        )
        if not inside:
            problems.append(
                f"{code} ({label}) is outside {group.group_id} "
                f"({group.age_start} to {group.age_end})"
            )

    for group in scheme.groups:
        sides = ("men", "women") if scheme.by_sex else ("all",)
        for side in sides:
            lines = group.sides[side]
            for code in lines.numerator:
                check(code, group, side, "numerator")
            for code in lines.denominator:
                if code not in lines.numerator:
                    check(code, group, side, "denominator")
    return problems
