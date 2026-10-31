"""The age band scheme registry (ADR-0013).

`docs/semantics/age_bands.json` names, for each sex-by-age table the site
draws a pyramid from, which provider lines make each output band, broad group
and indicator. Nothing is grouped by reading labels: a band is exactly the sum
of the lines declared here, and the loader refuses a scheme that uses a line
twice or leaves one of the table's age lines out.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

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


#: The scheme the pyramid routes read.
DEFAULT_SCHEME_ID = "acs_b01001_five_year"


def default_scheme() -> AgeScheme:
    return load_registry()[DEFAULT_SCHEME_ID]
