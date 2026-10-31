"""Which ACS years a grain is requested for, declared rather than counted.

The scheduled `acs_ingest` run used to request place and tract slices for
"the N newest years" of each dataset. That is a moving target: when the
Bureau publishes a new year, the oldest year in the window silently drops out
of the plan, and a history loaded through a run-time override is not described
by the configuration at all. A declared scope names the years:

    "newest"        the newest published year (the default)
    "newest:3"      the three newest published years
    "2009-2024"     every published year from 2009 through 2024
    "2019"          that one year
    "none"          no year

A scope is intersected with the years a dataset actually publishes, so
"2009-2024" over the 1-year dataset leaves out 2020, which the Bureau did not
release.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

_NEWEST = re.compile(r"^newest(?::(\d+))?$")
_RANGE = re.compile(r"^(\d{4})(?:-(\d{4}))?$")


class YearScopeError(ValueError):
    """A year scope declaration this module cannot read."""


@dataclass(frozen=True)
class YearScope:
    #: Take this many of the newest published years (0 with no range: none).
    newest: int = 0
    first: int | None = None
    last: int | None = None

    @classmethod
    def parse(cls, spec: str) -> YearScope:
        text = str(spec).strip().lower()
        if text == "none":
            return cls()
        match = _NEWEST.match(text)
        if match:
            count = int(match.group(1)) if match.group(1) else 1
            if count < 1:
                raise YearScopeError("newest:N needs N of at least 1")
            return cls(newest=count)
        match = _RANGE.match(text)
        if match:
            first = int(match.group(1))
            last = int(match.group(2)) if match.group(2) else first
            if last < first:
                raise YearScopeError(f"year scope {spec!r} ends before it starts")
            return cls(first=first, last=last)
        raise YearScopeError(
            f"year scope {spec!r} is not one of: newest, newest:N, YYYY, "
            "YYYY-YYYY, none"
        )

    def select(self, published: Iterable[int]) -> set[int]:
        """The published years this scope names."""
        years = sorted({int(year) for year in published}, reverse=True)
        if self.first is not None and self.last is not None:
            return {year for year in years if self.first <= year <= self.last}
        return set(years[: self.newest])


def validate_year_scope(spec: str) -> str:
    """Pydantic hook: refuse a scope that does not parse, keep its text."""
    YearScope.parse(spec)
    return spec
