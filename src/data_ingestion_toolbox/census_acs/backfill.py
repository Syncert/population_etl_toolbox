"""One ACS year at one grain, as an operator-requested backfill run.

The scheduled `acs_ingest` run requests places and tracts for each dataset's
declared years only (`AcsConfig.place_year_scope`, `tract_year_scope`).
A city or tract history is loaded instead as a sequence of small runs, each
naming one year and one grain in its Airflow `dag_run.conf`:

    {"acs_backfill": {"year": 2019, "geo_level": "tract"}}

Such a run plans only that year's slices at that grain, transforms only that
year, and so publishes only that year's serving chunk. A failure costs one
year, not the whole history, and the scheduled run is unchanged.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

#: The grains a backfill run may name. County, state and nation are loaded
#: for every year by the scheduled run already.
BACKFILL_GEO_LEVELS = frozenset({"place", "tract", "metro"})

#: The first year the Data API serves any ACS estimate (the 1-year 2005).
FIRST_ACS_YEAR = 2005


class BackfillScopeError(ValueError):
    """A `dag_run.conf` backfill request that does not name a loadable scope."""


@dataclass(frozen=True)
class BackfillScope:
    year: int
    geo_level: str

    @classmethod
    def from_conf(cls, conf: Mapping[str, Any] | None) -> BackfillScope | None:
        """Read `acs_backfill` from a run's conf; ``None`` for a normal run.

        A malformed request refuses rather than falling back to the scheduled
        plan: a typo must not turn a one-year run into the full monthly one.
        """
        raw = (conf or {}).get("acs_backfill")
        if raw is None:
            return None
        if not isinstance(raw, Mapping):
            raise BackfillScopeError("acs_backfill must be an object")
        unknown = set(raw) - {"year", "geo_level"}
        if unknown:
            raise BackfillScopeError(
                f"acs_backfill has unknown keys: {', '.join(sorted(unknown))}"
            )
        year = raw.get("year")
        if isinstance(year, bool) or not isinstance(year, int):
            raise BackfillScopeError("acs_backfill.year must be an integer year")
        if year < FIRST_ACS_YEAR:
            raise BackfillScopeError(
                f"acs_backfill.year must be {FIRST_ACS_YEAR} or later"
            )
        geo_level = raw.get("geo_level")
        if geo_level not in BACKFILL_GEO_LEVELS:
            raise BackfillScopeError(
                "acs_backfill.geo_level must be one of: "
                + ", ".join(sorted(BACKFILL_GEO_LEVELS))
            )
        return cls(year=year, geo_level=str(geo_level))

    def as_conf(self) -> dict[str, Any]:
        return {"acs_backfill": {"year": self.year, "geo_level": self.geo_level}}
