"""The one predicate every source DAG waits on (DAG-020).

`BETA_RESET_REINGESTION.md` §5 orders ingestion: the shared geography
reference first, then every source. Three DAGs enforced that with a real count
of `silver_ref.dim_geo_current`. The three sources added later checked
`to_regclass('silver_ref.dim_geo_entity')` instead -- which asks whether the
*table exists*, and the bootstrap manifest creates it, empty, in its
`reference` phase.

So the weaker guard passed on exactly the warehouse the ordering rule exists
to protect. A CDC, FBI or NASS run against a bootstrapped-but-empty reference
resolves every row as `unmapped`, the release is still marked `published`, and
the resolved-geography serving views exclude all of it. Nothing re-resolves an
already-published release, so the source reports `published` and serves
nothing -- which the live smoke tier notices afterwards rather than
preventing.

This module holds the thresholds once. A source that needs more than the
shared minimum says so at its call site rather than restating the shared part.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

#: What "the geography reference is loaded" means, for every source.
#:
#: One nation, the fifty states, and the counties. These are the counts the
#: ACS, BLS and PEP guards already used; naming them here is what makes the
#: other three able to ask the same question.
SHARED_GEOGRAPHY_MINIMUMS: Mapping[str, int] = {
    "nation": 1,
    "state": 50,
    "county": 3000,
}


class SharedGeographyNotLoaded(RuntimeError):
    """The reference is not loaded, and what was actually counted."""


def require_shared_geography_loaded(
    connection: Any,
    *,
    additional_minimums: Mapping[str, int] | None = None,
) -> dict[str, int]:
    """Refuse unless the shared geography reference carries real rows.

    Returns the observed counts for every grain it checked, so a caller can
    log what it saw rather than only that it was satisfied.

    `additional_minimums` is for a source that needs a grain the shared
    minimum does not cover -- Census PEP serves place-level estimates and
    requires eighteen thousand places. It adds to the shared thresholds and
    can neither replace nor lower them.
    """
    minimums = dict(SHARED_GEOGRAPHY_MINIMUMS)
    for grain, minimum in (additional_minimums or {}).items():
        # A caller may raise its own bar, never lower the shared one.
        minimums[grain] = max(int(minimum), minimums.get(grain, 0))

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT geo_type, COUNT(*)
            FROM silver_ref.dim_geo_current
            WHERE is_active AND geo_type = ANY(%s)
            GROUP BY geo_type
            """,
            (sorted(minimums),),
        )
        counted = {str(grain): int(count) for grain, count in cursor.fetchall()}

    # Every grain asked about, including the ones that answered nothing: a
    # message reading `county=3000` and saying nothing about `nation` leaves
    # the reader to guess which half of the predicate failed.
    observed = {grain: counted.get(grain, 0) for grain in sorted(minimums)}
    short = {
        grain: minimum
        for grain, minimum in minimums.items()
        if observed[grain] < minimum
    }
    if short:
        seen = ", ".join(f"{grain}={observed[grain]}" for grain in sorted(minimums))
        wanted = ", ".join(
            f"{grain}>={minimum}" for grain, minimum in sorted(short.items())
        )
        raise SharedGeographyNotLoaded(
            "shared geography is incomplete; run silver_ref successfully first "
            f"({seen}; needs {wanted})"
        )
    return observed
