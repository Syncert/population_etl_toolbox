"""What the warehouse's published freshness signal means to the served API.

`gold_glossary.dim_metric.freshness_state` is harvested, not computed here:
the warehouse decides when a measure's provider series has ended. The API's
job is to say so consistently, and it did not. Three surfaces read a metric
row and only one of them looked at the state:

- `/catalog/metrics/{metric_code}` published a retired row with every
  observation route its source has and `served_by_neutral_routes: true`,
  so a discovering client was told to query a measure that answers
  `total: 0`;
- a stored analysis configuration and an evidence-packet block validated a
  retired measure as current, because `_require_metric` raised only when the
  glossary row was *absent*.

The guide already states the contract -- a retired series "still resolves
through `GET /catalog/metrics/{metric_code}` and reports `freshness_state:
"retired"` ... It no longer answers observations" -- so this module is the
one place that turns the published state into that refusal (API-119).
"""

from __future__ import annotations

from typing import Any

#: The warehouse's published state for a measure whose provider series has
#: ended. Its history stays queryable through the catalog; its observations
#: are not served.
FRESHNESS_RETIRED = "retired"


def is_retired(freshness_state: Any) -> bool:
    """Whether a harvested ``freshness_state`` means retired.

    Takes the value rather than the row, because its two readers hold
    different shapes: a service reads a ``RowMapping`` from the glossary, the
    catalog resource a validated ``MetricCapability``. Compared
    case-insensitively and whitespace-stripped so a harvest that writes
    ``'Retired'`` is not silently read as current -- a missing or unknown
    state is *not* retirement, because inventing retirement from an unread
    signal would hide a measure the warehouse still publishes.
    """
    if freshness_state is None:
        return False
    return str(freshness_state).strip().lower() == FRESHNESS_RETIRED


def retirement_refusal(
    field: str, metric_code: str, freshness_state: Any
) -> str | None:
    """Why a stored document naming this measure cannot be replayed, or ``None``.

    One sentence for every surface that refuses one, so a configuration and a
    packet block report the same fact in the same words, and both say
    "retired" rather than the "not a published metric" they would have said
    only if the glossary row had been deleted.
    """
    if not is_retired(freshness_state):
        return None
    return (
        f"{field} '{metric_code}' is retired: the warehouse still publishes "
        "its catalog entry and its history, but the observation routes no "
        "longer answer it, so this document cannot be replayed as saved"
    )
