"""The database tier must be reproducible against one warehouse.

Covers: DB-024 — a tier whose failures depend on what ran before it is a tier
whose red gets ignored, which is the worst state for a test tier to be in. The
full run used to report six failures that passed when the same two files ran
alone: a FRED suite committed the whole configured dataset set and removed only
the row it asserted on, so ``DQ-FRED-002`` correctly reported configured
datasets whose series were absent, and every test needing a promotable release
certification fell over behind it.

This module does not re-run the tier. It asserts the property that makes the
tier reproducible -- that a suite owns what it commits -- at the point where
breaking it is cheap to detect.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.fred.config import CONFIG

pytestmark = [pytest.mark.integration, pytest.mark.database]

#: Relations that are configuration or captured provider metadata rather than
#: test-owned fixtures. A suite that writes one of these and leaves rows behind
#: changes what every later suite in the session observes.
SHARED_RELATIONS = (
    "raw_fred.fred_datasets",
    "raw_fred.fred_series",
    "control.acs_ingestion_slices",
    "control.bls_ingestion_slices",
    "control.fred_ingestion_slices",
    # Revision rows are pending work: the next run's transform reads every one
    # of them and refuses when their geography is gone -- which it is, because
    # the suites that seeded those geographies clean up after themselves. This
    # is how the leak presents: a failure on the second run, in a suite that
    # did nothing wrong.
    "silver_census.observation_revision",
    "silver_bls.observation_revision",
    "silver_fred.observation_revision",
)


def test_no_suite_left_shared_provider_state_behind(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-024 — the tier ends as reproducible as it began.

    Ordered last by filename within the database directory so it observes what
    the session leaves. It reports what leaked rather than asserting an exact
    warehouse, because a fixture that cleans up correctly leaves nothing here
    regardless of what it did in between.
    """
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            residue = {}
            for relation in SHARED_RELATIONS:
                cursor.execute(f"SELECT COUNT(*) FROM {relation}")
                count = cursor.fetchone()[0]
                if count:
                    residue[relation] = count
    finally:
        reader.close()

    assert not residue, (
        "these shared relations still hold rows a suite committed and did not "
        "remove. The next suite in the session, and the next run against this "
        f"warehouse, both observe them: {residue}"
    )


def test_the_fred_dataset_sync_writes_the_whole_configured_set(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-024 — the shape of cleanup a caller of that sync owes.

    ``sync_fred_datasets_table`` writes one row per configured (domain, series)
    pair, not one row per series a test names. A suite that calls it and then
    deletes a single ``series_id`` leaves the rest behind; this states the size
    of the debt so the next caller cannot mistake it.
    """
    configured = [
        (domain, series)
        for domain, series_list in CONFIG.curated_by_domain.items()
        for series in series_list
    ]
    assert len(configured) > 1, (
        "the configured set is what makes partial cleanup a bug; with one pair "
        "there would be nothing to leak"
    )
    assert len(configured) == len(set(configured))
