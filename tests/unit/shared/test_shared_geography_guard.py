"""The one predicate every source DAG waits on.

Covers: DAG-020 -- the shared geography guard counts rows rather than asking
        whether a table exists, the thresholds live in one place, and a source
        that needs more says so without restating the shared minimum.

The defect this replaces was not a missing guard. It was a guard that asked
`to_regclass('silver_ref.dim_geo_entity')` -- whether the table exists -- on a
warehouse whose bootstrap manifest creates that table, empty, before any
source runs. It passed on exactly the state the ordering rule exists to
protect.
"""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.silver_ref.geography_guard import (
    SHARED_GEOGRAPHY_MINIMUMS,
    SharedGeographyNotLoaded,
    require_shared_geography_loaded,
)

pytestmark = [pytest.mark.unit]


class _Cursor:
    def __init__(self, counts: dict[str, int], recorder: list[tuple[str, object]]):
        self._counts = counts
        self._recorder = recorder
        self._rows: list[tuple[str, int]] = []

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_exception: object) -> None:
        return None

    def execute(self, statement: str, parameters: object = None) -> None:
        self._recorder.append((" ".join(statement.split()), parameters))
        asked = list(parameters[0]) if parameters else []
        self._rows = [
            (grain, count) for grain, count in self._counts.items() if grain in asked
        ]

    def fetchall(self) -> list[tuple[str, int]]:
        return self._rows


class _Connection:
    def __init__(self, counts: dict[str, int]):
        self._counts = counts
        self.executed: list[tuple[str, object]] = []

    def cursor(self) -> _Cursor:
        return _Cursor(self._counts, self.executed)


LOADED = {"nation": 1, "state": 56, "county": 3144, "place": 19500}


def test_a_loaded_reference_passes_and_says_what_it_counted() -> None:
    """Covers: DAG-020 — a caller can log what it saw, not only that it passed."""
    observed = require_shared_geography_loaded(_Connection(LOADED))
    assert observed == {"county": 3144, "nation": 1, "state": 56}


def test_a_bootstrapped_but_empty_reference_is_refused() -> None:
    """Covers: DAG-020 — the state the old guard passed on.

    The manifest creates the table in its `reference` phase, so `to_regclass`
    answered a name on every fresh warehouse. This is what happens instead.
    """
    with pytest.raises(SharedGeographyNotLoaded) as refusal:
        require_shared_geography_loaded(_Connection({}))

    message = str(refusal.value)
    # Every grain it asked about, including the ones that answered nothing:
    # a message naming only `county` leaves a reader guessing which half of
    # the predicate failed.
    assert "nation=0" in message
    assert "state=0" in message
    assert "county=0" in message
    assert "run silver_ref successfully first" in message


def test_a_partly_loaded_reference_names_what_is_short() -> None:
    """Covers: DAG-020 — an interrupted geography load is not a loaded one."""
    with pytest.raises(SharedGeographyNotLoaded) as refusal:
        require_shared_geography_loaded(
            _Connection({"nation": 1, "state": 56, "county": 12})
        )

    message = str(refusal.value)
    assert "county=12" in message
    assert "county>=3000" in message
    # And the grains that were fine are reported without being demanded again.
    assert "state>=50" not in message


def test_the_thresholds_exist_once() -> None:
    """Covers: DAG-020 — the shared minimum is not restated per source."""
    assert SHARED_GEOGRAPHY_MINIMUMS == {"nation": 1, "state": 50, "county": 3000}


def test_a_source_may_add_a_grain_but_never_lower_the_shared_one() -> None:
    """Covers: DAG-020 — Census PEP needs places; nobody needs fewer counties."""
    observed = require_shared_geography_loaded(
        _Connection(LOADED), additional_minimums={"place": 18000}
    )
    assert observed["place"] == 19500

    with pytest.raises(SharedGeographyNotLoaded, match="place>=18000"):
        require_shared_geography_loaded(
            _Connection({**LOADED, "place": 12}), additional_minimums={"place": 18000}
        )

    # An addition that tries to lower a shared threshold is ignored: the
    # shared minimum is the floor, and a source cannot opt out of it.
    with pytest.raises(SharedGeographyNotLoaded, match="county>=3000"):
        require_shared_geography_loaded(
            _Connection({"nation": 1, "state": 56, "county": 5}),
            additional_minimums={"county": 1},
        )


def test_the_predicate_counts_active_rows_in_the_current_dimension() -> None:
    """Covers: DAG-020 — it reads rows, which is the whole change."""
    connection = _Connection(LOADED)
    require_shared_geography_loaded(connection)

    [(statement, parameters)] = connection.executed
    assert "FROM silver_ref.dim_geo_current" in statement
    assert "WHERE is_active" in statement
    assert "COUNT(*)" in statement
    # Never the old question.
    assert "to_regclass" not in statement
    assert parameters == (["county", "nation", "state"],)
