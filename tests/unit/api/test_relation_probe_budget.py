"""A relation is probed once, and a composition resolves in one statement.

Covers: API-147 -- the serving-contract guard and metric resolution are
        memoised for the life of one request, and a composition resolves every
        measure it names in a single statement.

The warehouse session is ``REPEATABLE READ``, so neither a relation's
existence nor a published row's contents can change inside a request. Every
probe after the first bought nothing and held the connection a little longer
inside a pool whose exhaustion behaviour is already tested.

The session here is a stand-in that records what it was asked, which is the
only way to count statements without a database. The integration tier asserts
the same budget against a real one.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy.exc import SQLAlchemyError

from apps.api.services.contracts import (
    ServingContractUnavailable,
    relation_is_absent,
    require_relation,
    session_memo,
)
from apps.api.services.neutral_observations_service import (
    resolve_metric,
    resolve_metrics,
)

pytestmark = [pytest.mark.unit, pytest.mark.api]

METRIC_RELATION = "gold_glossary.dim_metric"


class _Result:
    def __init__(self, rows: list[dict[str, Any]] | None = None, scalar: Any = True):
        self._rows = rows or []
        self._scalar = scalar

    def mappings(self) -> "_Result":
        return self

    def all(self) -> list[dict[str, Any]]:
        return self._rows

    def first(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None

    def scalar(self) -> Any:
        return self._scalar


class _Session:
    """A session that answers, and remembers every statement it was given.

    It carries a real `info` dict, because that is where the memo lives; a
    stub without one is covered by its own test below.
    """

    def __init__(self, metrics: dict[str, dict[str, Any]] | None = None, present=True):
        self.info: dict[str, Any] = {}
        self.bind = object()
        self.statements: list[tuple[str, dict[str, Any]]] = []
        self._metrics = metrics or {}
        self._present = present

    def execute(self, query: Any, params: dict[str, Any] | None = None) -> _Result:
        rendered = " ".join(str(query).split())
        self.statements.append((rendered, dict(params or {})))
        if "to_regclass" in rendered:
            return _Result(scalar=self._present)
        if "metric_codes" in (params or {}):
            found = [
                self._metrics[code]
                for code in params["metric_codes"]
                if code in self._metrics
            ]
            return _Result(rows=sorted(found, key=lambda row: row["metric_code"]))
        row = self._metrics.get((params or {}).get("metric_code"))
        return _Result(rows=[row] if row else [])

    def probes(self) -> list[str]:
        return [
            parameters["relation_name"]
            for statement, parameters in self.statements
            if "to_regclass" in statement
        ]

    def metric_reads(self) -> list[tuple[str, dict[str, Any]]]:
        return [
            (statement, parameters)
            for statement, parameters in self.statements
            if METRIC_RELATION in statement and "to_regclass" not in statement
        ]


def _metric(code: str) -> dict[str, Any]:
    return {"metric_code": code, "source_code": code.split(":")[0]}


class TestTheProbeMemo:
    def test_a_relation_is_probed_once_however_often_it_is_guarded(self) -> None:
        """Covers: API-147 — fifteen call sites, one statement."""
        session = _Session()
        for _ in range(15):
            require_relation(session, METRIC_RELATION)

        assert session.probes() == [METRIC_RELATION]

    def test_each_distinct_relation_is_still_probed(self) -> None:
        """Covers: API-147 — memoised by name, not collapsed to one answer."""
        session = _Session()
        require_relation(session, METRIC_RELATION)
        require_relation(session, "gold_glossary.dim_geography")
        require_relation(session, METRIC_RELATION)

        assert session.probes() == [METRIC_RELATION, "gold_glossary.dim_geography"]

    def test_an_absent_relation_stays_absent_and_is_asked_once(self) -> None:
        """Covers: API-147 — the refusal is memoised too, not only the pass."""
        session = _Session(present=False)
        for _ in range(4):
            with pytest.raises(ServingContractUnavailable, match=METRIC_RELATION):
                require_relation(session, METRIC_RELATION)

        assert session.probes() == [METRIC_RELATION]

    def test_a_driver_that_raised_answered_nothing_and_is_asked_again(self) -> None:
        """Covers: API-147 — a failure is not an answer to memoise.

        A later call in the same request may reach a working connection, and
        recording "absent" from a raised statement would invent a deployment
        fault out of a transient one.
        """

        class _Raising(_Session):
            def __init__(self) -> None:
                super().__init__()
                self.attempts = 0

            def execute(self, query: Any, params: dict[str, Any] | None = None):
                self.attempts += 1
                raise SQLAlchemyError("connection lost")

        session = _Raising()
        assert relation_is_absent(session, METRIC_RELATION) is False
        assert relation_is_absent(session, METRIC_RELATION) is False
        assert session.attempts == 2

    def test_the_memo_does_not_outlive_the_session(self) -> None:
        """Covers: API-147 — it cannot outlive the snapshot it was true under."""
        first = _Session()
        require_relation(first, METRIC_RELATION)
        second = _Session()
        require_relation(second, METRIC_RELATION)

        assert first.probes() == [METRIC_RELATION]
        assert second.probes() == [METRIC_RELATION]

    def test_a_stub_without_an_info_mapping_behaves_as_it_always_did(self) -> None:
        """Covers: API-147 — a deterministic test double is not memoised."""

        class _Bare:
            bind = object()

            def __init__(self) -> None:
                self.calls = 0

            def execute(self, query: Any, params: dict[str, Any] | None = None):
                self.calls += 1
                return _Result(scalar=True)

        session = _Bare()
        require_relation(session, METRIC_RELATION)
        require_relation(session, METRIC_RELATION)
        assert session.calls == 2
        assert session_memo(session, "anything") is None


class TestBatchMetricResolution:
    def test_a_composition_resolves_in_one_statement(self) -> None:
        """Covers: API-147 — eight measures, one round trip."""
        codes = [f"FRED:M{index}" for index in range(8)]
        session = _Session({code: _metric(code) for code in codes})

        resolved = resolve_metrics(session, codes)

        assert sorted(resolved) == sorted(codes)
        assert len(session.metric_reads()) == 1
        # And the relation behind it is guarded exactly once.
        assert session.probes() == [METRIC_RELATION]

    def test_the_batch_answers_what_the_single_form_answers(self) -> None:
        """Covers: API-147 — same rows, and the same silence for an unknown."""
        known, unknown = "FRED:UNRATE", "NO:SUCH"
        batched = _Session({known: _metric(known)})
        singly = _Session({known: _metric(known)})

        resolved = resolve_metrics(batched, [known, unknown])
        assert resolved[known] == resolve_metric(singly, known)
        # An unknown code is absent from the answer, which is what `None` says.
        assert unknown not in resolved
        assert resolve_metric(singly, unknown) is None

    def test_a_code_resolved_in_the_batch_is_not_read_again(self) -> None:
        """Covers: API-147 — this is what makes a packet cost one statement."""
        codes = ["FRED:UNRATE", "BLS:LNS14000000"]
        session = _Session({code: _metric(code) for code in codes})

        resolve_metrics(session, codes)
        for _ in range(20):
            for code in codes:
                assert resolve_metric(session, code) is not None

        assert len(session.metric_reads()) == 1

    def test_an_unknown_code_is_not_re_read_either(self) -> None:
        """Covers: API-147 — recording only the hits sends `None` back to the DB."""
        session = _Session({"FRED:UNRATE": _metric("FRED:UNRATE")})

        resolve_metrics(session, ["FRED:UNRATE", "NO:SUCH"])
        for _ in range(5):
            assert resolve_metric(session, "NO:SUCH") is None

        assert len(session.metric_reads()) == 1

    def test_a_second_batch_asks_only_for_what_is_new(self) -> None:
        """Covers: API-147 — a packet's later blocks add measures, not repeats."""
        session = _Session({code: _metric(code) for code in ("A:1", "B:2", "C:3")})

        resolve_metrics(session, ["A:1", "B:2"])
        resolve_metrics(session, ["B:2", "C:3"])

        reads = session.metric_reads()
        assert len(reads) == 2
        assert reads[1][1]["metric_codes"] == ["C:3"]

        # And the second answer still carries every code it was asked about,
        # not only the one it had to read.
        assert sorted(resolve_metrics(session, ["B:2", "C:3"])) == ["B:2", "C:3"]
        assert len(session.metric_reads()) == 2

    def test_an_empty_composition_reads_nothing_at_all(self) -> None:
        """Covers: API-147 — including the guard, which has nothing to guard."""
        session = _Session()
        assert resolve_metrics(session, []) == {}
        assert resolve_metrics(session, ["", None]) == {}
        assert session.statements == []
