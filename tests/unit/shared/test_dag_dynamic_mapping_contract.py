"""What a dynamic expansion with nothing to expand over takes down with it.

Airflow marks a mapped task that expands to *zero* instances ``skipped``, not
``success``: there was no work, so there is no run to succeed. A downstream
task left on Airflow's default ``all_success`` trigger rule is then skipped
too, and the skip runs the length of the chain -- in this repository that
chain carries the silver transform, the gold refresh, the chunked serving
refresh and the publisher-ready event, so an empty plan would serve nothing
and announce nothing rather than serving the history already in silver.

``none_failed`` is the rule that separates the two cases: it still refuses to
run behind an upstream that *failed*, which is the protection these chains
were given the rule for, and it runs behind one that was skipped for having
nothing to do.

The wiring is read statically, from the DAG source, because the DAG tier
needs Airflow installed and this contract is about what the files declare.
The reader refuses any wiring it cannot resolve rather than passing over it
(see ``test_every_mapped_dag_is_fully_resolved``), so a DAG written in a
shape this does not understand fails here instead of going unchecked.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path

import pytest


pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
DAG_DIRECTORY = REPOSITORY_ROOT / "dags"

# Airflow's own default for a task that declares no rule.
DEFAULT_TRIGGER_RULE = "all_success"
# The rules that run behind a *skipped* upstream while still refusing to run
# behind a *failed* one.
SKIP_TOLERANT_TRIGGER_RULES = frozenset({"none_failed"})


@dataclass
class _Task:
    """One assigned task in a DAG's wiring."""

    variable: str
    function: str
    mapped: bool
    trigger_rule: str
    line: int
    downstream: set[str] = field(default_factory=set)


@dataclass
class _Wiring:
    """A DAG file's task functions, assigned tasks, and unresolved names."""

    path: Path
    tasks: dict[str, _Task] = field(default_factory=dict)
    unresolved: list[str] = field(default_factory=list)

    @property
    def mapped(self) -> list[_Task]:
        return [task for task in self.tasks.values() if task.mapped]

    def reachable_from(self, start: _Task) -> dict[str, _Task]:
        """Every task the given task's completion gates, transitively."""
        seen: dict[str, _Task] = {}
        frontier = list(start.downstream)
        while frontier:
            variable = frontier.pop()
            if variable in seen:
                continue
            task = self.tasks[variable]
            seen[variable] = task
            frontier.extend(task.downstream)
        return seen


def _decorated_task_trigger_rules(tree: ast.AST, wiring: _Wiring) -> dict[str, str]:
    """The declared trigger rule of every ``@task`` function, by name."""
    rules: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Name) and decorator.id == "task":
                rules[node.name] = DEFAULT_TRIGGER_RULE
                continue
            if not isinstance(decorator, ast.Call):
                continue
            if not (
                isinstance(decorator.func, ast.Name) and decorator.func.id == "task"
            ):
                continue
            rules[node.name] = DEFAULT_TRIGGER_RULE
            for keyword in decorator.keywords:
                if keyword.arg != "trigger_rule":
                    continue
                if isinstance(keyword.value, ast.Constant) and isinstance(
                    keyword.value.value, str
                ):
                    rules[node.name] = keyword.value.value
                else:
                    wiring.unresolved.append(
                        f"{node.name} declares a trigger rule this cannot read "
                        f"(line {keyword.value.lineno})"
                    )
    return rules


def _base_name_and_mapping(call: ast.Call) -> tuple[str | None, bool]:
    """The task function a call expression invokes, and whether it maps it."""
    mapped = False
    node: ast.expr = call.func
    while isinstance(node, ast.Attribute):
        if node.attr in {"expand", "expand_kwargs"}:
            mapped = True
        elif node.attr not in {"partial", "override"}:
            return None, mapped
        node = node.value
        if isinstance(node, ast.Call):
            node = node.func
    if isinstance(node, ast.Name):
        return node.id, mapped
    return None, mapped


def _chain(node: ast.expr) -> list[ast.expr]:
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift):
        return _chain(node.left) + _chain(node.right)
    return [node]


def _read_wiring(path: Path) -> _Wiring:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    wiring = _Wiring(path=path)
    rules = _decorated_task_trigger_rules(tree, wiring)

    if "<<" in source:
        wiring.unresolved.append(
            "the file wires a dependency with '<<', which this reader does not follow"
        )

    # Pass one: every variable bound to a task call, mapped or not.
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name) or not isinstance(node.value, ast.Call):
            continue
        function, mapped = _base_name_and_mapping(node.value)
        if function is None or function not in rules:
            continue
        wiring.tasks[target.id] = _Task(
            variable=target.id,
            function=function,
            mapped=mapped,
            trigger_rule=rules[function],
            line=node.lineno,
        )

    # Pass two: the edges. An argument that is another task is an implicit
    # dependency in Airflow exactly as `>>` is an explicit one.
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if (
                isinstance(target, ast.Name)
                and target.id in wiring.tasks
                and isinstance(node.value, ast.Call)
            ):
                arguments = list(node.value.args) + [
                    keyword.value for keyword in node.value.keywords
                ]
                for argument in arguments:
                    if isinstance(argument, ast.Name) and argument.id in wiring.tasks:
                        wiring.tasks[argument.id].downstream.add(target.id)
            continue
        if not isinstance(node, ast.Expr):
            continue
        if not (
            isinstance(node.value, ast.BinOp) and isinstance(node.value.op, ast.RShift)
        ):
            continue
        operands = _chain(node.value)
        names = []
        for operand in operands:
            if isinstance(operand, ast.Name) and operand.id in wiring.tasks:
                names.append(operand.id)
            else:
                wiring.unresolved.append(
                    f"a '>>' chain on line {node.lineno} names something this "
                    "reader cannot resolve to a task"
                )
                names.append(None)
        for upstream, downstream in zip(names, names[1:]):
            if upstream is not None and downstream is not None:
                wiring.tasks[upstream].downstream.add(downstream)
    return wiring


def _dag_wirings() -> dict[str, _Wiring]:
    return {
        path.name: _read_wiring(path) for path in sorted(DAG_DIRECTORY.glob("*.py"))
    }


def test_every_mapped_dag_is_fully_resolved() -> None:
    """Covers: ETL-051 — the wiring reader refuses what it cannot follow.

    The contract below is only worth what this reader sees. A DAG that maps a
    task and wires it in a shape the reader skips would pass the contract
    without being read at all, so an unresolved name is a failure here rather
    than silence there.
    """
    for name, wiring in _dag_wirings().items():
        if not wiring.mapped:
            continue
        assert wiring.unresolved == [], (
            f"{name} maps a task but its wiring could not be read completely: "
            + "; ".join(wiring.unresolved)
        )


def test_dynamic_mapping_is_declared_where_it_is_used() -> None:
    """Covers: ETL-051 — the mapped DAGs and their gated work are both found.

    Keeps the trigger-rule contract from passing because nothing matched: the
    three provider DAGs that expand a task must still be the ones found to
    expand a task, and each expansion must still gate downstream work.
    """
    wirings = _dag_wirings()
    mapped_dags = sorted(name for name, wiring in wirings.items() if wiring.mapped)
    assert mapped_dags == [
        "acs_ingest_dag.py",
        "bls_ingest_dag.py",
        "fred_ingest_dag.py",
    ]
    for name in mapped_dags:
        wiring = wirings[name]
        for task in wiring.mapped:
            if task.function.startswith("ingest"):
                assert wiring.reachable_from(task), (
                    f"{name}: the mapped task '{task.variable}' gates nothing, so "
                    "this file no longer proves what a zero-instance expansion costs"
                )


def test_a_zero_instance_expansion_does_not_skip_the_work_behind_it() -> None:
    """Covers: ETL-051 — work behind a mapped task tolerates an empty expansion.

    Every task a mapped task gates, directly or transitively, declares
    ``none_failed``. Airflow skips a mapped task that expands to zero
    instances, and Airflow's default ``all_success`` treats that skip as
    reason not to run -- which in these DAGs would skip the silver transform,
    the gold refresh, the chunked serving refresh and the publisher event for a
    plan that simply had nothing new to fetch.
    """
    offenders = []
    for name, wiring in _dag_wirings().items():
        for mapped in wiring.mapped:
            for task in wiring.reachable_from(mapped).values():
                if task.trigger_rule in SKIP_TOLERANT_TRIGGER_RULES:
                    continue
                offenders.append(
                    f"{name}:{task.line} '{task.function}' runs on "
                    f"trigger_rule={task.trigger_rule!r} behind the mapped task "
                    f"'{mapped.variable}'"
                )
    assert offenders == [], (
        "a mapped task that expands to zero instances is skipped, and these "
        "downstream tasks would be skipped with it: "
        + "; ".join(sorted(set(offenders)))
    )
